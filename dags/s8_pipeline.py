from datetime import datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
import sqlalchemy as sa

BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"
AIRCRAFT_CSV = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
FUEL_RATES_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"
DB_URL = "sqlite:///hr_database.db"
FILE_LIMIT = 100


def download_and_store():
    response = requests.get(BASE_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".json.gz")][:FILE_LIMIT]

    all_aircraft = []
    for filename in links:
        r = requests.get(BASE_URL + filename)
        import gzip, json, io
        with gzip.open(io.BytesIO(r.content), "rt") as f:
            data = json.load(f)
        timestamp = data.get("now", 0)
        for ac in data.get("aircraft", []):
            all_aircraft.append({
                "icao": ac.get("hex", "").strip(),
                "registration": ac.get("r"),
                "type": ac.get("t"),
                "lat": ac.get("lat"),
                "lon": ac.get("lon"),
                "timestamp": timestamp,
            })

    df = pd.DataFrame(all_aircraft)
    df.to_parquet("/tmp/s8_bronze.parquet", index=False)


def enrich_and_load():
    import pandas as pd

    df = pd.read_parquet("/tmp/s8_bronze.parquet")

    # Download aircraft metadata
    try:
        meta = pd.read_csv(AIRCRAFT_CSV, low_memory=False)
        meta = meta.rename(columns={
            "icao24": "icao",
            "registration": "registration",
            "owner": "owner",
            "manufacturername": "manufacturer",
            "model": "model",
        })
        meta["icao"] = meta["icao"].str.strip().str.lower()
        df = df.merge(meta[["icao", "owner", "manufacturer", "model"]], on="icao", how="left")
    except Exception:
        df["owner"] = None
        df["manufacturer"] = None
        df["model"] = None

    # Save silver layer
    df.to_parquet("/tmp/s8_silver.parquet", index=False)

    # Load into DB
    engine = sa.create_engine(DB_URL)
    with engine.connect() as conn:
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS s8_aircraft (
                icao TEXT PRIMARY KEY,
                registration TEXT,
                type TEXT,
                owner TEXT,
                manufacturer TEXT,
                model TEXT
            )
        """))
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS s8_tracking (
                icao TEXT,
                day TEXT,
                observation_count INTEGER,
                PRIMARY KEY (icao, day)
            )
        """))
        conn.commit()

    aircraft_df = df[["icao", "registration", "type", "owner", "manufacturer", "model"]].drop_duplicates("icao")
    aircraft_df.to_sql("s8_aircraft", engine, if_exists="replace", index=False)

    tracking_df = df.groupby("icao").size().reset_index(name="observation_count")
    tracking_df["day"] = "2023-11-01"
    tracking_df.to_sql("s8_tracking", engine, if_exists="replace", index=False)


with DAG(
    dag_id="s8_aircraft_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="download_and_store", python_callable=download_and_store)
    t2 = PythonOperator(task_id="enrich_and_load", python_callable=enrich_and_load)
    t1 >> t2
