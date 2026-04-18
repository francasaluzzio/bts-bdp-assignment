import os
import json
import requests
from bs4 import BeautifulSoup
from typing import Annotated
from fastapi import APIRouter, status
from fastapi.params import Query
from bdi_api.settings import Settings

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)


@s1.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"

    # Clean the download folder before writing
    if os.path.exists(download_dir):
        for f in os.listdir(download_dir):
            os.remove(os.path.join(download_dir, f))
    else:
        os.makedirs(download_dir)

    # Get list of files from the page
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".json.gz")]
    links = links[:file_limit]

    # Download each file
    for filename in links:
        file_url = base_url + filename
        r = requests.get(file_url)
        with open(os.path.join(download_dir, filename), "wb") as f:
            f.write(r.content)

    return "OK"


@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    import gzip

    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")

    # Clean prepared folder
    if os.path.exists(prepared_dir):
        for f in os.listdir(prepared_dir):
            os.remove(os.path.join(prepared_dir, f))
    else:
        os.makedirs(prepared_dir)

    # Process each file
    for filename in os.listdir(raw_dir):
        filepath = os.path.join(raw_dir, filename)
        with gzip.open(filepath, "rt") as f:
            data = json.load(f)

        aircraft_list = data.get("aircraft", [])
        timestamp = data.get("now", 0)

        prepared = []
        for ac in aircraft_list:
            prepared.append({
                "icao": ac.get("hex", "").strip(),
                "registration": ac.get("r", ""),
                "type": ac.get("t", ""),
                "lat": ac.get("lat"),
                "lon": ac.get("lon"),
                "timestamp": timestamp,
                "alt_baro": ac.get("alt_baro"),
                "gs": ac.get("gs"),
                "emergency": ac.get("emergency", "none") not in ("none", "", None),
            })

        out_filename = filename.replace(".json.gz", ".json")
        with open(os.path.join(prepared_dir, out_filename), "w") as f:
            json.dump(prepared, f)

    return "OK"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    aircraft = {}

    for filename in os.listdir(prepared_dir):
        filepath = os.path.join(prepared_dir, filename)
        with open(filepath) as f:
            data = json.load(f)
        for ac in data:
            icao = ac.get("icao")
            if icao and icao not in aircraft:
                aircraft[icao] = {
                    "icao": icao,
                    "registration": ac.get("registration", ""),
                    "type": ac.get("type", ""),
                }

    sorted_aircraft = sorted(aircraft.values(), key=lambda x: x["icao"])
    start = page * num_results
    return sorted_aircraft[start: start + num_results]


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    positions = []

    for filename in os.listdir(prepared_dir):
        filepath = os.path.join(prepared_dir, filename)
        with open(filepath) as f:
            data = json.load(f)
        for ac in data:
            if ac.get("icao") == icao and ac.get("lat") and ac.get("lon"):
                positions.append({
                    "timestamp": ac.get("timestamp"),
                    "lat": ac.get("lat"),
                    "lon": ac.get("lon"),
                })

    positions.sort(key=lambda x: x["timestamp"])
    start = page * num_results
    return positions[start: start + num_results]


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    max_altitude = 0
    max_speed = 0
    had_emergency = False

    for filename in os.listdir(prepared_dir):
        filepath = os.path.join(prepared_dir, filename)
        with open(filepath) as f:
            data = json.load(f)
        for ac in data:
            if ac.get("icao") == icao:
                alt = ac.get("alt_baro")
                if isinstance(alt, (int, float)):
                    max_altitude = max(max_altitude, alt)
                gs = ac.get("gs")
                if isinstance(gs, (int, float)):
                    max_speed = max(max_speed, gs)
                if ac.get("emergency"):
                    had_emergency = True

    return {
        "max_altitude_baro": max_altitude,
        "max_ground_speed": max_speed,
        "had_emergency": had_emergency,
    }
