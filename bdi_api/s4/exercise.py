import gzip
import json
import os
from typing import Annotated

import boto3
import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


@s4.post("/aircraft/download")
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
    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix = "raw/day=20231101/"

    # Get list of files from the page
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".json.gz")]
    links = links[:file_limit]

    s3_client = boto3.client("s3")

    for filename in links:
        file_url = base_url + filename
        r = requests.get(file_url)
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_prefix + filename,
            Body=r.content,
        )

    return "OK"


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    s3_bucket = settings.s3_bucket
    s3_prefix = "raw/day=20231101/"
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")

    # Clean prepared folder
    if os.path.exists(prepared_dir):
        for f in os.listdir(prepared_dir):
            os.remove(os.path.join(prepared_dir, f))
    else:
        os.makedirs(prepared_dir)

    s3_client = boto3.client("s3")

    # List all files in S3
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    objects = response.get("Contents", [])

    for obj in objects:
        key = obj["Key"]
        filename = os.path.basename(key)

        # Download from S3
        s3_response = s3_client.get_object(Bucket=s3_bucket, Key=key)
        compressed_data = s3_response["Body"].read()

        # Decompress and parse
        with gzip.open(__import__("io").BytesIO(compressed_data), "rt") as f:
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
