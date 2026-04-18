import json
import os

import pandas as pd
import requests
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import text

from bdi_api.settings import Settings

settings = Settings()

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)

FUEL_RATES_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"


class AircraftReturn(BaseModel):
    icao: str
    registration: str | None
    type: str | None
    owner: str | None
    manufacturer: str | None
    model: str | None


class AircraftCO2Return(BaseModel):
    icao: str
    hours_flown: float
    co2: float | None


def get_engine():
    import sqlalchemy as sa
    return sa.create_engine(settings.db_url)


@s8.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[AircraftReturn]:
    engine = get_engine()
    offset = page * num_results
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT icao, registration, type, owner, manufacturer, model
            FROM s8_aircraft
            ORDER BY icao ASC
            LIMIT :limit OFFSET :offset
        """), {"limit": num_results, "offset": offset})
        return [
            AircraftReturn(
                icao=row.icao,
                registration=row.registration,
                type=row.type,
                owner=row.owner,
                manufacturer=row.manufacturer,
                model=row.model,
            )
            for row in result
        ]


@s8.get("/aircraft/{icao}/co2")
def get_aircraft_co2(icao: str, day: str) -> AircraftCO2Return:
    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT type, observation_count
            FROM s8_tracking
            WHERE icao = :icao AND day = :day
        """), {"icao": icao, "day": day})
        row = result.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Aircraft not found")

    hours_flown = (row.observation_count * 5) / 3600

    try:
        fuel_rates = requests.get(FUEL_RATES_URL).json()
        galph = fuel_rates.get(row.type, {}).get("galph")
    except Exception:
        galph = None

    if galph is not None:
        fuel_used_kg = hours_flown * galph * 3.04
        co2 = (fuel_used_kg * 3.15) / 907.185
    else:
        co2 = None

    return AircraftCO2Return(icao=icao, hours_flown=hours_flown, co2=co2)
