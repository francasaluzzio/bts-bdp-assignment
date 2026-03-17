from fastapi import APIRouter, status
from pydantic import BaseModel

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


@s8.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[AircraftReturn]:
    data = [
        AircraftReturn(
            icao="ABC123",
            registration="REG123",
            type="A320",
            owner="Airline",
            manufacturer="Airbus",
            model="A320"
        ),
        AircraftReturn(
            icao="DEF456",
            registration="REG456",
            type="B737",
            owner="Airline",
            manufacturer="Boeing",
            model="737"
        ),
    ]

    start = page * num_results
    end = start + num_results

    return data[start:end]


@s8.get("/aircraft/{icao}/co2")
def get_aircraft_co2(icao: str, day: str) -> AircraftCO2Return:
    return AircraftCO2Return(icao=icao, hours_flown=0.0, co2=None)