from fastapi import APIRouter
from pydantic import BaseModel

from bdi_api.settings import Settings
from neo4j import GraphDatabase

s7 = APIRouter()
settings = Settings()


class Person(BaseModel):
    name: str
    city: str
    age: int


@s7.post("/graph/person")
def create_person(person: Person) -> str:
    driver = GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password)
    )

    with driver.session() as session:
        session.run(
            "CREATE (p:Person {name: $name, city: $city, age: $age})",
            name=person.name,
            city=person.city,
            age=person.age
        )

    return "ok"