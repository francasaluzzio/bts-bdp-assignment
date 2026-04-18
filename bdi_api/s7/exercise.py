from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from bdi_api.settings import Settings
from neo4j import GraphDatabase

s7 = APIRouter(prefix="/api/s7", tags=["s7"])
settings = Settings()


class Person(BaseModel):
    name: str
    city: str
    age: int


class Relationship(BaseModel):
    person1: str
    person2: str


def get_driver():
    return GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password)
    )


@s7.post("/graph/person")
def create_person(person: Person) -> str:
    driver = get_driver()
    with driver.session() as session:
        session.run(
            "CREATE (p:Person {name: $name, city: $city, age: $age})",
            name=person.name,
            city=person.city,
            age=person.age
        )
    return "ok"


@s7.get("/graph/persons")
def list_persons() -> list[dict]:
    driver = get_driver()
    with driver.session() as session:
        result = session.run("MATCH (p:Person) RETURN p ORDER BY p.name")
        return [
            {
                "name": record["p"]["name"],
                "city": record["p"]["city"],
                "age": record["p"]["age"],
            }
            for record in result
        ]


@s7.get("/graph/person/{name}/friends")
def get_friends(name: str) -> list[dict]:
    driver = get_driver()
    with driver.session() as session:
        # Check person exists
        exists = session.run("MATCH (p:Person {name: $name}) RETURN p", name=name).single()
        if not exists:
            raise HTTPException(status_code=404, detail="Person not found")

        result = session.run("""
            MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(friend:Person)
            RETURN friend
            ORDER BY friend.name
        """, name=name)
        return [
            {
                "name": record["friend"]["name"],
                "city": record["friend"]["city"],
                "age": record["friend"]["age"],
            }
            for record in result
        ]


@s7.post("/graph/relationship")
def create_relationship(relationship: Relationship) -> str:
    driver = get_driver()
    with driver.session() as session:
        # Check both persons exist
        p1 = session.run("MATCH (p:Person {name: $name}) RETURN p", name=relationship.person1).single()
        p2 = session.run("MATCH (p:Person {name: $name}) RETURN p", name=relationship.person2).single()
        if not p1:
            raise HTTPException(status_code=404, detail=f"Person {relationship.person1} not found")
        if not p2:
            raise HTTPException(status_code=404, detail=f"Person {relationship.person2} not found")

        session.run("""
            MATCH (a:Person {name: $name1}), (b:Person {name: $name2})
            MERGE (a)-[:FRIENDS_WITH]-(b)
        """, name1=relationship.person1, name2=relationship.person2)
    return "ok"


@s7.get("/graph/person/{name}/recommendations")
def get_recommendations(name: str) -> list[dict]:
    driver = get_driver()
    with driver.session() as session:
        exists = session.run("MATCH (p:Person {name: $name}) RETURN p", name=name).single()
        if not exists:
            raise HTTPException(status_code=404, detail="Person not found")

        result = session.run("""
            MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(friend)-[:FRIENDS_WITH]-(rec:Person)
            WHERE rec.name <> $name
            AND NOT (p)-[:FRIENDS_WITH]-(rec)
            RETURN rec, COUNT(friend) AS mutual_friends
            ORDER BY mutual_friends DESC, rec.name
        """, name=name)
        return [
            {
                "name": record["rec"]["name"],
                "city": record["rec"]["city"],
                "age": record["rec"]["age"],
                "mutual_friends": record["mutual_friends"],
            }
            for record in result
        ]
