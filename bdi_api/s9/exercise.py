import json
import os
from datetime import datetime

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

s9 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s9",
    tags=["s9"],
)

DATA_DIR = os.path.dirname(__file__)


def load_pipelines():
    with open(os.path.join(DATA_DIR, "data.json")) as f:
        return json.load(f)


def load_stages():
    with open(os.path.join(DATA_DIR, "stages_data.json")) as f:
        return json.load(f)


class PipelineRun(BaseModel):
    id: str
    repository: str
    branch: str
    status: str
    triggered_by: str
    started_at: datetime
    finished_at: datetime | None
    stages: list[str]


class PipelineStage(BaseModel):
    name: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    logs_url: str


@s9.get("/pipelines")
def list_pipelines(
    repository: str | None = None,
    status_filter: str | None = None,
    num_results: int = 100,
    page: int = 0,
) -> list[PipelineRun]:
    pipelines = load_pipelines()

    if repository:
        pipelines = [p for p in pipelines if p["repository"] == repository]
    if status_filter:
        pipelines = [p for p in pipelines if p["status"] == status_filter]

    pipelines.sort(key=lambda x: x["started_at"], reverse=True)

    start = page * num_results
    return [PipelineRun(**p) for p in pipelines[start: start + num_results]]


@s9.get("/pipelines/{pipeline_id}/stages")
def get_pipeline_stages(pipeline_id: str) -> list[PipelineStage]:
    pipelines = load_pipelines()
    pipeline_ids = [p["id"] for p in pipelines]

    if pipeline_id not in pipeline_ids:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    stages = load_stages()
    return [PipelineStage(**s) for s in stages.get(pipeline_id, [])]
