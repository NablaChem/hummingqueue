from unicodedata import name
from pydantic import BaseModel, Field
from typing import List, Optional

counter = 0

from fastapi import FastAPI

app = FastAPI()


class NodeAuth(BaseModel):
    nodeid: str = Field(..., description="User-generated identifier of the node.")
    ownerid: str = Field(..., description="System-generated identifier of the owner.")
    computesecret: str = Field(..., description="Authentication for node.")


class UserAuth(BaseModel):
    userid: str = Field(..., description="System-generated identifier of the user.")
    projectid: Optional[str] = Field(
        ..., description="System-generated identifier of a project."
    )


class JobFetch(NodeAuth):
    in_cache: List[str] = Field(..., description="Already cached containers.")
    cores: int = Field(..., description="Number of cores available.")
    memory_mb: int = Field(..., description="Available memory in MB.")


class JobSpec(BaseModel):
    container: str = Field(..., description="URL of the container to run.")
    command: str = Field(..., description="Command to run.")
    datasource: str = Field(
        ..., description="URL, e.g. to S3 directory containing the data to be present."
    )
    datatarget: str = Field(
        ...,
        description="URL, e.g. to S3 directory where all modified files should be uploaded to.",
    )
    core_limit: int = Field(..., description="Requested number of cores.")
    memory_mb_limit: int = Field(..., description="Requested memory in MB.")
    tags: Optional[List[str]] = Field(
        ...,
        description="List of tags this calculation belongs to. Unique within a project or (if no project is specified) for a user.",
    )
    jobid: str = Field(..., description="System-generated job identifier.")


@app.post("/heartbeat")
def register_heartbeat(body: NodeAuth):
    # TODO: write to mongodb
    pass


@app.post("/job/fetch")
def job_fetch(body: JobFetch):
    global counter
    jobids = list(range(counter, counter + body.cores))
    counter += body.cores
    return {_: None for _ in jobids}


@app.post("/job/create")
def job_create(body: JobSpec):
    pass
