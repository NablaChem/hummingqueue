from multiprocessing.sharedctypes import Value
from pydantic import BaseModel, Field, constr, root_validator
from typing import List, Optional
import time
import secrets

from . import validators
from . import helpers
from . import auth

counter = 0

from fastapi import FastAPI, Header

app = FastAPI()


class AdminAuth(BaseModel):
    admin_token: str = Field(..., description="Installation-specific admin token.")

    _validator_admin = root_validator(allow_reuse=True)(validators.admin)


class NodeAuth(BaseModel):
    node_id: str = Field(..., description="User-generated identifier of the node.")
    owner_token: str = Field(
        ..., description="System-generated identifier of the owner."
    )
    compute_token: str = Field(..., description="Authentication for node.")

    _validator_owner = root_validator(allow_reuse=True)(validators.owner)
    _validator_compute_token = root_validator(allow_reuse=True)(
        validators.compute_token
    )


class UserAuth(BaseModel):
    user_token: str = Field(..., description="System-generated identifier of the user.")
    collaboration_token: Optional[str] = Field(
        ..., description="System-generated identifier of a collaboration."
    )


class JobFetch(NodeAuth):
    in_cache: List[str] = Field(..., description="Already cached containers.")
    cores: int = Field(..., description="Number of cores available.")
    memory_mb: int = Field(..., description="Available memory in MB.")


class JobSpec(UserAuth):
    container_hash: str = Field(..., description="Checksum of the container to run.")
    # command: constr = Field(..., description="Command to run.", max_length=2000)
    # data_source: str = Field(
    #     ..., description="S3 directory containing the data to be present."
    # )
    # data_target: str = Field(
    #     ...,
    #     description="S3 directory where all modified files should be uploaded to.",
    # )
    core_limit: int = Field(..., description="Requested number of cores.")
    memory_mb_limit: int = Field(..., description="Requested memory in MB.")
    time_seconds_limit: int = Field(..., description="Requested wall time duration.")
    tags: Optional[List[str]] = Field(
        ...,
        description="List of tags this calculation belongs to. Unique within a collaboration or (if no collaboration is specified) for a user.",
    )


@app.get("/challenge")
def current_challenge():
    return helpers.get_valid_challenges()["current"]


@app.post("/heartbeat")
def register_heartbeat(
    body: NodeAuth, signature: str = Header(default=None, convert_underscores=False)
):
    criteria = {"owner_token": body.owner_token, "node_id": body.node_id}
    heartbeat = criteria.copy()
    heartbeat["seen"] = time.time()
    auth.db.heartbeats.replace_one(criteria, heartbeat)
    return helpers.get_valid_challenges()["current"]


@app.post("/job/fetch")
def job_fetch(body: JobFetch):
    global counter
    jobids = list(range(counter, counter + body.cores))
    counter += body.cores
    return {_: None for _ in jobids}


@app.post("/job/create")
def job_create(body: JobSpec):
    pass


class OwnerCreateResponse(BaseModel):
    owner_token: str = Field(..., description="Newly created owner token.")


@app.post(
    "/owner/create",
    summary="Creates a new owner.",
    description="Only accessible to the admin of the installation.",
    tags=["authentication"],
    responses={
        403: {"description": "Authentication failed."},
        200: {"description": "Owner created.", "model": OwnerCreateResponse},
    },
)
def owner_create(body: AdminAuth):
    owner_token = secrets.token_urlsafe(auth.TOKEN_LENGTH)
    auth.db.users.insert_one({"token": owner_token, "is_owner": True})
    return {"owner_token": owner_token}


class OwnerFirstTimeAuth(BaseModel):
    owner_token: str = Field(..., description="Owner token.")
    public_key: str = Field(..., description="Base64-encoded DER format public key.")

    _validator_owner = root_validator(allow_reuse=True)(validators.owner)
    _validator_owner_no_key = root_validator(allow_reuse=True)(validators.owner_no_key)
    _validator_public_key_consistent = root_validator(allow_reuse=True)(
        validators.public_key_consistent
    )


@app.post(
    "/owner/activate",
    summary="Initial setup for a new owner.",
    description="The user-supplied public key will be used for future authentication of this owner. Can only be called once for a given owner.",
    tags=["authentication"],
    responses={
        404: {"description": "No such owner."},
        403: {"description": "Cannot initialize again."},
        200: {"description": "Public key stored."},
    },
)
def owner_activate(body: OwnerFirstTimeAuth):
    auth.db.users.update_one(
        {"owner_token": body.owner_token, "is_owner": True},
        {"public_key": body.public_key},
    )
