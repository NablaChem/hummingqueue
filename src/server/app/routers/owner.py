from pydantic import BaseModel, Field
from typing import Optional
import enum
from fastapi import APIRouter, Request, Header, Depends, HTTPException, status
import json
import base64

from .. import validators
from .. import helpers
from .. import auth

app = APIRouter()


class AdminAuth(BaseModel):
    admin_token: str = Field(..., description="Installation-specific admin token.")

    class Config:
        schema_extra = {
            "example": {
                "admin_token": "admin",
            }
        }


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
    # validate request
    validators.is_admin(body)

    # handle request
    owner_token = helpers.new_token(helpers.TokenTypes.OWNER)
    auth.db.users.insert_one({"token": owner_token, "is_owner": True})
    return {"owner_token": owner_token}


class SignedRequest(BaseModel):
    challenge: str = Field(..., description="Current challenge.")
    time: int = Field(
        ...,
        description="Current unix timestamp for UTC time zone.",
    )

    class Config:
        schema_extra = {
            "example": {
                "challenge": helpers.get_valid_challenges()[0]["current"],
                "time": helpers.utc_now(),
            }
        }


class OwnerFirstTimeAuth(SignedRequest):
    owner_token: str = Field(..., description="Owner token.", regex="^O-[A-Za-z0-9]+$")
    verify_key: str = Field(
        ..., description="Base64-encoded Curve25519 signature verify key."
    )

    class Config:
        schema_extra = {
            "example": {
                "owner_token": "O-3yofQTVDveBf5U8PZf3nay",
                "verify_key": "EXThJPyCI8/lKjWvD8EouAaYi3nwMIDN3cct43n0ES4=",
            }
        }


class UserFirstTimeAuth(SignedRequest):
    user_token: str = Field(..., description="User token.", regex="^U-[A-Za-z0-9]+$")
    verify_key: str = Field(
        ..., description="Base64-encoded Curve25519 signature verify key."
    )

    class Config:
        schema_extra = {
            "example": {
                "user_token": "U-3yofQTVDveBf5U8PZf3nay",
                "verify_key": "EXThJPyCI8/lKjWvD8EouAaYi3nwMIDN3cct43n0ES4=",
            }
        }


class EmptyResponse(BaseModel):
    ...


@app.post(
    "/owner/activate",
    summary="Initial setup for a new owner.",
    description="The user-supplied verify key will be used for future authentication of this owner. Can only be called once for a given owner.",
    tags=["authentication"],
    responses={
        404: {"description": "No such owner."},
        403: {"description": "Authentication error."},
        200: {
            "description": "Signature verification key successfully stored.",
            "model": EmptyResponse,
        },
    },
)
async def owner_activate(
    body: OwnerFirstTimeAuth,
    request: Request,
    hmq_signature: str = helpers.HMQ_SIGNATURE_HEADER,
):
    # validate request
    owner = validators.owner_exists(body)
    validators.owner_has_no_key(owner)
    await validators.public_key_consistent(body, request)
    validators.time_window_valid(body)

    # handle request
    auth.db.users.update_one(
        {"token": body.owner_token, "is_owner": True},
        {"$set": {"verify_key": body.verify_key}},
    )


class KeySignatureReason(str, enum.Enum):
    ADD_OWN_ENCRYPTION_KEY = "ADD_OWN_ENCRYPTION_KEY"
    AUTHORIZE_USER = "AUTHORIZE_USER"


class SignKeyRequest(SignedRequest):
    user_token: Optional[str] = Field(
        None,
        description="User token for the user who is signing the key.",
        regex="^U-[A-Za-z0-9]+$",
    )
    owner_token: Optional[str] = Field(
        None,
        description="Owner token for the user who is signing the key.",
        regex="^O-[A-Za-z0-9]+$",
    )
    reason: KeySignatureReason = Field(..., description="Type of signature.")
    key: str = Field(..., description="Base64-encoded key to be signed.")

    class Config:
        schema_extra = {
            "example": {
                "owner_token": "O-3yofQTVDveBf5U8PZf3nay",
                "key": "NP88KNsQy2zsbP09zdyNu1NUPfgj+qm4GdEcyQZhoWs=",
                "reason": "ADD_OWN_ENCRYPTION_KEY",
            }
        }


@app.post(
    "/user/sign",
    summary="Adds a new key signature to the database.",
    description="This keeps track of the key signing process to build a full key chain starting from the owners.",
    tags=["authentication"],
    responses={
        400: {"description": "Validation error."},
        404: {"description": "No such user."},
        403: {"description": "Authentication error."},
        200: {
            "description": "Signature successfully stored.",
            "model": EmptyResponse,
        },
    },
)
async def signature_add(
    body: SignKeyRequest,
    request: Request,
    hmq_signature: str = helpers.HMQ_SIGNATURE_HEADER,
):
    user_or_owner_token = validators.user_or_owner_exists(body)
    if body.owner_token is not None and body.user_token is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only one of owner_token or user_token can be specified.",
        )

    await validators.verify_signed_request(
        body, request, user_or_owner_token, hmq_signature
    )

    auth.db.key_signatures.insert_one(
        {"action": await request.json(), "signature": hmq_signature}
    )


@app.post(
    "/user/create",
    summary="Creates a new user.",
    description="Open for everybody.",
    tags=["authentication"],
    responses={
        200: {"description": "User created."},
    },
)
def user_create():
    # handle request
    user_token = helpers.new_token(helpers.TokenTypes.USER)
    auth.db.users.insert_one({"token": user_token, "is_owner": False})
    return {"user_token": user_token}


@app.post(
    "/user/activate",
    summary="Initial setup for a new user.",
    description="The user-supplied verify key will be used for future authentication. Can only be called once for a given user.",
    tags=["authentication"],
    responses={
        404: {"description": "No such user."},
        403: {"description": "Authentication error."},
        200: {
            "description": "Signature verification key successfully stored.",
            "model": EmptyResponse,
        },
    },
)
async def user_activate(
    body: UserFirstTimeAuth,
    request: Request,
    hmq_signature: str = helpers.HMQ_SIGNATURE_HEADER,
):
    # validate request
    user = validators.user_exists(body)
    validators.user_has_no_key(user)
    await validators.public_key_consistent(body, request)
    validators.time_window_valid(body)

    # handle request
    auth.db.users.update_one(
        {"token": body.user_token, "is_owner": False},
        {"$set": {"verify_key": body.verify_key}},
    )


@app.post(
    "/project/create",
    summary="Creates a new project.",
    description="A project is a project that can be joined by many owners to pool their compute resources. Every job needs to be attached to exactly one project. Open for everybody.",
    tags=["authentication"],
    responses={
        200: {"description": "project created."},
    },
)
def project_create():
    # handle request
    project_token = helpers.new_token(helpers.TokenTypes.PROJECT)
    auth.db.projects.insert_one({"token": project_token})
    return {"project_token": project_token}


class ProjectSignatureReason(str, enum.Enum):
    JOIN_PROJECT = "JOIN_PROJECT"


class SignProjectRequest(SignedRequest):
    owner_token: str = Field(
        ...,
        description="Owner token for the user who is joining a project.",
        regex="^O-[A-Za-z0-9]+$",
    )
    reason: ProjectSignatureReason = Field(..., description="Type of signature.")
    project_token: str = Field(
        ...,
        description="Project token.",
        regex="^P-[A-Za-z0-9]+$",
    )
    alias: str = Field(
        ...,
        description="""A human-readable alias for the project from the perspective of the owner. 
        Note that other users can see this name during signature verification or project selection. 
        For legal reasons, do not include personally identifiable information.""",
    )

    class Config:
        schema_extra = {
            "example": {
                "owner_token": "O-3yofQTVDveBf5U8PZf3nay",
                "project_token": "P-3w8Z4XT73RMYqbJm4fTHJY",
                "alias": "the next Big Thing",
                "reason": "JOIN_PROJECT",
            }
        }


@app.post(
    "/project/join",
    summary="Join a project as owner.",
    description="""A project is used to organize many calculations and track resource usage.
    Signing a project means that the owner is allowing the project and all its users to submit jobs to be run on the resources of the owner. 
    Note that signing a project alone is not sufficent for running compute jobs on other owners' resources. 
    The owner also needs to sign the other owner's key.""",
    tags=["authentication"],
    responses={
        400: {"description": "Validation error."},
        404: {"description": "No such user."},
        403: {"description": "Authentication error."},
        200: {
            "description": "Signature successfully stored.",
            "model": EmptyResponse,
        },
    },
)
async def project_signature_add(
    body: SignProjectRequest,
    request: Request,
    hmq_signature: str = helpers.HMQ_SIGNATURE_HEADER,
):
    owner = validators.owner_exists(body)
    project = validators.project_exists(body)
    await validators.verify_signed_request(
        body, request, body.owner_token, hmq_signature
    )

    auth.db.project_signatures.insert_one(
        {"action": await request.json(), "signature": hmq_signature}
    )
