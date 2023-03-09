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


class SignatureReason(str, enum.Enum):
    ADD_OWN_ENCRYPTION_KEY = "ADD_OWN_ENCRYPTION_KEY"


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
    reason: SignatureReason = Field(..., description="Type of signature.")
    key: str = Field(..., description="Base64-encoded key to be signed.")


@app.post(
    "/sign/key",
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
        {
            "action": await request.json(),
            "signature": hmq_signature,
            "user_token": user_or_owner_token,
        }
    )
