from pydantic import BaseModel, Field

from fastapi import APIRouter, Request

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


class OwnerFirstTimeAuth(SignedRequest):
    owner_token: str = Field(..., description="Owner token.")
    public_key: str = Field(..., description="Base64-encoded DER format public key.")


@app.post(
    "/owner/activate",
    summary="Initial setup for a new owner.",
    description="The user-supplied public key will be used for future authentication of this owner. Can only be called once for a given owner.",
    tags=["authentication"],
    responses={
        404: {"description": "No such owner."},
        403: {"description": "Authentication error."},
        200: {"description": "Public key stored."},
    },
)
async def owner_activate(body: OwnerFirstTimeAuth, request: Request):
    # validate request
    owner = validators.owner_exists(body)
    validators.owner_has_no_key(owner)
    await validators.public_key_consistent(body, request)

    # handle request
    auth.db.users.update_one(
        {"token": body.owner_token, "is_owner": True},
        {"$set": {"public_key": body.public_key}},
    )
