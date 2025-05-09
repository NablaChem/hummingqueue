from pydantic import BaseModel, Field
import base64
import time
import nacl
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from .. import auth

app = APIRouter()

with open("app/VERSION", "r") as f:
    VERSION = f.read().strip()


@app.get(
    "/auth/challenge",
    tags=["security"],
)
def auth_challenge():
    now = str(time.time())
    return {"challenge": now}


@app.get(
    "/version",
    tags=["security"],
)
def version():
    return {"version": VERSION}


class UserAdd(BaseModel):
    sign: str = Field(..., description="Base64-encoded user signing public key")
    encrypt: str = Field(..., description="Base64-encoded user encryption public key")
    signature: str = Field(
        ..., description="Base64-encoded admin signature of the signing key"
    )
    username: str = Field(..., description="Username")
    compute: str = Field(..., description="Compute secret encrypted for this user.")


@app.post("/user/add", tags=["security"])
async def user_add(body: UserAdd):
    # verify admin signature
    try:
        auth.admin_signature.verify(
            body.sign.encode("ascii"), base64.b64decode(body.signature)
        )
    except:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid admin signature"
        )

    await auth.db.users.insert_one(
        {
            "sign": body.sign,
            "encrypt": body.encrypt,
            "signature": body.signature,
            "username": body.username,
            "compute": body.compute,
        }
    )
    return {"status": "ok"}


class UserSecrets(BaseModel):
    sign: str = Field(..., description="Base64-encoded user signing public key")


@app.post("/user/secrets", tags=["security"])
async def user_secrets(body: UserSecrets):
    entry = await auth.db.users.find_one({"sign": body.sign})
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Invalid user signing key"
        )

    return {
        "compute": entry["compute"],
        "admin": auth.admin_signature.encode(
            encoder=nacl.encoding.Base64Encoder
        ).decode("ascii"),
    }
