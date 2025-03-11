from pydantic import BaseModel, Field
from typing import List
import base64
import time
from fastapi import APIRouter, HTTPException, status
from nacl.signing import VerifyKey
from pydantic import BaseModel, Field

from .. import auth

app = APIRouter()


class FunctionRegister(BaseModel):
    function: str = Field(..., description="Base64-encoded and encrypted function")
    major: int = Field(..., description="Major version number")
    minor: int = Field(..., description="Minor version number")
    digest: str = Field(..., description="SHA256 digest of function")
    signature: str = Field(..., description="Base64 Signature of the function digest")
    signing_key: str = Field(..., description="Public key of the signing key")
    packages: List[str] = Field(
        ..., description="List of encrypted packages to install"
    )
    packages_hashes: List[str] = Field(
        ..., description="Hashes of the packages to install in same order as packages."
    )
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")


@app.post(
    "/function/register",
    tags=["compute"],
)
def task_register(body: FunctionRegister):
    auth.verify_challenge(body.challenge)

    # verify signature
    owner = auth.db.users.find_one({"sign": body.signing_key})
    if not owner:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Unknown signing key"
        )
    try:
        VerifyKey(base64.b64decode(owner["sign"])).verify(
            body.digest.encode("ascii"), base64.b64decode(body.signature)
        )
    except:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid signature"
        )

    # store function
    if not auth.db.functions.find_one({"digest": body.digest}):
        auth.db.functions.insert_one(
            {
                "digest": body.digest,
                "function": body.function,
                "signature": body.signature,
                "signing_key": body.signing_key,
                "major": body.major,
                "minor": body.minor,
                "packages": dict(zip(body.packages_hashes, body.packages)),
            }
        )
        auth.db.logs.insert_one(
            {"event": "function/register", "id": body.digest, "ts": time.time()}
        )
    return {"status": "ok"}


@app.get(
    "/function/fetch/{digest}",
    tags=["compute"],
)
def function_fetch(digest: str):
    code = auth.db.functions.find_one({"digest": digest})
    if code:
        authorization = auth.db.users.find_one({"sign": code["signing_key"]})
        return {
            "function": code["function"],
            "packages": code["packages"],
            "signature": code["signature"],
            "signing_key": code["signing_key"],
            "authorization": authorization["signature"],
        }
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid digest")
