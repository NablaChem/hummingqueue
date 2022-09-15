from multiprocessing.sharedctypes import Value
from . import auth
from . import helpers
import secrets
from fastapi import HTTPException, status, Request
import rsa
import base64
from pydantic import BaseModel


def is_admin(body: BaseModel):
    if not secrets.compare_digest(auth.admin_token, body.admin_token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Invalid admin token."
        )


def owner_exists(body: BaseModel):
    """Verifies the owner token resolves to a valid owner."""
    owner = auth.db.users.find_one({"is_owner": True, "owner_token": body.owner_token})
    if owner is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Unknown owner.")

    return owner


def owner_has_no_key(owner: dict):
    """Verifies no public key is set for that owner."""
    if "public_key" in owner:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "Owner already has a public key assigned."
        )


def public_key_consistent(body: BaseModel, request: Request):
    # Request is signed with the public key supplied in the payload
    try:
        pubkey = rsa.PublicKey.load_pkcs1(base64.b64decode(body.public_key), "DER")
    except:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Public key invalid.")

    try:
        rsa.verify(request.body(), request.headers["signature"], pubkey)
    except:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Message signature not valid.")


def compute_token_valid(body: BaseModel, owner: dict):
    if "public_key" not in owner:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "No public key specified for this owner."
        )

    pubkey = rsa.PublicKey.load_pkcs1(base64.b64decode(owner["public_key"]), "DER")
    challenges = helpers.get_valid_challenges()
    valid = False
    for challenge_kind in ["current", "previous"]:
        try:
            rsa.verify(
                challenges[challenge_kind].encode("utf8"), body.compute_token, pubkey
            )
            valid = True
        except:
            pass
    if not valid:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Invalid compute token.")


def body_signed(request: Request, owner: dict):
    if "public_key" not in owner:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "No public key specified for this owner."
        )

    try:
        pubkey = rsa.PublicKey.load_pkcs1(base64.b64decode(owner.public_key), "DER")
    except:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Public key invalid.")

    try:
        rsa.verify(request.body(), request.headers["signature"], pubkey)
    except:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Message signature not valid.")
