from multiprocessing.sharedctypes import Value
from . import auth
from . import helpers
import secrets
import json
from fastapi import HTTPException, status, Request
from nacl.signing import VerifyKey
import nacl
import base64
from pydantic import BaseModel


def is_admin(body: BaseModel):
    if not secrets.compare_digest(auth.admin_token, body.admin_token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Invalid admin token."
        )


def owner_exists(body: BaseModel):
    """Verifies the owner token resolves to a valid owner."""
    owner = auth.db.users.find_one({"is_owner": True, "token": body.owner_token})
    if owner is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Unknown owner.")

    return owner


def user_exists(body: BaseModel):
    """Verifies the user token resolves to a valid user."""
    user = auth.db.users.find_one({"is_owner": False, "token": body.user_token})
    if user is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Unknown user.")

    return user


def user_or_owner_exists(body: BaseModel) -> str:
    checked_owner = False
    checked_user = False

    if "user_token" in body.dict():
        checked_owner = True
        owner = auth.db.users.find_one({"is_owner": True, "token": body.owner_token})
        if owner is not None:
            return owner["token"]

    if "user_token" in body.dict():
        checked_user = True
        user = auth.db.users.find_one({"is_owner": False, "token": body.user_token})
        if user is not None:
            return user["token"]

    if checked_owner or checked_user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Unknown user or owner.")
    else:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, "No user or owner token supplied."
        )


def owner_has_no_key(owner: dict):
    """Verifies no verify key is set for that owner."""
    if "verify_key" in owner:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "Owner already has a verify key assigned."
        )


def user_has_no_key(user: dict):
    """Verifies no verify key is set for that user."""
    if "verify_key" in user:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "User already has a verify key assigned."
        )


def time_window_valid(model: BaseModel):
    if abs(helpers.utc_now() - model.time) > auth.MAXIMUM_REQUEST_AGE_SECONDS:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"Message timestamp outside of time window of {auth.MAXIMUM_REQUEST_AGE_SECONDS} seconds.",
        )


async def verify_signed_request(
    model: BaseModel,
    request: Request,
    authority: str,
    hmq_signature: str = helpers.HMQ_SIGNATURE_HEADER,
):
    # check time
    time_window_valid(model)

    # challenge valid
    if model.challenge not in helpers.get_valid_challenges()[0].values():
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "Challenge not valid.",
        )

    # signature valid
    try:
        verify_key = helpers.get_verify_key(authority)
    except:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "No verify key found for this user or owner token.",
        )

    payload = json.dumps(await request.json(), sort_keys=True).encode("utf-8")
    try:
        verify_key.verify(
            payload,
            base64.b64decode(hmq_signature.encode("ascii")),
        )
    except:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "Signature verification failed.",
        )
    return payload


async def public_key_consistent(body: BaseModel, request: Request):
    # Request is signed with the public key supplied in the payload
    try:
        verify_key = VerifyKey(base64.b64decode(body.verify_key))
    except:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Verify key invalid.")

    try:
        verify_key.verify(
            await request.body(),
            base64.b64decode(request.headers["hmq-signature"].encode("ascii")),
        )
    except:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Message signature not valid.")


def compute_token_valid(body: BaseModel, owner: dict):
    if "public_key" not in owner:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "No public key specified for this owner."
        )

    verify_key = VerifyKey(base64.b64decode(owner["verify_key"]))
    challenges = helpers.get_valid_challenges()
    valid = False
    for challenge_kind in ["current", "previous"]:
        try:
            verify_key.verify(
                challenges[challenge_kind].encode("utf8"),
                body.compute_token,
            )
            valid = True
        except nacl.exceptions.BadSignatureError:
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
