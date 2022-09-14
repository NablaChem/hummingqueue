from . import auth
from . import helpers
import secrets
from fastapi import HTTPException
import rsa
import base64


def admin(cls, values):
    if not secrets.compare_digest(auth.admin_token, values["admin_token"]):
        raise HTTPException(status_code=403, detail="Invalid admin token.")
    return values


def owner(cls, values):
    owner_token = values.get("owner_token")

    # owner exists?
    owner = auth.db.users.find_one({"is_owner": True, "owner_token": owner_token})
    if owner is None:
        raise ValueError("Unknown owner.")
    values["owner"] = owner

    return values


def compute_token(cls, values):
    # compute token is correct?
    pubkey = rsa.PublicKey.load_pkcs1(
        base64.b64decode(values["owner"]["public_key"]), "DER"
    )
    challenges = helpers.get_valid_challenges()
    valid = False
    for challenge_kind in ["current", "previous"]:
        try:
            rsa.verify(challenges[challenge_kind].encode("utf8"), compute_token, pubkey)
            valid = True
        except:
            pass
    if not valid:
        raise ValueError("Invalid compute token.")
    return values
