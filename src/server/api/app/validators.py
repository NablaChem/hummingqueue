from multiprocessing.sharedctypes import Value
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
    """Verifies the owner token resolves to a valid owner.

    Parameters
    ----------
    values : dict
        Current request parameters

    Returns
    -------
    dict
        Updated request parameters

    Raises
    ------
    ValueError
        Validation failed.
    """
    owner_token = values.get("owner_token")

    # owner exists?
    owner = auth.db.users.find_one({"is_owner": True, "owner_token": owner_token})
    if owner is None:
        raise ValueError("Unknown owner.")
    values["owner"] = owner

    return values


def owner_no_key(cls, values):
    """Verifies no public key is set for that owner.

    Parameters
    ----------
    values : dict
        Current request parameters

    Returns
    -------
    dict
        Updated request parameters

    Raises
    ------
    ValueError
        Validation failed.
    """
    if "public_key" in values["owner"]:
        raise ValueError("Owner already has a public key assigned.")
    return values


def public_key_consistent(cls, values):
    # Request is signed with the public key supplied in the payload
    try:
        pubkey = rsa.PublicKey.load_pkcs1(base64.b64decode(values["public_key"]), "DER")
    except:
        raise ValueError("Public key invalid.")

    try:
        rsa.verify(body, headersignature, pubkey)
    except:
        raise ValueError("Message signature not valid.")

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
