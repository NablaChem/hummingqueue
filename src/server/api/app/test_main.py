from fastapi.testclient import TestClient
import time
import rsa
import base64
import json

from .main import app
from . import auth

client = TestClient(app)


def test_read_main():
    response = client.get("/challenge")
    assert response.status_code == 200
    renew_a = response.json()["renew_in"]
    time.sleep(3)
    response = client.get("/challenge")
    assert response.status_code == 200
    renew_b = response.json()["renew_in"]
    assert 2.9 < renew_a - renew_b < 3.1


def test_create_owner():
    response = client.post(
        "/owner/create", json={"admin_token": "WRONGTOKEN" + auth.admin_token}
    )
    assert response.status_code == 403
    response = client.post("/owner/create", json={"admin_token": auth.admin_token})
    assert response.status_code == 200


def test_activate_owner():
    response = client.get("/challenge")
    challenge = response.json()["challenge"]

    response = client.post("/owner/create", json={"admin_token": auth.admin_token})
    owner_token = response.json()["owner_token"]

    pubkey, privkey = rsa.newkeys(2048)
    pubkey_base64 = base64.b64encode(pubkey.save_pkcs1("DER")).decode("ascii")

    payload = {
        "owner_token": owner_token,
        "public_key": pubkey_base64,
        "challenge": challenge,
    }

    def sign(payload: dict, privkey):
        message = json.dumps(payload).encode("utf8")
        signature = rsa.sign(message, privkey, "SHA-384")
        return message, base64.b64encode(signature).decode("ascii")

    payload, signature = sign(payload, privkey)

    # wrong signature
    response = client.post(
        "/owner/activate",
        data=payload,
        headers={"hmq-signature": "wrong-signature"},
    )
    assert response.status_code == 403

    response = client.post(
        "/owner/activate",
        data=payload,
        headers={"hmq-signature": signature},
    )
    assert response.status_code == 200

    # public key can only be set once
    response = client.post(
        "/owner/activate",
        data=payload,
        headers={"hmq-signature": signature},
    )
    assert response.status_code == 403
