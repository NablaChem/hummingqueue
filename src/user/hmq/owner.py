import click
import base64
import configparser
import sys
from nacl.signing import SigningKey
from nacl.public import PrivateKey
from datetime import datetime, timezone
from pathlib import Path
import json
import enum
import urllib3
import requests
import time
import structlog
import socket

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


def _get_config_dir():
    return Path("~/.hummingqueue").expanduser()


def _get_config_file():
    return _get_config_dir() / "config.ini"


def warning(msg, **kwargs):
    log = structlog.get_logger()
    log.warning(msg, **kwargs)


def error(msg, **kwargs):
    log = structlog.get_logger()
    log.error(msg, **kwargs)
    sys.exit(1)


def bug(msg, **kwargs):
    log = structlog.get_logger()
    log.error(msg, **kwargs)
    log.error(
        """This is a bug. Please make sure you updated to the latest version with
    
    pip install --upgrade hmq

If the bug persists after the update, please consider filing a bug report via 
    
    https://github.com/NablaChem/hummingqueue/issues/new
    """
    )
    sys.exit(1)


def success(msg, **kwargs):
    log = structlog.get_logger()
    log.info(msg, **kwargs)


@click.group()
def owner_init_group():
    pass


class APIRole(str, enum.Enum):
    OWNER = "owner"
    USER = "user"
    NONE = "none"


class API:
    def __init__(self, instance):
        self._instance = self._clean_instance(instance)
        self._challenge = None

        # load key from config
        config = configparser.ConfigParser()
        config.read(_get_config_file())
        self._signkeys = dict()
        for role in "owner user".split():
            try:
                signkey_base64 = config[role]["sign"]
            except:
                continue
            self._signkeys[role] = SigningKey(
                base64.b64decode(signkey_base64.encode("ascii"))
            )

    def _get(self, url, **kwargs):
        return requests.get(url, verify=self._verify, **kwargs)

    def _post(self, url, **kwargs):
        return requests.post(url, verify=self._verify, **kwargs)

    def _clean_instance(self, instance):
        self._verify = True
        if instance.endswith("hmq.localhost.nablachem.org"):
            urllib3.disable_warnings()
            warning("Detected development domain: allowing self-signed certificates.")
            self._verify = False

        # resolve DNS
        try:
            socket.gethostbyname(instance)
        except socket.gaierror:
            error("Cannot resolve instance URL.")

        # try to add http/https if not present
        cases = []
        if instance.startswith("https://") or instance.startswith("http://"):
            cases.append(instance)
        else:
            for schema in "https http".split():
                cases.append(f"{schema}://{instance}")

        # test if instance is reachable
        for case in cases:
            try:
                response = self._get(f"{case}/ping")
                if response.status_code == 200 and response.content == b"pong":
                    return case
            except requests.exceptions.SSLError:
                error("Instance does not use a valid SSL certificate.")
            except:
                pass

        error("Instance does not answer as expected. Is the instance URL correct?")

    def _sign(self, payload, role: APIRole):
        message = json.dumps(payload, sort_keys=True).encode("utf8")
        signature = None
        if role is not APIRole.NONE:
            signed = self._signkeys[role.value].sign(message)
            signature = base64.b64encode(signed.signature).decode("ascii")
        return message, signature

    def _update_challenge(self):
        if self._challenge is None or self._renew_at < time.time():
            try:
                response = self._get(f"{self._instance}/challenge").json()
            except:
                error("Unable to reach instance to fetch current challenge.")
            self._challenge = response["challenge"]
            self._renew_at = time.time() + response["renew_in"]

    def post(self, endpoint: str, payload: dict, role: APIRole):
        if role is not APIRole.NONE:
            self._update_challenge()
            payload["challenge"] = self._challenge
            payload["time"] = int(datetime.now(timezone.utc).timestamp())

        message, signature = self._sign(payload, role)
        headers = {}
        if role is not APIRole.NONE:
            headers = {"hmq-signature": signature}

        request = self._post(
            f"{self._instance}/{endpoint}",
            data=message,
            headers=headers,
        )
        response = request.content
        message = None
        if str(request.status_code).startswith("2"):
            try:
                response = request.json()
            except:
                bug("Server response is not JSON.")
        else:
            try:
                errordesc = request.json()
            except:
                bug("Server response is not JSON, indicating a server error.")

            # field missing?
            msg = None
            try:
                msg = errordesc["detail"][0]["msg"]
            except:
                pass
            if msg == "field required":
                bug(f"Unable to communicate with instance via {endpoint}: {errordesc}")

            # regular error with message?
            try:
                if errordesc == dict(detail=errordesc["detail"]):
                    return response, errordesc["detail"], request.status_code
            except:
                pass

            bug(f"Unable to deal with server response from {endpoint}: {errordesc}")
        return response, message, request.status_code


KeySet = collections.namedtuple("KeySet", "sign verify encrypt public")


def _generate_keys():
    signing_key = SigningKey.generate()
    verify_key = signing_key.verify_key
    signing_key_base64 = base64.b64encode(signing_key.encode()).decode("ascii")
    verify_key_base64 = base64.b64encode(verify_key.encode()).decode("ascii")

    # generate encryption key
    encryption_key = PrivateKey.generate()
    encryption_key_base64 = base64.b64encode(encryption_key.encode()).decode("ascii")
    public_key = encryption_key.public_key
    public_key_base64 = base64.b64encode(public_key.encode()).decode("ascii")
    return KeySet(
        signing_key_base64, verify_key_base64, encryption_key_base64, public_key_base64
    )


def _submit_keys(keys: KeySet, role: APIRole, instance: str, token: str):
    prefix = "owner"
    if role is APIRole.USER:
        prefix = "user"
    # submit signing key
    payload = {f"{prefix}_token": token, "verify_key": keys.verify}
    api = API(instance)
    response, message, status = api.post("{prefix}/activate", payload, role=role)
    if status != 200:
        error("Unable to upload signing key", error=message)
    success("Owner signing key set up.", instance=instance)

    # submit encryption key
    payload = {
        f"{prefix}_token": token,
        "key": keys.public,
        "reason": "ADD_OWN_ENCRYPTION_KEY",
    }
    response, message, status = api.post("sign/key", payload, role=role)
    if status != 200:
        error("Unable to upload encryption key.", error=message)
    success("Owner encryption key set up.", instance=instance)


def _create_user(instance: str):
    api = API(instance)
    response, message, status = api.post("user/create", {}, role=APIRole.NONE)
    user = response["user_token"]
    if status != 200:
        error("Unable to generate own user token.", error=message)
    success("Generated new user token.", instance=instance, token=user)
    return user


def _authorize_user(instance, owner, user_key):
    api = API(instance)
    payload = {
        "owner_token": owner,
        "key": user_key,
        "reason": "AUTHORIZE_USER",
    }
    response, message, status = api.post("sign/key", payload, role=APIRole.OWNER)
    if status != 200:
        error("Unable to authorize user.", error=message)
    success("Authorized user.", instance=instance)


@owner_init_group.command(context_settings=CONTEXT_SETTINGS)
@click.option("--instance", default="https://hmq.nablachem.org", help="Instance URL")
@click.argument("owner")
def owner_init(instance, owner):
    """Initializes a hardware owner account. You will need to obtain an owner token from the instance operator.

    This is the very first step of onboarding new resources. Generates a private key and registers the public counterpart at the hummingqueue installation INSTANCE for the user TOKEN."""
    if _get_config_file().exists():
        error("Setup has been run already.")

    keys_owner = _generate_keys()
    _submit_keys(keys_owner, APIRole.OWNER, instance, owner)

    # persist keys
    config = configparser.ConfigParser()
    config["owner"] = {
        "sign": keys_owner.sign,
        "encrypt": keys_owner.encrypt,
        "instance": instance,
        "token": owner,
    }
    if not _get_config_dir().exists():
        _get_config_dir().mkdir()
    with open(_get_config_file(), "w") as fh:
        config.write(fh)

    # create user
    user = _create_user(instance)
    keys_user = _generate_keys()

    config["user"] = {
        "sign": keys_user.sign,
        "encrypt": keys_user.encrypt,
        "instance": instance,
        "token": user,
    }
    with open(_get_config_file(), "w") as fh:
        config.write(fh)

    _submit_keys(keys_user, APIRole.User, instance, user)

    # authorize user
    _authorize_user(instance, owner, keys_user.verify)

    config["aliases"] = {user: "MYSELF"}
    with open(_get_config_file(), "w") as fh:
        config.write(fh)
