import click
import rsa
import base64
import configparser
import colorama
import sys
from pathlib import Path


def _get_config_dir():
    return Path.expanduser("~/.hummingqueue")


def _get_config_file():
    return _get_config_dir() / "config.ini"


def error(msg):
    print(colorama.Fore.RED + "EE\t" + msg + colorama.Style.RESET_ALL)
    sys.exit(1)


def success(msg):
    print(colorama.Fore.GREEN + "II\t" + msg + colorama.Style.RESET_ALL)


@click.command()
@click.option(
    "instance", help="The base URL of the installation you want to connect to."
)
@click.option("token", help="Your owner token.")
def owner_setup(instance, token):
    """Generates a private key and registers the public counterpart."""
    if _get_config_file().exists():
        error("Setup has been run already.")

    pubkey, privkey = rsa.newkeys(2048)
    pubkey_base64 = base64.b64encode(pubkey.save_pkcs1("DER")).decode("ascii")
    privkey_base64 = base64.b64encode(privkey.save_pkcs1("DER")).decode("ascii")

    # persist key
    config = configparser.ConfigParser()
    config["default"] = {
        "public": pubkey_base64,
        "private": privkey_base64,
        "instance": instance,
    }
    if not _get_config_dir().exists():
        _get_config_dir().mkdir()
    with open(_get_config_file()) as fh:
        config.write(fh)

    # submit key
    payload = {"owner_token": token, "public_key": pubkey}
    payload_json, signature = sign(payload, privkey)


if __name__ == "__main__":
    colorama.init()
    owner_setup()
