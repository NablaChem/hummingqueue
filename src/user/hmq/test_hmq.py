from http.client import responses
from click.testing import CliRunner
from . import hmq
import requests


def test_hello_world():
    runner = CliRunner()
    response = requests.post(
        "http://api.lvh.me/owner/create", json={"admin_token": "admin"}
    )
    assert response.status_code == 200
    owner = response.json()["owner_token"]
    result = runner.invoke(hmq.cli, ["owner-init", "http://api.lvh.me", owner])
    assert "Public key set up" in result.output
    assert result.exit_code == 0
