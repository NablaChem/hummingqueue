# %%
import uuid
import sys
import traceback
import time
import requests
import multiprocessing as mp
import base64
import socket
import urllib3
import cloudpickle
import json
import hashlib
import configparser
from pathlib import Path
from nacl.secret import SecretBox

functions = {}


class API:
    def __init__(self):
        self._box = None
        self._url = None

    def setup(self, url, key):
        config = configparser.ConfigParser()
        config.read(Path("~/.hummingqueue").expanduser() / "config.ini")
        config["server"] = {"url": url, "key": key}
        Path("~/.hummingqueue").expanduser().mkdir(exist_ok=True)
        with open(Path("~/.hummingqueue").expanduser() / "config.ini", "w") as f:
            config.write(f)

    def _clean_instance(self, instance):
        self._verify = True
        if instance.endswith("hmq.localhost.nablachem.org"):
            urllib3.disable_warnings()
            self._verify = False
            return "https://hmq.localhost.nablachem.org"

        # resolve DNS
        try:
            socket.gethostbyname(instance)
        except socket.gaierror:
            raise ValueError("Cannot resolve instance URL.")

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
                response = self._get(f"/ping", baseurl=case)
                if response.status_code == 200 and response.content == b"pong":
                    return case
            except requests.exceptions.SSLError:
                raise ValueError("Instance does not use a valid SSL certificate.")
            except:
                pass

        raise ValueError("Instance is not reachable.")

    def _build_box(self):
        if self._box is None:
            config = configparser.ConfigParser()
            config.read(Path("~/.hummingqueue").expanduser() / "config.ini")
            self._box = SecretBox(base64.b64decode(config["server"]["key"]))
            self._url = self._clean_instance(config["server"]["url"])

    def _encrypt(self, obj, raw=False):
        self._build_box()

        if raw:
            message = obj.encode("utf8")
        else:
            message = json.dumps(obj).encode("utf8")
        message = base64.b64encode(self._box.encrypt(message)).decode("ascii")
        return message

    def _post(self, endpoint, payload):
        response = requests.post(
            self._url + endpoint, json=payload, verify=self._verify
        )
        return response.json()

    def _get(self, endpoint, baseurl=None):
        if baseurl is None:
            baseurl = self._url
        response = requests.get(baseurl + endpoint, verify=self._verify)
        return response

    def register_function(self, remote_function: dict, digest: str):
        remote_function["callable"] = base64.b64encode(
            remote_function["callable"]
        ).decode("ascii")
        payload = {
            "function": self._encrypt(remote_function),
            "digest": digest,
            "challenge": self._encrypt(str(time.time()), raw=True),
        }
        result = self._post("/function/register", payload)
        try:
            if result["status"] == "ok":
                return True
        except:
            pass
        return False

    def submit_tasks(self, tag: str, digest: str, calls: list):
        calls = [self._encrypt(call) for call in calls]
        callstr = json.dumps(calls)
        calldigest = hashlib.sha256(callstr.encode("utf8")).hexdigest()
        payload = {
            "tag": tag,
            "function": digest,
            "calls": callstr,
            "digest": calldigest,
            "challenge": self._encrypt(str(time.time()), raw=True),
        }
        uuids = self._post("/tasks/submit", payload)
        return uuids

    def warm_cache(self, function: str):
        self._build_box()
        if function in functions:
            return

        remote_function = self._box.decrypt(
            base64.b64decode(
                self._get(f"/function/fetch/{function}").json()["function"]
            )
        )
        remote_function = json.loads(remote_function)
        callable = base64.b64decode(remote_function["callable"])
        functions[function] = cloudpickle.loads(callable)

    @staticmethod
    def _worker(hmqid, call, function, secret, baseurl, verify):
        def get_challenge(box):
            payload = str(time.time())
            return base64.b64encode(box.encrypt(payload.encode("utf8"))).decode("ascii")

        box = SecretBox(secret)
        overall_start = time.time()
        errormsg = None
        result = None

        args, kwargs = json.loads(box.decrypt(base64.b64decode(call.encode("utf8"))))
        api.warm_cache(function)

        starttime = time.time()
        try:
            result = functions[function](*args, **kwargs)
        except:
            errormsg = traceback.format_exc()
            result = None
        endtime = time.time()
        result = base64.b64encode(
            box.encrypt(json.dumps(result).encode("utf8"))
        ).decode("ascii")

        overall_end = time.time()
        payload = {
            "task": hmqid,
            "duration": endtime - starttime,
            "walltime": overall_end - overall_start,
        }

        if errormsg is not None:
            payload["error"] = errormsg
        else:
            payload["result"] = result
        payload = {"results": [payload], "challenge": get_challenge(box)}

        print(payload)
        res = requests.post(
            f"{baseurl}/results/store",
            json=payload,
            verify=verify,
        )

    def _get_challenge(self):
        return str(time.time())


api = API()


class Tag:
    def __init__(self, name):
        self.name = name + "_" + str(uuid.uuid4())
        self.tasks = []
        self._results = {}
        self._errors = {}

    def _add_uuids(self, uuids: list):
        self.tasks += uuids

    def to_file(self, filename):
        meta = {
            "name": self.name,
            "ntasks": len(self.tasks),
            "status": {
                "PENDING": len(self.tasks) - len(self._results),
                "DONE": len(self._results),
                "FAILED": len(self._errors),
            },
        }
        payload = json.dumps(
            {"tasks": self.tasks, "results": self._results, "errors": self._errors}
        )
        with open(filename, "w") as f:
            f.write(json.dumps(meta) + "\n" + payload)

    @staticmethod
    def from_file(filename):
        meta = None
        for line in open(filename):
            if meta is None:
                meta = json.loads(line.strip())
            else:
                payload = json.loads(line.strip())
                t = Tag(meta["name"])
                t.tasks = payload["tasks"]
                t._results = payload["results"]
                t._errors = payload["errors"]
        return t

    def wait(self, keep=False):
        """Waits for all tasks in a tag and deletes them from the queue unless keep is specified."""
        missing = [
            _ for _ in self.tasks if _ not in self._results and _ not in self._errors
        ]

        while len(self.tasks) > len(self._results) + len(self._errors):
            time.sleep(1)

            # retrieve all
            payload = {
                "challenge": self._encrypt(self._get_challenge(), raw=True),
                "count": 1000,
            }
        # try:
        #    tasks = self._post(f"/task/fetch", payload)

    def retrieve(self):
        ...


def setup(url, key):
    api.setup(url, key)


def task(func):
    def wrapper(*args, **kwargs):
        func._calls.append((args, kwargs))

    def submit(tag=None):
        # register function with server
        callable = cloudpickle.dumps(func)

        requirements = {
            "python.major": sys.version_info.major,
            "python.minor": sys.version_info.minor,
        }
        remote_function = {
            "callable": callable,
            "requirements": requirements,
        }

        digest = hashlib.sha256(callable).hexdigest()
        if not api.register_function(remote_function, digest):
            raise ValueError("Could not register function with server.")

        # send calls to server
        tag = Tag(func.__name__)
        tag._add_uuids(api.submit_tasks(tag.name, digest, func._calls))
        func._calls = []
        return tag

    wrapper.submit = submit
    func._calls = []

    return wrapper


def unwrap(hmqid: str, call: str, function: str):
    api._build_box()
    api._worker(hmqid, call, function, api._box._key, api._url, api._verify)
