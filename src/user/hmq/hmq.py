import uuid
import inspect
import typing
import sys
import traceback
import os
import time
import importlib.metadata
import requests
import base64
import socket
import urllib3
import cloudpickle
import json
import hashlib
import configparser
import warnings
import functools
import numpy as np
import pandas as pd
import sqlite3
import zlib
import secrets
from pathlib import Path
import nacl
from nacl.secret import SecretBox
from nacl.public import PrivateKey, SealedBox, PublicKey
from nacl.signing import SigningKey, VerifyKey
import tqdm
from rq import Worker
from urllib.parse import urlparse, ParseResult

functions = {}


# TODO HERE: add to director, read from toml
def enable_sentry(sentry_dsn, gateway_port=None):
    if sentry_dsn == "":
        return

    import sentry_sdk

    if gateway_port is None:
        gateway_port = 443

    original = urlparse(sentry_dsn)
    new = ParseResult(
        scheme=original.scheme,
        netloc="{}:{}".format(original.hostname, gateway_port),
        path=original.path,
        params=original.params,
        query=original.query,
        fragment=original.fragment,
    )
    sentry_sdk.init(dsn=new.geturl())


class API:
    def __init__(self):
        self._box = None
        self._url = None
        self._challenge_time = 0
        self._session = None

    def setup(self, url: str):
        config = configparser.ConfigParser()
        config.read(Path("~/.hummingqueue").expanduser() / "config.ini")

        # generate new random user key
        if "server" in config:
            if config["server"]["url"] == url:
                user_signing_key = SigningKey(
                    config["server"]["sign"], encoder=nacl.encoding.Base64Encoder
                )
                user_encryption_key = PrivateKey(
                    config["server"]["encrypt"], encoder=nacl.encoding.Base64Encoder
                )
            else:
                raise ValueError(
                    "Already configured for another instance. Currently, only one instance is supported."
                )
        else:
            user_signing_key = SigningKey.generate()
            user_encryption_key = PrivateKey.generate()
        sign_b64 = user_signing_key.encode(encoder=nacl.encoding.Base64Encoder).decode(
            "ascii"
        )
        encrypt_b64 = user_encryption_key.encode(
            encoder=nacl.encoding.Base64Encoder
        ).decode("ascii")
        config["server"] = {"url": url, "sign": sign_b64, "encrypt": encrypt_b64}

        Path("~/.hummingqueue").expanduser().mkdir(exist_ok=True)
        with open(Path("~/.hummingqueue").expanduser() / "config.ini", "w") as f:
            config.write(f)

        # build registration string
        verify_key = user_signing_key.verify_key.encode(
            encoder=nacl.encoding.Base64Encoder
        ).decode("ascii")
        public_key = user_encryption_key.public_key.encode(
            encoder=nacl.encoding.Base64Encoder
        ).decode("ascii")
        return f"ADD-USER_{verify_key}_{public_key}"

    def grant_access(self, signing_request: str, adminkey: str, username: str):
        self._build_box()

        prefix, verify_key, public_key = signing_request.split("_")
        if prefix != "ADD-USER":
            raise ValueError("Invalid signing request.")

        # sign to give permission
        admin_signing_key = SigningKey(adminkey, encoder=nacl.encoding.Base64Encoder)
        signature = admin_signing_key.sign(verify_key.encode("ascii")).signature
        b64_signature = base64.b64encode(signature).decode("ascii")

        # encrypt compute secret for user
        user_encryption_key = base64.b64decode(public_key)
        box = SealedBox(PublicKey(user_encryption_key))
        encrypted_secret = base64.b64encode(box.encrypt(self._computesecret)).decode(
            "ascii"
        )

        payload = {
            "sign": verify_key,
            "encrypt": public_key,
            "signature": b64_signature,
            "username": username,
            "compute": encrypted_secret,
        }
        try:
            response = self._post("/user/add", payload)
        except:
            raise ValueError("Cannot submit signature. Is the admin key correct?")

    def ping(self):
        self._build_box()
        try:
            response = self._get("/ping").content.decode("ascii")
        except:
            return False
        return response == "pong"

    def _clean_instance(self, instance):
        self._verify = True
        if instance.endswith("hmq.localhost.nablachem.org"):
            urllib3.disable_warnings()
            self._verify = False
            return "https://hmq.localhost.nablachem.org"

        # resolve DNS
        try:
            socket.gethostbyname(instance.split(":")[0])
        except socket.gaierror:
            raise ValueError("Cannot resolve instance URL.")

        # try to add http/https if not present
        cases = []
        if instance.startswith("https://") or instance.startswith("http://"):
            cases.append(instance)
        else:
            for schema in "http https".split():
                cases.append(f"{schema}://{instance}")

        # test if instance is reachable
        for case in cases:
            try:
                response = self._get(f"/ping", baseurl=case)
                if response.status_code == 200 and response.content == b"pong":
                    break
            except requests.exceptions.SSLError:
                raise ValueError("Instance does not use a valid SSL certificate.")
            except:
                pass
        else:
            raise ValueError("Instance is not reachable.")

        # test for version match
        server_version = self._get(f"/version", baseurl=case).json()["version"]
        client_version = importlib.metadata.version("hmq")
        if server_version != client_version:
            warnings.warn(
                f"Version mismatch. Server is {server_version}, client is {client_version}. Please update: pip install --upgrade hmq",
                DeprecationWarning,
            )
        return case

    def _build_box(self, offline=False):
        if self._box is None:
            config = configparser.ConfigParser()
            config.read(Path("~/.hummingqueue").expanduser() / "config.ini")
            if offline:
                self._url = config["server"]["url"]
            else:
                self._url = self._clean_instance(config["server"]["url"])
            self._signature = SigningKey(
                config["server"]["sign"], encoder=nacl.encoding.Base64Encoder
            )
            try:
                self._computesecret = base64.b64decode(config["server"]["compute"])
            except:
                # no compute secret yet, need to fetch it
                public_key = self._signature.verify_key.encode(
                    encoder=nacl.encoding.Base64Encoder
                ).decode("ascii")
                response = self._post("/user/secrets", {"sign": public_key})

                # decrypt response
                user_encryption_key = PrivateKey(
                    config["server"]["encrypt"], encoder=nacl.encoding.Base64Encoder
                )
                userbox = SealedBox(user_encryption_key)
                for item in "compute".split():
                    self._computesecret = userbox.decrypt(
                        base64.b64decode(response[item])
                    )
                    config["server"][item] = base64.b64encode(
                        self._computesecret
                    ).decode("ascii")
                config["server"]["admin"] = response["admin"]

                # save result
                with open(
                    Path("~/.hummingqueue").expanduser() / "config.ini", "w"
                ) as f:
                    config.write(f)

                # finally load compute secret
                self._computesecret = base64.b64decode(config["server"]["compute"])
            try:
                self._adminkey = base64.b64decode(config["server"]["admin"])
            except:
                pass
            self._box = SecretBox(self._computesecret)

    def _encrypt(self, obj, raw=False) -> str:
        self._build_box()

        if raw:
            message = obj.encode("utf8")
        else:
            message = json.dumps(obj, cls=AutomaticEncoder).encode("utf8")
        message = base64.b64encode(self._box.encrypt(message)).decode("ascii")
        return message

    def _decrypt(self, obj, raw=False):
        self._build_box()

        message = obj.encode("utf-8")
        intermediate = self._box.decrypt(base64.b64decode(message.decode("ascii")))
        if raw:
            return intermediate

        message = json.loads(intermediate)
        return message

    def _hash(self, obj):
        self._build_box()

        message = obj.encode("utf-8")
        return hashlib.sha256(self._computesecret + message).hexdigest()

    def missing_dependencies(self, datacenter, installlist, python_version):
        payload = {
            "datacenter": datacenter,
            "version": python_version,
            "challenge": self._get_challenge(),
        }
        requirements = self._post("/queue/requirements", payload)

        # decrypt requirements
        requirements = [
            self._decrypt(_, raw=True).decode("ascii") for _ in requirements
        ]

        try:
            with open(installlist, "r") as fh:
                installed = fh.read().splitlines()
        except FileNotFoundError:
            installed = []
        return [req for req in requirements if req not in installed]

    def _init_session(self):
        if self._session is None:
            self._session = requests.Session()

    def _post(self, endpoint, payload):
        self._init_session()
        if endpoint != "/user/secrets":
            self._build_box()

        if os.getenv("HMQ_DEBUG") is not None:
            print(f"### POST {self._url}{endpoint}")
            print(json.dumps(payload, indent=4))
            print("# " + "-" * 30)

        response = self._session.post(
            self._url + endpoint, json=payload, verify=self._verify
        )
        if os.getenv("HMQ_DEBUG") is not None:
            print(f"# RESPONSE {response.status_code}")
            print("# " + "-" * 30)
            try:
                print(json.dumps(response.json(), indent=4))
            except:
                print(response.content)
            print("#" * 30)
        if response.status_code in (404, 422):
            raise ValueError("Unable to post request.")
        return response.json()

    def _get(self, endpoint, baseurl=None):
        self._init_session()
        if baseurl is None:
            baseurl = self._url
        response = self._session.get(baseurl + endpoint, verify=self._verify)
        return response

    def register_function(self, remote_function: dict, digest: str):
        self._build_box()

        remote_function["callable"] = base64.b64encode(
            remote_function["callable"]
        ).decode("ascii")

        # sign digest
        signature = self._signature.sign(digest.encode("ascii")).signature
        b64_signature = base64.b64encode(signature).decode("ascii")

        payload = {
            "function": self._encrypt(remote_function),
            "digest": digest,
            "signature": b64_signature,
            "signing_key": self._signature.verify_key.encode(
                encoder=nacl.encoding.Base64Encoder
            ).decode("ascii"),
            "major": sys.version_info.major,
            "minor": sys.version_info.minor,
            "packages": [
                self._encrypt(_, raw=True) for _ in remote_function["packages"]
            ],
            "packages_hashes": [self._hash(_) for _ in remote_function["packages"]],
            "challenge": self._get_challenge(),
        }
        result = self._post("/function/register", payload)
        try:
            if result["status"] == "ok":
                return True
        except:
            pass

        return False

    def retrieve_results(self, tasks: list[str]):
        payload = {
            "tasks": tasks,
            "challenge": self._get_challenge(),
        }
        results = {}
        for task, result in self._post("/results/retrieve", payload).items():
            if result is None:
                results[task] = result
                continue
            for key in "result error".split():
                if result[key]:
                    # queue messages are not encrypted, results/compute errors are
                    if not result[key].startswith("Hummingqueue"):
                        result[key] = self._decrypt(result[key])
            results[task] = result
        return results

    def submit_tasks(
        self,
        tag: str,
        digest: str,
        calls: list,
        ncores: int = 1,
        datacenters: list = None,
    ):
        calls = [self._encrypt(call) for call in calls]

        uuids = []
        limit_mb = 14
        limit = limit_mb * 1024 * 1024
        while len(calls) > 0:
            chunk = [calls.pop(0)]
            chunk_length = len(chunk[0])
            while chunk_length < limit and len(calls) > 0:
                next_length = len(calls[0])
                if chunk_length + next_length > limit:
                    break
                chunk.append(calls.pop(0))
                chunk_length += len(chunk[-1])

            if chunk_length > limit:
                raise ValueError(
                    f"Chunk size exceeded {limit_mb} MB limit: cannot submit task."
                )

            chunkstr = json.dumps(chunk)
            chunk_digest = hashlib.sha256(chunkstr.encode("utf8")).hexdigest()
            payload = {
                "tag": tag,
                "function": digest,
                "calls": chunkstr,
                "digest": chunk_digest,
                "ncores": ncores,
                "datacenters": datacenters,
                "challenge": self._get_challenge(),
            }
            uuids += self._post("/tasks/submit", payload)

        return uuids

    def find_tasks(self, tag: str):
        payload = {"tag": tag, "challenge": self._get_challenge()}
        tasks = self._post("/tasks/find", payload)
        return tasks

    def sync_tasks(self, datacenter: str, known: list[str]) -> list[str]:
        payload = {
            "datacenter": datacenter,
            "known": known,
            "challenge": self._get_challenge(),
        }
        stale = self._post("/tasks/sync", payload)
        return stale

    def get_tasks_status(self, tasks: list[str]):
        payload = {
            "tasks": tasks,
            "challenge": self._get_challenge(),
        }
        return self._post("/tasks/inspect", payload)

    def delete_tasks(self, tasks: list[str]):
        payload = {
            "delete": tasks,
            "challenge": self._get_challenge(),
        }
        deleted = self._post("/tasks/delete", payload)
        return deleted

    def fetch_function(self, function: str):
        return self._get(f"/function/fetch/{function}").content

    def warm_cache(self, function: str, payload):
        self._build_box(offline=True)
        if function in functions:
            return

        # decrypt payload
        remote_function = self._decrypt(payload["function"])
        callable = base64.b64decode(remote_function["callable"])

        # verify digest
        digest = hashlib.sha256(callable).hexdigest()
        if function != digest:
            raise ValueError("Function digest does not match.")

        # verify digest signature
        signature = base64.b64decode(payload["signature"])
        verify_key = VerifyKey(base64.b64decode(payload["signing_key"]))
        try:
            verify_key.verify(function.encode("ascii"), signature)
        except:
            raise ValueError("Cannot verify function signature.")

        # verify user authorization
        signature = base64.b64decode(payload["authorization"])
        admin_verify = VerifyKey(self._adminkey)
        try:
            admin_verify.verify(payload["signing_key"].encode("ascii"), signature)
        except:
            raise ValueError("Cannot verify function authorization.")

        try:
            functions[function] = cloudpickle.loads(callable)
        except Exception as e:
            msg = "Cannot deserialize function: " + repr(e)

            def raise_later(*args, **kwargs):
                raise ValueError(kwargs["hmq_message"])

            functions[function] = functools.partial(raise_later, hmq_message=msg)

    def dequeue_tasks(
        self,
        datacenter: str,
        packagelists: dict[str, str],
        maxtasks: int,
        available: int,
        allocated: int,
        running: int,
        used: int,
    ):
        # build list of packages
        packages = {}
        for pyver, packagelist in packagelists.items():
            with open(packagelist, "r") as fh:
                ps = fh.read().splitlines()

            packages[pyver] = list(map(self._hash, ps))

        payload = {
            "datacenter": datacenter,
            "challenge": self._get_challenge(),
            "maxtasks": maxtasks,
            "packages": packages,
            "available": available,
            "allocated": allocated,
            "running": running,
            "used": used,
        }
        return self._post("/tasks/dequeue", payload)

    def store_results(self, results: list):
        payload = {
            "results": results,
            "challenge": self._get_challenge(),
        }
        return self._post("/results/store", payload)

    @staticmethod
    def _worker(hmqid, call, function, computesecret):
        overall_start = time.time()
        box = SecretBox(computesecret)
        errormsg = None
        result = None

        args, kwargs = json.loads(box.decrypt(base64.b64decode(call.encode("utf8"))))

        starttime = time.time()
        try:
            result = functions[function](*args, **kwargs)
            result = base64.b64encode(
                box.encrypt(json.dumps(result, cls=AutomaticEncoder).encode("utf8"))
            ).decode("ascii")
            errormsg = None
        except:
            errormsg = traceback.format_exc()
            errormsg = base64.b64encode(
                box.encrypt(json.dumps(errormsg).encode("utf8"))
            ).decode("ascii")
            result = None
        endtime = time.time()

        # limit result size
        if result is not None:
            result_kb = len(result) / 1024
            allowed_kb = 250
            if result_kb > allowed_kb:
                result = None
                errormsg = f"Result size was too large: {result_kb} KB is more than the allowed {allowed_kb} KB."
                errormsg = base64.b64encode(
                    box.encrypt(json.dumps(errormsg).encode("utf8"))
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

        return payload

    def _get_challenge(self) -> str:
        """Uses the server provided challenge for up to ten seconds.

        Returns
        -------
        str
            Challenge
        """
        if time.time() - self._challenge_time > 10:
            self._build_box()
            response = str(int(float(self._get("/auth/challenge").json()["challenge"])))

            self._challenge = self._signature.sign(
                response.encode("ascii"), encoder=nacl.encoding.Base64Encoder
            ).decode("ascii")
            self._challenge_time = time.time()
        return self._challenge


class AutomaticEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            warnings.warn("Implicit conversion from numpy.integer to int.")
            return int(obj)
        elif isinstance(obj, np.floating):
            warnings.warn("Implicit conversion from numpy.floating to float.")
            return float(obj)
        elif isinstance(obj, np.ndarray):
            warnings.warn("Implicit conversion from numpy.ndarray to list.")
            return obj.tolist()
        elif isinstance(obj, pd.DataFrame):
            warnings.warn("Implicit conversion from pandas.DataFrame to list.")
            return obj.to_dict(orient="list")
        elif isinstance(obj, pd.Series):
            warnings.warn("Implicit conversion from pandas.Series to list.")
            return obj.to_list()
        return json.JSONEncoder.default(self, obj)


api = API()


class Tag:
    """Represents a collection of tasks."""

    def __init__(self, name):
        self._db = Tag._create_database()
        self.name = name + "_" + str(uuid.uuid4())

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value):
        self._name = value
        self._db.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('name', ?)", (value,)
        )

    @staticmethod
    def _create_database() -> sqlite3.Connection:
        """Creates an in-memory database for the tag with the current schema.

        Returns
        -------
        sqlite3.Connection
            The database.
        """
        db = sqlite3.connect(":memory:")
        db.execute(
            "CREATE TABLE tasks (task TEXT PRIMARY KEY, result BLOB, error BLOB) WITHOUT ROWID"
        )
        db.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        return db

    def _add_tasks(self, tasks: list[str]):
        """Registers a list of tasks with the tag.

        Parameters
        ----------
        tasks : list[str]
            List of task IDs.
        """
        with self._db:
            self._db.executemany(
                "INSERT INTO tasks (task) VALUES (?)", [(task,) for task in tasks]
            )

    def to_file(self, filename: str, stay_linked: bool = False):
        """Exports the current state to a file.

        Parameters
        ----------
        filename : str
            Export destination.
        stay_linked: bool
            If True, will switch this tag to a file-based database.
        """
        destination = sqlite3.connect(filename)
        with destination:
            self._db.backup(destination)
        if stay_linked:
            self._db.close()
            self._db = destination
        else:
            destination.close()

    @staticmethod
    def from_queue(tag: str) -> "Tag":
        """Loads a tag from the queue by downloading the corresponding tasks.

        Does not pull results.

        Parameters
        ----------
        tag : str
            The tag name.

        Returns
        -------
        Tag
            Populated object.
        """
        t = Tag("")
        t.name = tag
        t._add_tasks(api.find_tasks(tag))
        return t

    @staticmethod
    def from_file(filename: str) -> "Tag":
        """Loads a tag and all results for all tasks from a file.

        Parameters
        ----------
        filename : str
            Source file.

        Returns
        -------
        Tag
            The loaded tag.

        Raises
        ------
        ValueError
            If the file does not exist.
        """
        if not os.path.exists(filename):
            raise ValueError("File does not exist.")

        # detect whether file is a database or a text-based format
        with open(filename, "rb") as f:
            first_16_bytes = f.read(16)
        if first_16_bytes.startswith(b"SQLite format 3"):
            db = sqlite3.connect(filename)
            t = Tag("")
            t._db = db
            t._name = db.execute(
                "SELECT value FROM meta WHERE key = 'name'"
            ).fetchone()[0]
            return t

        # support older text-based format
        meta = None
        tasks = []
        rows = []
        for line in open(filename):
            if meta is None:
                meta = json.loads(line.strip())
            else:
                row = json.loads(line.strip())
                tasks.append(row["task"])
                rows.append((row["task"], row["result"], row["error"]))

        # convert to modern database format
        t = Tag("")
        t.name = meta["name"]
        with t._db:
            t._db.executemany(
                "INSERT INTO tasks (task, result, error) VALUES (?, ?, ?)", rows
            )
        return t

    @staticmethod
    def _to_blob(obj) -> bytes:
        return zlib.compress(json.dumps(obj).encode("utf-8"))

    @staticmethod
    def _from_blob(blob: bytes):
        return json.loads(zlib.decompress(blob).decode("utf-8"))

    def _pull_batch(self, tasks: list[str]):
        """Pulls data for a batch of tasks.

        Parameters
        ----------
        tasks : list[str]
            Task IDs.
        """
        if len(tasks) > 0:
            results = api.retrieve_results(tasks)
            updates = []
            for task, result in results.items():
                if result is not None:
                    updates.append(
                        (
                            Tag._to_blob(result["result"]),
                            Tag._to_blob(result["error"]),
                            task,
                        )
                    )

            with self._db:
                self._db.executemany(
                    "UPDATE tasks SET result = ?, error = ? WHERE task = ?", updates
                )

    @property
    def _open_tasks(self) -> list[str]:
        """Finds all tasks without results or errors.

        Returns
        -------
        list[str]
            Task IDs.
        """
        open_tasks = self._db.execute(
            "SELECT task FROM tasks WHERE result IS NULL AND error IS NULL"
        ).fetchall()
        return [_[0] for _ in open_tasks]

    def pull(self, blocking: bool = False, batchsize: int = 100) -> int:
        """Downloads data from the queue for all tasks in the tag.

        Parameters
        ----------
        blocking : bool, optional
            Whether to retry downloading until all data has been fetched, by default False
        batchsize : int, optional
            Number of tasks to download at once, by default 100.

        Returns
        -------
        int
            Number of remaining tasks for which neither result nor error is available.
        """
        open_tasks = self._open_tasks
        total_tasks = self._db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        while len(open_tasks) > 0:
            tasklist = []
            for task in tqdm.tqdm(
                open_tasks,
                desc="Pulling",
                unit="tasks",
                initial=total_tasks - len(open_tasks),
            ):
                tasklist.append(task)
                if len(tasklist) == batchsize:
                    self._pull_batch(tasklist)
                    tasklist = []
            self._pull_batch(tasklist)
            if not blocking:
                break
            time.sleep(5)
        remaining = self._db.execute(
            "SELECT COUNT(*) FROM tasks WHERE result IS NULL AND error IS NULL"
        ).fetchone()[0]
        return remaining

    def delete(self):
        """Delete all remaining tasks from the queue, i.e. abort the remaining calculations."""
        api.delete_tasks(self._open_tasks)

    @property
    def results(self) -> typing.Generator[str, None, None]:
        """Generator of all results.

        The ordering is stable as long as no tasks are added. This ordering is not the same as the order of submission.

        Returns
        -------
        typing.Generator[str, None, None]
            Results, or None if the task has not been completed or an error occurred.
        """
        for (result,) in self._db.execute(
            "SELECT result FROM tasks ORDER BY task"
        ).fetchall():
            yield Tag._from_blob(result)

    @property
    def errors(self) -> typing.Generator[str, None, None]:
        """Generator of all errors.

        The ordering is stable as long as no tasks are added. This ordering is not the same as the order of submission.

        Returns
        -------
        typing.Generator[str, None, None]
            Results, or None if the task has not been completed or an error occurred.
        """
        for (error,) in self._db.execute(
            "SELECT error FROM tasks ORDER BY task"
        ).fetchall():
            yield Tag._from_blob(error)


def request_access(url: str):
    return api.setup(url)


def grant_access(signing_request: str, adminkey: str, username: str):
    api.grant_access(signing_request, adminkey, username)


def task(func=None, **defaultkwargs):
    def task_decorator(func):
        def wrapper(*args, **kwargs):
            if defaultkwargs:
                kwargs.update(defaultkwargs)
                bound = inspect.signature(func).bind(*args, **kwargs)
                kwargs = bound.arguments
                for k in defaultkwargs.keys():
                    del kwargs[k]
                args = ()
            func._calls.append((args, kwargs))

        def submit(
            tag=None, ncores=1, datacenters: str = None, packages=[], quiet=False
        ):
            if not api.ping():
                print("Server is not reachable.")
                return

            # register function with server
            callable = func._callable

            remote_function = {
                "callable": callable,
                "packages": packages,
            }

            digest = hashlib.sha256(callable).hexdigest()
            if not quiet:
                print(f"Registering function with server: {digest}")

            if not api.register_function(remote_function, digest):
                raise ValueError("Could not register function with server.")

            if not quiet:
                print(f"Sending {len(func._calls)} tasks.")

            # send calls to server
            tag = Tag(tag or func.__name__)
            if datacenters is None:
                datacenters = []
            else:
                datacenters = [dc.strip() for dc in datacenters.split(",")]
            tag._add_tasks(
                api.submit_tasks(tag.name, digest, func._calls, ncores, datacenters)
            )
            func._calls = []
            print(f"Completed tag: {tag.name}")
            return tag

        if defaultkwargs:
            func._callable = cloudpickle.dumps(functools.partial(func, **defaultkwargs))
        else:
            func._callable = cloudpickle.dumps(func)
        wrapper.submit = submit
        func._calls = []

        return wrapper

    if func is None:
        return task_decorator
    else:
        return task_decorator(func)


def unwrap(hmqid: str, call: str, function: str):
    api._build_box()
    return api._worker(hmqid, call, function, api._box._key)


class CachedWorker(Worker):
    def execute_job(self, job, queue):
        payload = self.connection.hget("hmq:functions", job.kwargs["function"])
        api.warm_cache(job.kwargs["function"], json.loads(payload.decode("ascii")))
        # re-seed numpy random state
        np.random.seed(int.from_bytes(secrets.token_bytes(4)))
        return super().execute_job(job, queue)


def cli():
    if len(sys.argv) == 1:
        print(
            """Usage: hmq COMMAND [ARGS]
        
Commands:
request_access URL                      Request access to a hummingqueue instance.
grant_access SIGNING_REQUEST USERNAME   Grant access to a user."""
        )
        sys.exit(1)

    if sys.argv[1] == "request_access":
        if len(sys.argv) != 3:
            print("Usage: hmq request_access URL")
            sys.exit(1)
        print(request_access(sys.argv[2]))
        sys.exit(0)

    if sys.argv[1] == "grant_access":
        if len(sys.argv) != 4:
            print("Usage: hmq grant_access SIGNING_REQUEST USERNAME")
            sys.exit(1)
        admin_key = input("Admin key: ")
        grant_access(sys.argv[2], admin_key, sys.argv[3])
        sys.exit(0)

    print("Unknown command.")
    sys.exit(1)
