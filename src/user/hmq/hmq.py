# %%
import dns.resolver
import dns.message
import uuid
import sys
import traceback
import subprocess
import time
import requests
import base64
import socket
import urllib3
import cloudpickle
import json
import hashlib
import configparser
from pathlib import Path
import nacl
from nacl.secret import SecretBox
from nacl.public import PrivateKey, SealedBox, PublicKey
from nacl.signing import SigningKey, VerifyKey
import tqdm
from rq import Worker

functions = {}


class API:
    def __init__(self):
        self._box = None
        self._url = None

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
        encrypted_message = base64.b64encode(
            box.encrypt(self._messagekey.encode("utf8"))
        ).decode("ascii")

        payload = {
            "sign": verify_key,
            "encrypt": public_key,
            "signature": b64_signature,
            "username": username,
            "compute": encrypted_secret,
            "message": encrypted_message,
        }
        try:
            response = self._post("/user/add", payload)
        except:
            raise ValueError("Cannot submit signature. Is the admin key correct?")

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
                for item in "compute message".split():
                    self._computesecret = userbox.decrypt(
                        base64.b64decode(response[item])
                    )
                    config["server"][item] = base64.b64encode(
                        self._computesecret
                    ).decode("ascii")
                config["server"]["admin"] = response["admin"]
                config["server"]["message"] = base64.b64decode(
                    config["server"]["message"]
                ).decode("ascii")

                # save result
                with open(
                    Path("~/.hummingqueue").expanduser() / "config.ini", "w"
                ) as f:
                    config.write(f)

                # finally load compute secret
                self._computesecret = base64.b64decode(config["server"]["compute"])
            self._messagekey = config["server"]["message"]
            try:
                self._adminkey = base64.b64decode(config["server"]["admin"])
            except:
                pass
            self._box = SecretBox(self._computesecret)

    def _get_message_secret(self):
        self._build_box()
        return self._messagekey

    def _encrypt(self, obj, raw=False):
        self._build_box()

        if raw:
            message = obj.encode("utf8")
        else:
            message = json.dumps(obj).encode("utf8")
        message = base64.b64encode(self._box.encrypt(message)).decode("ascii")
        return message

    def _decrypt(self, obj):
        self._build_box()

        message = obj.encode("utf-8")
        message = json.loads(
            self._box.decrypt(base64.b64decode(message.decode("ascii")))
        )
        return message

    def _post(self, endpoint, payload):
        if endpoint != "/user/secrets":
            self._build_box()

        response = requests.post(
            self._url + endpoint, json=payload, verify=self._verify
        )
        if response.status_code in (404, 422):
            raise ValueError("Unable to post request.")
        return response.json()

    def _get(self, endpoint, baseurl=None):
        if baseurl is None:
            baseurl = self._url
        response = requests.get(baseurl + endpoint, verify=self._verify)
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
            "challenge": self._encrypt(str(time.time()), raw=True),
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
            "challenge": self._encrypt(str(time.time()), raw=True),
        }
        results = {}
        for task, result in self._post("/results/retrieve", payload).items():
            if result is None:
                results[task] = result
                continue
            for key in "result error".split():
                if result[key] is not None:
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
        callstr = json.dumps(calls)
        calldigest = hashlib.sha256(callstr.encode("utf8")).hexdigest()
        payload = {
            "tag": tag,
            "function": digest,
            "calls": callstr,
            "digest": calldigest,
            "ncores": ncores,
            "datacenters": datacenters,
            "challenge": self._encrypt(str(time.time()), raw=True),
        }
        uuids = self._post("/tasks/submit", payload)
        return uuids

    def warm_cache(self, function: str):
        self._build_box()
        if function in functions:
            return

        payload = self._get(f"/function/fetch/{function}").json()

        # decrypt payload
        remote_function = self._box.decrypt(base64.b64decode(payload["function"]))
        packages = [self._box.decrypt(base64.b64decode(_)) for _ in payload["packages"]]
        remote_function = json.loads(remote_function)
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

        # load function
        for package in packages:
            # system call to install via pip
            subprocess.run(
                [sys.executable, "-m", "pip", "install", package],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        functions[function] = cloudpickle.loads(callable)

    def has_jobs(self, datacenter):
        payload = {
            "datacenter": datacenter,
            "challenge": self._encrypt(str(time.time()), raw=True),
        }
        return self._post("/queue/haswork", payload)

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
            result = base64.b64encode(
                box.encrypt(json.dumps(result).encode("utf8"))
            ).decode("ascii")
            errormsg = None
        except:
            errormsg = traceback.format_exc()
            errormsg = base64.b64encode(
                box.encrypt(json.dumps(errormsg).encode("utf8"))
            ).decode("ascii")
            result = None
        endtime = time.time()

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
                "PENDING": len(self.tasks) - len(self._results) - len(self._errors),
                "DONE": len(self._results),
                "FAILED": len(self._errors),
            },
        }
        payload = ""
        for task in self.tasks:
            result = None
            if task in self._results:
                result = self._results[task]
            error = None
            if task in self._errors:
                error = self._errors[task]
            payload += (
                json.dumps({"task": task, "result": result, "error": error}) + "\n"
            )

        with open(filename, "w") as f:
            f.write(json.dumps(meta) + "\n" + payload)

    @staticmethod
    def from_file(filename):
        meta = None
        tasks = []
        results = {}
        errors = {}
        for line in open(filename):
            if meta is None:
                meta = json.loads(line.strip())
            else:
                row = json.loads(line.strip())
                tasks.append(row["task"])
                if row["result"] is not None:
                    results[row["task"]] = row["result"]
                if row["error"] is not None:
                    errors[row["task"]] = row["error"]

        t = Tag(meta["name"])
        t.tasks = tasks
        t._results = results
        t._errors = errors
        return t

    def _pull_batch(self, tasklist):
        if len(tasklist) > 0:
            results = api.retrieve_results(tasklist)
            for task, result in results.items():
                if result is not None:
                    if result["result"] is not None:
                        self._results[task] = result["result"]
                    else:
                        self._errors[task] = result["error"]

    def pull(self, blocking=False) -> int:
        remaining = len(self.tasks) - len(self._results) - len(self._errors)
        while remaining > 0:
            tasklist = []
            for task in tqdm.tqdm(self.tasks):
                if task not in self._results and task not in self._errors:
                    tasklist.append(task)
                if len(tasklist) > 50:
                    self._pull_batch(tasklist)
                    tasklist = []
            self._pull_batch(tasklist)
            remaining = len(self.tasks) - len(self._results) - len(self._errors)
            if not blocking:
                break
        return remaining

    @property
    def results(self):
        res = []
        for task in self.tasks:
            if task in self._results:
                res.append(self._results[task])
            else:
                res.append(None)
        return res

    @property
    def errors(self):
        res = []
        for task in self.tasks:
            if task in self._errors:
                res.append(self._errors[task])
            else:
                res.append(None)
        return res


def request_access(url: str):
    return api.setup(url)


def grant_access(signing_request: str, adminkey: str, username: str):
    api.grant_access(signing_request, adminkey, username)


def task(func):
    def wrapper(*args, **kwargs):
        func._calls.append((args, kwargs))

    def submit(tag=None, ncores=1, datacenters: str = None, packages=[]):
        # register function with server
        callable = cloudpickle.dumps(func)

        remote_function = {
            "callable": callable,
            "packages": packages,
        }

        digest = hashlib.sha256(callable).hexdigest()
        if not api.register_function(remote_function, digest):
            raise ValueError("Could not register function with server.")

        # send calls to server
        tag = Tag(func.__name__)
        if datacenters is None:
            datacenters = []
        else:
            datacenters = [dc.strip() for dc in datacenters.split(",")]
        tag._add_uuids(
            api.submit_tasks(tag.name, digest, func._calls, ncores, datacenters)
        )
        func._calls = []
        return tag

    wrapper.submit = submit
    func._calls = []

    return wrapper


def unwrap(hmqid: str, call: str, function: str):
    api._build_box()
    api._worker(hmqid, call, function, api._box._key, api._url, api._verify)


class CachedWorker(Worker):
    def execute_job(self, job, queue):
        api.warm_cache(job.kwargs["function"])
        ret = super().execute_job(job, queue)
        self.connection.hdel("id2id", job.kwargs["hmqid"])
        return ret


class OverrideResolver(dns.resolver.Resolver):
    def __init__(self, overrides):
        self._overrides = overrides

    def _build_message(self, qname):
        return dns.message.from_text(
            f"""id 12345
opcode QUERY
rcode NOERROR
flags QR RD RA
;QUESTION
{qname}. IN A
;ANSWER
{qname}. 37478 IN A {self._overrides[qname]}"""
        )

    def resolve(self, *args, **kwargs):
        if "qname" in kwargs:
            qname = kwargs["qname"]
        else:
            qname = args[0]
        if qname in self._overrides:
            return dns.resolver.Answer(
                qname,
                dns.rdatatype.RdataType.A,
                dns.rdataclass.RdataClass.IN,
                self._build_message(qname),
            )
        else:
            return dns.resolver.resolve(qname)


def use_tunnel(at: str, baseurl: str):
    over = OverrideResolver({f"hmq.{baseurl}": at, f"redis.{baseurl}": at})
    over.reset()
    dns.resolver.override_system_resolver(over)


def generate_traefik_config(from_ip, from_port, to_ip, to_port):
    return f"""providers:
  file:
    filename: traefik.yml

entryPoints:
  websecure:
    address: "{from_ip}:{from_port}"

tcp:
  routers:
    router4websecure:
      entryPoints:
        - websecure
      service: websecure-forward
      rule: "HostSNI(`*`)"
      tls:
         passthrough: true

  services:
    websecure-forward:
      loadBalancer:
        servers:
          - address: "{to_ip}:{to_port}" """
