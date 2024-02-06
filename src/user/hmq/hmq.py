import dns.resolver
import dns.message
import uuid
import sys
import traceback
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
            for schema in "https http".split():
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
            "challenge": self._get_challenge(),
        }
        uuids = self._post("/tasks/submit", payload)
        return uuids

    def find_tasks(self, tag: str):
        payload = {"tag": tag, "challenge": self._get_challenge()}
        tasks = self._post("/tasks/find", payload)
        return tasks

    def delete_tasks(self, tasks: list[str]):
        payload = {
            "delete": tasks,
            "challenge": self._get_challenge(),
        }
        print(payload)
        deleted = self._post("/tasks/delete", payload)
        return deleted

    def warm_cache(self, function: str):
        self._build_box()
        if function in functions:
            return

        payload = self._get(f"/function/fetch/{function}").json()

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

    def has_jobs(self, datacenter, packagelists):
        # build list of packages
        packages = {}
        for pyver, packagelist in packagelists.items():
            with open(packagelist, "r") as fh:
                ps = fh.read().splitlines()

            packages[pyver] = list(map(self._hash, ps))

        payload = {
            "datacenter": datacenter,
            "challenge": self._get_challenge(),
            "packages": packages,
        }
        return self._post("/queue/haswork", payload)

    @staticmethod
    def _worker(hmqid, call, function, secret, baseurl, verify):
        def get_challenge(box, baseurl, verify):
            challenge = requests.get(f"{baseurl}/auth/challenge", verify=verify)
            payload = str(challenge["challenge"])
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
        payload = {
            "results": [payload],
            "challenge": get_challenge(box, baseurl, verify),
        }

        res = requests.post(
            f"{baseurl}/results/store",
            json=payload,
            verify=verify,
        )

    def _get_challenge(self) -> str:
        """Uses the server provided challenge for up to ten seconds.

        Returns
        -------
        str
            Challenge
        """
        if time.time() - self._challenge_time > 10:
            response = str(self._get("/auth/challenge").json()["challenge"])
            self._challenge = self._encrypt(response, raw=True)
            self._challenge_time = time.time()
        return self._challenge


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
    def from_queue(tag: str):
        """Loads a tag from the queue by downloading all data (again). Use sparingly.

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
        t.tasks = api.find_tasks(tag)
        t._results = {}
        t._errors = {}
        return t

    @staticmethod
    def from_file(filename: str):
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

        t = Tag("")
        t.name = meta["name"]
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
            time.sleep(5)
        return remaining

    def delete(self):
        remaining_tasks = (
            set(self.tasks) - set(self._results.keys()) - set(self._errors.keys())
        )
        api.delete_tasks(list(remaining_tasks))

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

    def submit(tag=None, ncores=1, datacenters: str = None, packages=[], quiet=False):
        if not api.ping():
            print("Server is not reachable.")
            return

        # register function with server
        callable = cloudpickle.dumps(func)

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
        tag = Tag(func.__name__)
        if datacenters is None:
            datacenters = []
        else:
            datacenters = [dc.strip() for dc in datacenters.split(",")]
        tag._add_uuids(
            api.submit_tasks(tag.name, digest, func._calls, ncores, datacenters)
        )
        func._calls = []
        print(f"Completed tag: {tag.name}")
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


def use_tunnel(at: str, baseurl: str, sentry_dsn: str = None):
    redirects = {}
    if sentry_dsn is not None:
        url = urlparse(sentry_dsn)
        sentry_host = url.hostname
        sentry_ip = socket.gethostbyname(sentry_host)
        redirects[sentry_host] = sentry_ip

    tunnels = {f"hmq.{baseurl}": at, f"redis.{baseurl}": at}
    if sentry_dsn is not None:
        tunnels[sentry_host] = at
    over = OverrideResolver(tunnels)
    over.reset()
    dns.resolver.override_system_resolver(over)
    return redirects


def generate_traefik_config(from_ip, from_port, to_ip, to_port, redirects={}):
    # build per-host redirects
    counter = 0
    routers = ""
    services = ""
    for host, ip in redirects.items():
        routers += f"""
    routerhost{counter}:
      entryPoints:
        - websecure
      service: forward{counter}
      rule: "HostSNI(`{host}`)"
      tls:
        passthrough: true
"""
        services += f"""
    forward{counter}:
      loadBalancer:
        servers:
          - address: {ip}:443
"""
        counter += 1

    return f"""providers:
  file:
    filename: traefik.yml

entryPoints:
  websecure:
    address: "{from_ip}:{from_port}"

tcp:
  routers:
{routers}
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
          - address: "{to_ip}:{to_port}" 
{services}
"""


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
