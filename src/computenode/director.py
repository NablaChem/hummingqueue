import hmq
import subprocess
import shlex
import glob
import time
import rq.exceptions
import toml
import sys
import redis
import os
from string import Template
import rq
from rq import Queue, Worker
from rq.worker import WorkerStatus
from rq.results import Result
import re
import random
import datetime as dt

import time
from functools import wraps
import time
from functools import wraps


def ttl_cache(seconds):
    def decorator(fn):
        cache = {}

        @wraps(fn)
        def wrapper(self, *args):
            now = time.time()
            if args in cache:
                result, timestamp = cache[args]
                if now - timestamp < seconds:
                    return result
            result = fn(self, *args)
            cache[args] = (result, now)
            return result

        return wrapper

    return decorator


def ttl_property(seconds):
    """
    A decorator that creates a cached property with time-to-live (TTL) functionality.

    This decorator transforms a method into a property that caches its return value
    for a specified number of seconds. After the TTL expires, the next access will
    re-execute the original method and cache the new result.

    Args:
        seconds (int): The cache TTL in seconds. After this time, the cached value expires.

    Returns:
        function: A decorator that can be applied to instance methods.

    Usage:
        @ttl_property(seconds=60)
        def expensive_operation(self):
            # This will only run once per minute
            return costly_computation()

    Implementation details:
        - Uses instance attributes to store cached values and timestamps
        - Cache key format: "_cache_{method_name}"
        - Time key format: "_cache_time_{method_name}"
        - Thread-safe for single-threaded applications
    """

    def decorator(fn):
        cache_key = f"_cache_{fn.__name__}"
        time_key = f"_cache_time_{fn.__name__}"

        @property
        @wraps(fn)
        def wrapper(self):
            now = time.time()
            last_time = getattr(self, time_key, 0)
            if now - last_time < seconds and hasattr(self, cache_key):
                return getattr(self, cache_key)

            value = fn(self)
            setattr(self, cache_key, value)
            setattr(self, time_key, now)
            return value

        return wrapper

    return decorator


try:
    import sentry_sdk

    transaction_context = sentry_sdk.start_transaction
    span_context = sentry_sdk.start_span
    from sentry_sdk.crons import monitor
except ImportError:
    import contextlib

    transaction_context = contextlib.nullcontext
    span_context = contextlib.nullcontext
    monitor = None


def parse_queue_name(queue: str) -> tuple[str, str]:
    regex = r"py-(?P<pythonversion>.+)-nc-(?P<numcores>.+)-dc-(?P<datacenter>.+)"
    m = re.match(regex, queue)
    if m is None:
        return None, None
    version = m.group("pythonversion")
    numcores = m.group("numcores")
    return version, numcores


class DependencyManager:
    def __init__(self, config):
        self._config = config

    @property
    def packagelists(self) -> dict[str, str]:
        paths = glob.glob(f"{self._config['datacenter']['envs']}/envs/hmq_*/pkg.list")
        ps = {}
        for path in paths:
            pyver = path.split("/")[-2].split("_")[-1]
            if pyver == "prod":
                continue
            ps[pyver] = path
        return ps

    def _install_env(self, pyver):
        installfile = f"{self._config['datacenter']['tmpdir']}/install.sh"
        with open(installfile, "w") as fh:
            fh.write(
                f"""#!/bin/bash
    export MAMBA_ROOT_PREFIX={self._config["datacenter"]["envs"]}
    eval "$({self._config["datacenter"]["binaries"]}/micromamba shell hook --shell bash )"
    micromamba create -n hmq_{pyver}
    micromamba activate hmq_{pyver}
    micromamba install python={pyver} -c conda-forge -y
    pip install hmq
    """
            )
        subprocess.run(shlex.split(f"chmod +x {installfile}"))
        subprocess.run(shlex.split(installfile))
        os.remove(installfile)

    def _install_packages(self, pyver, missing):
        installlist = f"{self._config['datacenter']['envs']}/envs/hmq_{pyver}/pkg.list"
        installfile = f"{self._config['datacenter']['tmpdir']}/install.sh"
        with open(installfile, "w") as fh:
            fh.write(
                f"""#!/bin/bash
    export MAMBA_ROOT_PREFIX={self._config["datacenter"]["envs"]}
    eval "$({self._config["datacenter"]["binaries"]}/micromamba shell hook --shell bash )"
    micromamba activate hmq_{pyver}
    for package in {" ".join(missing)};
    do
        pip install $package;
        echo $package >> {installlist};
    done
    """
            )
        subprocess.run(shlex.split(f"chmod +x {installfile}"))
        subprocess.run(shlex.split(installfile))
        os.remove(installfile)

    def meet_all(self, pyvers):
        # install python versions
        for pyver in set(pyvers):
            if not os.path.exists(
                f"{self._config['datacenter']['envs']}/envs/hmq_{pyver}"
            ):
                self._install_env(pyver)

        # install requirements
        for pyver in set(pyvers):
            installlist = (
                f"{self._config['datacenter']['envs']}/envs/hmq_{pyver}/pkg.list"
            )
            missing = hmq.api.missing_dependencies(
                self._config["datacenter"]["name"], installlist, pyver
            )
            if len(missing) > 0:
                self._install_packages(pyver, missing)


class SlurmManager:
    def __init__(self, config):
        self._config = config
        self._username = os.getenv("USER")
        if not self._username:
            print("Error: USER environment variable not set")
            sys.exit(1)
        self._idle_update_time = 0

    def read_template(self, filename: str):
        with open(filename) as fh:
            self._templatestr = fh.read()

    def submit_job(self, variables: dict):
        tmpdir = self._config["datacenter"]["tmpdir"]
        with open(f"{tmpdir}/hmq.job", "w") as fh:
            content = Template(self._templatestr).substitute(variables)
            fh.write(content)

        subprocess.run(shlex.split("sbatch hmq.job"), cwd=tmpdir)

    @ttl_property(seconds=60)
    def allocated_units(self):
        cmd = f"squeue -u {self._username} -t R -o '%C' -h"
        try:
            lines = subprocess.check_output(shlex.split(cmd)).splitlines()
        except subprocess.CalledProcessError:
            return 0
        cores = 0
        for line in lines:
            cores += int(line.decode("ascii"))
        return cores

    @ttl_property(seconds=60)
    def idle_compute_units(self):
        cmd = f'sinfo -o "%n %m %a %C" -p {self._config["datacenter"]["partitions"]}'
        try:
            lines = subprocess.check_output(shlex.split(cmd)).splitlines()
        except subprocess.CalledProcessError:
            # SLURM failure, e.g. from downtime/network issues.
            return 0
        nodes = {}
        for nodeinfo in lines[1:]:
            try:
                name, mem, avl, cores = nodeinfo.decode("ascii").split()
                if avl != "up":
                    continue
                memunits = int(int(mem) / 4000)
                cores = int(cores.split("/")[1])
                nodes[name] = min(memunits, cores)
            except:
                continue
        return sum(nodes.values())

    @ttl_property(seconds=60)
    def best_partition(self):
        partitions = self._config["datacenter"]["partitions"].split(",")
        pending_counts = {}
        for partition in partitions:
            pending_counts[partition] = 0
        njobs = 0
        cmd = f"squeue -u {self._username} -o '%T %P' -h"
        output = subprocess.check_output(shlex.split(cmd))
        for line in output.splitlines():
            try:
                job_state, job_partition = line.decode("ascii").strip().split()
            except:
                continue
            if job_state == "COMPLETING":
                continue
            njobs += 1
            if job_state == "PENDING":
                pending_counts[job_partition] += 1
        if njobs >= self._config["datacenter"]["maxjobs"]:
            return

        for partition in partitions:
            if pending_counts[partition] > 5:
                continue
            return partition


class RedisManager:
    def __init__(self, config):
        # check redis is reachable
        self._r = redis.StrictRedis(
            host=config["datacenter"]["redis_host"],
            port=config["datacenter"]["redis_port"],
            password=config["datacenter"]["redis_pass"],
        )
        try:
            self._r.ping()
        except:
            print(
                "Error: cluster-local redis not reachable with the given credentials."
            )
            sys.exit(1)

    @property
    def total_jobs(self):
        all_queues = Queue.all(connection=self._r)
        result = sum([q.count for q in all_queues])
        return result

    @property
    def awaiting_queues(self):
        all_queues = Queue.all(connection=self._r)
        awaiting = []
        for q in all_queues:
            if q.count > 0:
                awaiting.append(q.name)
        return awaiting

    def add_map_entries(self, mapping):
        self._r.hset("hmq:hmq2rq", mapping=mapping)

    def remove_map_entries(self, hmqids: list):
        if hmqids:
            self._r.hdel("hmq:hmq2rq", *hmqids)

    def cache_function(self, function):
        if not self._r.hexists("hmq:functions", function):
            content = hmq.api.fetch_function(function)
            self._r.hset("hmq:functions", function, content)

    @property
    def to_upload(self):
        rqids = []
        for queue in Queue.all(connection=self._r):
            for job in queue.finished_job_registry.get_job_ids():
                rqids.append(job)
        return rqids

    def remove_from_registry(self, rqid):
        """Clears a job from any registry.

        Required if the registry holds a job that does not exist any more.
        Rare: result of a race condition."""
        for queue in Queue.all(connection=self._r):
            queue.finished_job_registry.remove(rqid)

    def clear_idle_and_empty(self):
        all_queues = Queue.all(connection=self._r)

        for queue in all_queues:
            # all cases when to keep the queue
            if queue.count > 0:
                continue
            if queue.started_job_registry.count > 0:
                continue
            if queue.failed_job_registry.count > 0:
                continue

            # intermediate jobs stuck?
            if self._r.llen(queue.intermediate_queue_key) > 0:
                worker = hmq.CachedWorker(queue.name, connection=self._r)
                worker.clean_registries()
                continue

            # cleanup: drop full queue
            if queue.finished_job_registry.count == 0:
                queue.delete(delete_jobs=True)

        # no queue left: delete function cache
        all_queues = Queue.all(connection=self._r)
        if len(all_queues) == 0:
            self._r.delete("hmq:functions")

    @ttl_cache(seconds=600)
    def retry_failed_jobs(self):
        # find all started jobs that the queues do not know about, then fail them
        cursor = 0
        matches = []
        prefix = rq.job.Job.redis_job_namespace_prefix
        while True:
            cursor, keys = self._r.scan(cursor=cursor, match=f"{prefix}*", count=1000)
            matches.extend(keys)
            if cursor == 0:
                break
        stale = set([_[len(prefix) :].decode("ascii") for _ in matches])

        for queue in Queue.all(connection=self._r):
            # reduce stale list by those which are known
            stale = stale - set(queue.get_job_ids())

            for registry in (
                queue.failed_job_registry,
                queue.started_job_registry,
                queue.canceled_job_registry,
                queue.deferred_job_registry,
                queue.scheduled_job_registry,
                queue.finished_job_registry,
            ):
                stale = stale - set(registry.get_job_ids())

            # jobs which completed but failed
            for jobid in queue.failed_job_registry.get_job_ids():
                try:
                    job = rq.job.Job.fetch(jobid, connection=self._r)
                    job.requeue()
                except rq.exceptions.NoSuchJobError:
                    queue.failed_job_registry.remove(jobid)

        # remove stales from hmq known list
        values = [_.decode("ascii") for _ in self._r.hgetall("hmq:hmq2rq").values()]
        self.remove_map_entries(list(set(values) & set(stale)))

        for jobid in stale:
            self._r.delete(f"{prefix}{jobid}")

    @property
    def hmqids(self) -> list[str]:
        result = [_.decode("ascii") for _ in self._r.hkeys("hmq:hmq2rq")]
        return result

    @ttl_cache(seconds=60)
    def clean_hmq2rq(self):
        """Remove entries from the mapping where the rq job no longer exists."""
        to_be_deleted = []
        for hmqid, rqid in self._r.hgetall("hmq:hmq2rq").items():
            if not rq.job.Job.exists(rqid.decode("ascii"), connection=self._r):
                to_be_deleted.append(hmqid)
        if to_be_deleted:
            self._r.hdel("hmq:hmq2rq", *to_be_deleted)

    def cancel_and_delete(self, hmqid: str):
        try:
            rqid = self._r.hget("hmq:hmq2rq", hmqid).decode("ascii")
        except:
            return
        job = None
        try:
            job = rq.job.Job.fetch(rqid, connection=self._r)
        except rq.exceptions.NoSuchJobError:
            pass
        if job:
            try:
                Result.delete_all(job)
            except:
                pass
            try:
                rq.command.send_stop_job_command(self._r, rqid)
            except:
                pass
            try:
                job.cancel()
                job.delete()
            except:
                pass
        self._r.hdel("hmq:hmq2rq", hmqid)

    @property
    def running_tasks(self):
        running = 0
        for queue in Queue.all(connection=self._r):
            running += queue.started_job_registry.count
        return running

    @ttl_property(seconds=60)
    def busy_units(self):
        busy = 0
        for worker in Worker.all(connection=self._r):
            if worker.get_state() == WorkerStatus.BUSY:
                _, numcores = parse_queue_name(worker.queue_names()[0])
                busy += int(numcores)
        return busy


def main():
    # test for conda env name
    if os.getenv("CONDA_DEFAULT_ENV") != "hmq_prod":
        print("Error: conda environment not activated or not called hmq_prod")
        sys.exit(1)

    # load config
    configfile = sys.argv[1] + "/config.toml"
    with open(configfile) as f:
        config = toml.load(f)

    # set up sentry
    try:
        sentry_sdk.init(
            dsn=config["sentry"]["director"],
            enable_tracing=True,
            traces_sample_rate=0.01,
        )
    except:
        pass

    # verify binaries
    for binary in ["micromamba"]:
        if not os.path.exists(config["datacenter"]["binaries"] + "/" + binary):
            print(f"Error: {binary} not found in {config['datacenter']['binaries']}")
            sys.exit(1)

    # set up domain managers
    slurm = SlurmManager(config)
    slurm.read_template(f"{sys.argv[1]}/hmq.job")
    localredis = RedisManager(config)
    dependencies = DependencyManager(config)

    first = True
    while True:
        # terminate if STOP file is present
        if os.path.exists(f"{sys.argv[1]}/STOP"):
            break

        if not first:
            print("Waiting...")
            time.sleep(10)

            # send heartbeat for monitoring
            if monitor:
                with monitor(monitor_slug=config["sentry"]["monitor"]):
                    pass

        first = False
        with transaction_context(op="director", name=config["datacenter"]["name"]):
            # maintenance: clear function cache
            with span_context(op="clear_function_cache"):
                localredis.clear_idle_and_empty()

            # maintenance: remove deleted jobs
            with span_context(op="remove_deleted_jobs"):
                hmqids = localredis.hmqids
                try:
                    hmqids = random.sample(hmqids, 200)
                except ValueError:
                    continue
                table = hmq.api.get_tasks_status(hmqids)
                for hmqid, status in table.items():
                    if status == "deleted":
                        localredis.cancel_and_delete(hmqid)

            # maintenance: restart failed jobs
            with span_context(op="restart_failed_jobs"):
                localredis.retry_failed_jobs()

            # upload results
            with span_context(op="upload_results"):
                max_batch_size = 2 * 1024 * 1024  # 2MB in bytes
                batch_payloads = []
                batch_jobs = []
                current_batch_size = 0

                for rqid in localredis.to_upload:
                    try:
                        job = rq.job.Job.fetch(rqid, connection=localredis._r)
                    except rq.exceptions.NoSuchJobError:
                        localredis.remove_from_registry(rqid)
                        continue

                    payload = job.result
                    payload_size = len(str(payload))

                    def _process_batch(batch_payloads):
                        try:
                            hmq.api.store_results(batch_payloads)

                            to_be_deleted = []
                            # Clean up jobs after successful batch upload
                            for batch_job, batch_payload in zip(
                                batch_jobs, batch_payloads
                            ):
                                to_be_deleted.append(batch_payload["task"])
                                Result.delete_all(batch_job)
                                batch_job.delete()
                            localredis.remove_map_entries(to_be_deleted)
                        except:
                            pass

                    # If adding this payload would exceed the size limit, process current batch first
                    if (
                        current_batch_size + payload_size > max_batch_size
                        and batch_payloads
                    ):
                        _process_batch(batch_payloads)
                        batch_payloads = []
                        batch_jobs = []
                        current_batch_size = 0

                    # Add current payload to batch
                    batch_payloads.append(payload)
                    batch_jobs.append(job)
                    current_batch_size += payload_size

                # Process remaining items in final batch
                if batch_payloads:
                    _process_batch(batch_payloads)

            # sync open work
            with span_context(op="sync_tasks"):
                localredis.clean_hmq2rq()
                stale = hmq.api.sync_tasks(
                    datacenter=config["datacenter"]["name"], known=localredis.hmqids
                )
                for hmqid in stale:
                    localredis.cancel_and_delete(hmqid)

            # add more work to the queues
            with span_context(op="add_more_work"):
                nslots = max(0, 3000 - localredis.total_jobs)

                qtasks = hmq.api.dequeue_tasks(
                    datacenter=config["datacenter"]["name"],
                    packagelists=dependencies.packagelists,
                    maxtasks=nslots,
                    available=slurm.idle_compute_units,
                    allocated=slurm.allocated_units,
                    running=localredis.running_tasks,
                    used=localredis.busy_units,
                )
                added_functions = []
                queues = {}
                if nslots > 0:
                    pyvers = []
                    for queuename, tasks in qtasks.items():
                        if queuename not in queues:
                            queues[queuename] = rq.Queue(
                                queuename, connection=localredis._r
                            )
                        version, numcores = parse_queue_name(queuename)
                        if version is None:
                            continue
                        pyvers.append(version)

                        mappings = {}
                        for task in tasks:
                            task["result_ttl"] = -1
                            if task["function"] not in added_functions:
                                localredis.cache_function(task["function"])
                                added_functions.append(task["function"])

                            rq_job = queues[queuename].enqueue("hmq.unwrap", **task)
                            mappings[task["hmqid"]] = rq_job.id
                        localredis.add_map_entries(mappings)

                    dependencies.meet_all(list(set(pyvers)))

            # submit one job for random populated queues with work to do
            with span_context(op="submit_jobs"):
                awaiting_queues = localredis.awaiting_queues
                if len(awaiting_queues) > 0:
                    partition = slurm.best_partition
                    if partition is None:
                        continue
                    queuename = random.choice(awaiting_queues)
                    version, numcores = parse_queue_name(queuename)
                    if version is None:
                        continue

                    variables = {
                        "pyver": version,
                        "ncores": numcores,
                        "queues": queuename,
                        "envs": config["datacenter"]["envs"],
                        "baseurl": config["server"]["baseurl"],
                        "redis_port": config["datacenter"]["redis_port"],
                        "redis_host": config["datacenter"]["redis_host"],
                        "redis_pass": config["datacenter"]["redis_pass"],
                        "binaries": config["datacenter"]["binaries"],
                        "partitions": partition,
                    }
                    try:
                        if config["containers"]["method"] == "udocker":
                            variables["udockerdir"] = config["containers"]["udockerdir"]
                            variables["containermethod"] = config["containers"][
                                "method"
                            ]
                    except:
                        pass
                    slurm.submit_job(variables)


if __name__ == "__main__":
    main()
