import hmq
import subprocess
import shlex
import glob
import time
import toml
import sys
import redis
import os
from string import Template
import rq
from rq import Queue
from rq.results import Result
import re
import random


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

    @property
    def queued_jobs(self):
        # test how many jobs are already queued
        cmd = f"squeue -u {os.getenv('USER', '')} -o %T -h"
        output = subprocess.check_output(shlex.split(cmd))
        njobs = len(
            [_ for _ in output.splitlines() if "COMPLETING" not in _.decode("ascii")]
        )
        return njobs

    @property
    def idle_compute_units(self):
        if time.time() - self._idle_update_time > 60:
            cmd = (
                f'sinfo -o "%n %e %a %C" -p {self._config["datacenter"]["partitions"]}'
            )
            lines = subprocess.check_output(shlex.split(cmd)).splitlines()
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
            self._idle_compute_units = sum(nodes.values())

        return self._idle_compute_units


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
        return sum([q.count for q in all_queues])

    @property
    def awaiting_queues(self):
        all_queues = Queue.all(connection=self._r)
        awaiting = []
        for q in all_queues:
            if q.count > 0 and q.count > q.started_job_registry.count:
                awaiting.append(q.name)
        return awaiting

    def add_map_entry(self, hmqid, rqid):
        self._r.hset("hmq:hmq2rq", hmqid, rqid)

    def remove_map_entry(self, hmqid):
        self._r.hdel("hmq:hmq2rq", hmqid)

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

    def clear_idle_and_empty(self):
        all_queues = Queue.all(connection=self._r)
        for queue in all_queues:
            if queue.count > 0:
                return False
            if queue.started_job_registry.count > 0:
                return False
            if queue.failed_job_registry.count > 0:
                return False

            if queue.finished_job_registry.count == 0:
                queue.delete(delete_jobs=True)

        self._r.delete("hmq:functions")

    @property
    def hmqids(self):
        return self._r.hkeys("hmq:hmq2rq")

    def cancel_and_delete(self, hmqid: str):
        rqid = self._r.hget("hmq:hmq2rq", hmqid)
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
                job.cancel()
                job.delete()
            except:
                pass
        self._r.hdel("hmq:hmq2rq", hmqid)


def main():
    # test for conda env name
    if os.getenv("CONDA_DEFAULT_ENV") != "hmq_prod":
        print("Error: conda environment not activated or not called hmq_prod")
        sys.exit(1)

    # load config
    configfile = sys.argv[1] + "/config.toml"
    with open(configfile) as f:
        config = toml.load(f)

    # if sentry is configured, set it up
    if "sentry" in config and "director" in config["sentry"]:
        import sentry_sdk

        sentry_sdk.init(
            dsn=config["sentry"]["director"],
        )

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
        if not first:
            print("Waiting...")
            time.sleep(10)
        first = False

        # maintenance: clear function cache
        localredis.clear_idle_and_empty()

        # maintenance: remove deleted jobs
        table = hmq.api.get_tasks_status(localredis.hmqids)
        for hmqid, status in table.items():
            if status == "deleted":
                localredis.cancel_and_delete(hmqid)

        # upload results
        for rqid in localredis.to_upload:
            job = rq.job.Job.fetch(rqid, connection=localredis._r)
            payload = job.result
            try:
                hmq.api.store_results([payload])
            except:
                continue
            localredis.remove_map_entry(payload["task"])
            Result.delete_all(job)
            job.delete()

        # add more work to the queues
        nslots = 3000 - localredis.total_jobs
        if nslots <= 0:
            continue

        qtasks = hmq.api.dequeue_tasks(
            config["datacenter"]["name"],
            dependencies.packagelists,
            nslots,
            slurm.idle_compute_units,
        )
        pyvers = []
        for queuename, tasks in qtasks.items():
            queue = rq.Queue(queuename, connection=localredis._r)
            version, numcores = parse_queue_name(queuename)
            if version is None:
                continue
            pyvers.append(version)

            for task in tasks:
                task["result_ttl"] = -1
                rq_job = queue.enqueue("hmq.unwrap", **task)
                localredis.add_map_entry(task["hmqid"], rq_job.id)
                localredis.cache_function(task["function"])

        dependencies.meet_all(list(set(pyvers)))

        # submit one job for random populated queues with work to do
        awaiting_queues = localredis.awaiting_queues
        if (
            len(awaiting_queues) > 0
            and slurm.queued_jobs <= config["datacenter"]["maxjobs"]
        ):
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
                "partitions": config["datacenter"]["partitions"],
            }
            slurm.submit_job(variables)


if __name__ == "__main__":
    main()
