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
        cmd = f"squeue -u {os.getenv('USER', '')} "
        output = subprocess.check_output(shlex.split(cmd))
        njobs = len(output.splitlines()) - 1
        return njobs

    @property
    def idle_compute_units(self):
        if time.time() - self._idle_update_time > 60:
            cmd = 'sinfo -o "%n %e %a %C %P" -p {config["datacenter"]["partitions"]}'
            lines = subprocess.check_output(shlex.split(cmd)).splitlines()
            nodes = {}
            for nodeinfo in lines[1:]:
                name, mem, avl, cores, partition = nodeinfo.split()
                memunits = int(mem) / 4000
                cores = int(cores.split("/")[1])
                nodes[name] = min(memunits, cores)
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
        total = 0
        for queue in self._r.smembers("rq:queues"):
            total += self._r.llen(queue)
        return total

    @property
    def active_queues(self):
        return [_.replace("rq:queue:", "") for _ in self._r.smembers("rq:queues")]


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

    while True:
        time.sleep(10)

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
        for queuename, tasks in qtasks:
            queue = rq.Queue(queuename, connection=localredis._r)
            version, numcores = parse_queue_name(queuename)
            pyvers.append(version)
            if version is None:
                continue

            for task in tasks:
                rq_job = queue.enqueue("hmq.unwrap", **task)
                localredis._r.hset("hmq2rq", task["hmqid"], rq_job.id)

        dependencies.meet_all(pyvers)

        njobs = slurm.queued_jobs

        # submit jobs for random populated queues
        active_queues = localredis.active_queues
        while njobs < config["datacenter"]["maxjobs"]:
            if len(active_queues) == 0:
                break

            queuename = random.choice(active_queues)
            version, numcores = parse_queue_name(queuename)
            if version is None:
                continue

            variables = {
                "pyver": version,
                "ncores": numcores,
                "queues": queuename,
                "envs": config["datacenter"]["envs"],
                "baseurl": config["server"]["baseurl"],
                "redis_port": config["server"]["redis_port"],
                "redis_host": config["server"]["redis_host"],
                "binaries": config["datacenter"]["binaries"],
                "partitions": config["datacenter"]["partitions"],
            }
            slurm.submit_job(variables)
            njobs += 1


if __name__ == "__main__":
    main()
