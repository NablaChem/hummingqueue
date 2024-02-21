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


def install(config, pyver):
    installfile = f"{config['datacenter']['tmpdir']}/install.sh"
    with open(installfile, "w") as fh:
        fh.write(
            f"""#!/bin/bash
export MAMBA_ROOT_PREFIX={config["datacenter"]["envs"]}
eval "$({config["datacenter"]["binaries"]}/micromamba shell hook --shell bash )"
micromamba create -n hmq_{pyver}
micromamba activate hmq_{pyver}
micromamba install python={pyver} -c conda-forge -y
pip install hmq
"""
        )
    subprocess.run(shlex.split(f"chmod +x {installfile}"))
    subprocess.run(shlex.split(installfile))
    os.remove(installfile)


def install_packages(config, pyver, missing):
    installlist = f"{config['datacenter']['envs']}/envs/hmq_{pyver}/pkg.list"
    installfile = f"{config['datacenter']['tmpdir']}/install.sh"
    with open(installfile, "w") as fh:
        fh.write(
            f"""#!/bin/bash
export MAMBA_ROOT_PREFIX={config["datacenter"]["envs"]}
eval "$({config["datacenter"]["binaries"]}/micromamba shell hook --shell bash )"
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


def build_packagelists(config) -> dict[str, str]:
    paths = glob.glob(f"{config['datacenter']['envs']}/envs/hmq_*/pkg.list")
    ps = {}
    for path in paths:
        pyver = path.split("/")[-2].split("_")[-1]
        if pyver == "base":
            continue
        ps[pyver] = path
    return ps


def parse_queue_name(queue: str) -> tuple[str, str]:
    regex = r"py-(?P<pythonversion>.+)-nc-(?P<numcores>.+)-dc-(?P<datacenter>.+)"
    m = re.match(regex, queue)
    if m is None:
        return None, None
    version = m.group("pythonversion")
    numcores = m.group("numcores")
    return version, numcores


class SlurmDirector:
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


def main():
    # test for conda env name
    if os.getenv("CONDA_DEFAULT_ENV") != "hmq_base":
        print("Error: conda environment not activated or not called hmq_base")
        sys.exit(1)

    # load config
    configfile = sys.argv[1] + "/config.toml"
    with open(configfile) as f:
        config = toml.load(f)

    # verify binaries
    for binary in ["micromamba"]:
        if not os.path.exists(config["datacenter"]["binaries"] + "/" + binary):
            print(f"Error: {binary} not found in {config['datacenter']['binaries']}")
            sys.exit(1)

    # check redis is reachable
    r = redis.StrictRedis(
        host=config["server"]["redis_host"],
        port=config["server"]["redis_port"],
        password=hmq.api._get_message_secret(),
    )
    try:
        r.ping()
    except:
        print("Error: cluster-local redis not reachable with the given credentials.")
        sys.exit(1)

    director = SlurmDirector(config)
    director.read_template(f"{sys.argv[1]}/hmq.job")

    while True:
        time.sleep(10)

        pyvers = []

        # get jobs
        packagelists = build_packagelists(config)

        # count empty slots
        nslots = 3000
        # TODO: improve to use rq native methods
        for queue in r.smembers("rq:queues"):
            nslots -= r.llen(queue)
        if nslots <= 0:
            continue

        qtasks = hmq.api.dequeue_tasks(
            config["datacenter"]["name"],
            packagelists,
            nslots,
            director.idle_compute_units,
        )
        for queuename, tasks in qtasks:
            queue = rq.Queue(queuename, connection=r)
            version, numcores = parse_queue_name(queuename)
            if version is None:
                continue

            for task in tasks:
                rq_job = queue.enqueue("hmq.unwrap", **task)
                r.hset("hmq2rq", task["hmqid"], rq_job.id)

        # install python versions
        for pyver in set(pyvers):
            if os.path.exists(f"{config['datacenter']['envs']}/envs/hmq_{pyver}"):
                continue
            install(config, pyver)

        # install requirements
        for pyver in set(pyvers):
            installlist = f"{config['datacenter']['envs']}/envs/hmq_{pyver}/pkg.list"
            missing = hmq.api.missing_dependencies(
                config["datacenter"]["name"], installlist, pyver
            )
            if len(missing) > 0:
                install_packages(config, pyver, missing)

        njobs = director.queued_jobs

        # submit jobs for random populated queues
        active_queues = [_.replace("rq:queue:", "") for _ in r.smembers("rq:queues")]
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
            director.submit_job(variables)
            njobs += 1


if __name__ == "__main__":
    main()
