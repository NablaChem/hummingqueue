import hmq
import subprocess
import shlex
import socket
import glob
import time
import toml
import sys
import os
from string import Template

# sentry
import sentry_sdk

sentry_sdk.init(
    dsn="https://94f2cd3ea02b7eac1a06d8e0f387645d@o4504798970904576.ingest.sentry.io/4506149927190528"
)


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


def build_packagelists(config):
    paths = glob.glob(f"{config['datacenter']['envs']}/envs/hmq_*/pkg.list")
    ps = {}
    for path in paths:
        pyver = path.split("/")[-2].split("_")[-1]
        if pyver == "base":
            continue
        ps[pyver] = path
    return ps


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
    for binary in ["traefik", "micromamba"]:
        if not os.path.exists(config["datacenter"]["binaries"] + "/" + binary):
            print(f"Error: {binary} not found in {config['datacenter']['binaries']}")
            sys.exit(1)

    # setup tunnel
    server_address = socket.gethostbyname(f'hmq.{config["server"]["baseurl"]}')
    try:
        sentrydsn = config["sentry"]["computenode"]
    except:
        sentrydsn = None
    redirects = hmq.use_tunnel(
        config["gateway"]["address"], config["server"]["baseurl"], sentrydsn
    )

    # generate traefik config
    filename = config["datacenter"]["tmpdir"] + "/traefik.yml"
    content = hmq.generate_traefik_config(
        config["gateway"]["address"],
        str(config["gateway"]["port"]),
        server_address,
        "443",
        redirects,
    )
    with open(filename, "w") as f:
        f.write(content)

    # run traefik
    cmd = f"{config['datacenter']['binaries']}/traefik --config traefik.yml"
    p = subprocess.Popen(shlex.split(cmd), cwd=config["datacenter"]["tmpdir"])

    # request jobs / run heartbeat
    with open(f"{sys.argv[1]}/hmq.job") as fh:
        templatestr = fh.read()

    while True:
        time.sleep(10)

        aggregates = dict()
        pyvers = []

        # get jobs
        packagelists = build_packagelists(config)
        qs = hmq.api.has_jobs(config["datacenter"]["name"], packagelists)
        for q in qs:
            variables = {
                "pyver": q["version"],
                "ncores": q["numcores"],
                "queues": q["name"],
                "envs": config["datacenter"]["envs"],
                "baseurl": config["server"]["baseurl"],
                "gateway": config["gateway"]["address"],
                "gatewayport": config["gateway"]["port"],
                "binaries": config["datacenter"]["binaries"],
            }
            pyvers.append(q["version"])
            key = f"{q['version']}-{q['numcores']}"
            if key not in aggregates:
                aggregates[key] = variables
            else:
                aggregates[key]["queues"] += " " + q["name"]

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

        # test how many jobs are already queued
        cmd = f"squeue -u {os.getenv('USER', '')} "
        output = subprocess.check_output(shlex.split(cmd))
        njobs = len(output.splitlines()) - 1
        if njobs >= config["datacenter"]["maxjobs"]:
            continue

        # submit jobs
        for variables in aggregates.values():
            with open(f"{config['datacenter']['tmpdir']}/hmq.job", "w") as fh:
                content = Template(templatestr).substitute(variables)
                fh.write(content)

            subprocess.run(
                shlex.split("sbatch hmq.job"), cwd=config["datacenter"]["tmpdir"]
            )
            njobs += 1
            if njobs >= config["datacenter"]["maxjobs"]:
                break


if __name__ == "__main__":
    main()
