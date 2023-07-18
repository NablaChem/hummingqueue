import hmq
import subprocess
import shlex
import socket
import time
import toml
import sys
import os
from string import Template

def install(config, pyver):
    installfile=f"{config["datacenter"]["tmpdir"]}/install.sh"
    with open(installfile) as fh:
        fh.write(f'''#!/bin/bash
export MAMBA_ROOT_PREFIX={config["datacenter"]["envs"]}
eval "$({config["datacenter"]["binaries"]}/micromamba shell hook --shell bash )"
micromamba create -n hmq_{pyver}
micromamba activate hmq_{pyver}
micromamba install python={pyver} -c conda-forge -y
pip install hmq
''')
    subprocess.run(shlex.split(f"chmod +x {installfile}"))
    subprocess.run(shlex.split(installfile))
    os.remove(installfile)

def main():
    # test for conda env name
    if os.getenv("CONDA_DEFAULT_ENV") != "hmq_base":
        print("Error: conda environment not activated or not called hmq_base")
        sys.exit(1)

    # load config
    configfile = f"{sys.argv[1]}/config.toml"
    with open(configfile) as f:
        config = toml.load(f)

    # verify binaries
    for binary in ["traefik", "micromamba"]:
        if not os.path.exists(config["datacenter"]["binaries"] + "/" + binary):
            print(f"Error: {binary} not found in {config['datacenter']['binaries']}")
            sys.exit(1)

    # generate traefik config
    filename = config["datacenter"]["tmpdir"] + "/traefik.yml"
    server_address = socket.gethostbyname(f'hmq.{config["server"]["baseurl"]}')
    content = hmq.generate_traefik_config(
        config["gateway"]["address"],
        str(config["gateway"]["port"]),
        server_address,
        "443",
    )
    with open(filename, "w") as f:
        f.write(content)

    # run traefik
    cmd = f"{config['datacenter']['binaries']}/traefik --config traefik.yml"
    p = subprocess.Popen(shlex.split(cmd), cwd=config["datacenter"]["tmpdir"])

    # setup tunnel
    hmq.use_tunnel(config["gateway"]["address"], config["server"]["baseurl"])

    # request jobs / run heartbeat
    with open(f"{sys.argv[1]}/hmq.job") as fh:
        templatestr = fh.read()

    while True:
        time.sleep(10)

        aggregates = dict()
        pyvers = []

        # get jobs
        qs = hmq.api.has_jobs(config["datacenter"]["name"])
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

            install(pyver)

        # test how many jobs are already queued
        output = subprocess.check_output(shlex.split("squeue -u $USER"), shell=True)
        njobs = len(output.splitlines()) - 1
        if njobs >= config["datacenter"]["maxjobs"]:
            continue

        # submit jobs
        for variables in aggregates.values():
            with open(f"{config['datacenter']['tmpdir']}/hmq.job", "w") as fh:
                content = Template(templatestr).substitute(variables)
                fh.write(content)

            subprocess.run(shlex.split("sbatch hmq.job"), cwd=config["datacenter"]["tmpdir"])
            njobs += 1
            if njobs >= config["datacenter"]["maxjobs"]:
                break


if __name__ == "__main__":
    main()
