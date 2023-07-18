import hmq
import socket
import time
import toml
import sys
import os
from string import Template


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
    # os.system(f"{config['datacenter']['binaries']}/traefik --configfile {filename} &")

    # setup tunnel
    # hmq.use_tunnel(config["gateway"]["address"], config["server"]["baseurl"])

    # request jobs / run heartbeat
    with open(f"{sys.argv[1]}/hmq.job") as fh:
        templatestr = fh.read()

    while True:
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
            print(Template(templatestr).substitute(variables))

        # check python env exists, build it otherwise
        # upgrade hmq in env

        # sleep
        time.sleep(10)


if __name__ == "__main__":
    main()
