from setuptools import setup

with open("VERSION") as fh:
    version = fh.read().strip()
setup(
    name="hmq",
    version=version,
    description="Distributed computing.",
    url="https://github.com/NablaChem/hummingqueue",
    author="Guido Falk von Rudorff",
    author_email="guido@vonrudorff.de",
    license="AGPL-3.0",
    packages=["hmq"],
    zip_safe=False,
    install_requires=[
        "dnspython",
        "requests",
        "cloudpickle",
        "pynacl",
        "tqdm",
        "rq",
        "toml",
    ],
    entry_points={
        "console_scripts": ["hmq=hmq:cli"],
    },
)
