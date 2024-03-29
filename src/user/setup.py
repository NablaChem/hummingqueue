from setuptools import setup

setup(
    name="hmq",
    version="VERSION",
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
        "numpy",
        "pandas",
    ],
    entry_points={
        "console_scripts": ["hmq=hmq:cli"],
    },
)
