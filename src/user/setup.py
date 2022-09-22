from setuptools import setup, find_packages
setup(
    name='hmq',
    install_requires=[
        'click',
    ],
    entry_points={
        'console_scripts': [
            'hmq=hmq.hmq:cli'
        ],
    },
)
