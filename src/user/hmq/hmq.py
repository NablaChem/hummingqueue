import click
from . import owner


cli = click.CommandCollection(
    sources=[
        owner.owner_init_group,
    ]
)

if __name__ == "__main__":
    cli()
