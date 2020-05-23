import click
import sys
from os import path


@click.group()
def cli():
    pass


@click.command()
@click.option("--sim_file", help="Path to the file to read in")
@click.option("--dia_table_file", help="Path where the file should be saved")
@click.option("--skip_rows", help="Number or rows to skip when building a file", default=0)
def DiaSourceTable(sim_file, dia_table_file, skip_rows):
    if not sim_file:
        print("Open only mode not yet supported")
        sys.exit(1)
    if not dia_table_file:
        print("A path for the dia table must be supplied")
    if not path.exists(sim_file):
        print("input file does not exist")
    from .DiaSourceFileTable import DiaSourceFileTable
    table = DiaSourceFileTable(sim_file, dia_table_file, skip_rows=skip_rows)
    table.make_file()


@click.command()
@click.option("--sim_file", help="Path to the file to read in")
@click.option("--dia_table_file", help="Path where the file should be saved")
@click.option("--skip_rows", help="Number or rows to skip when building a file", default=0)
def MCORBTable(sim_file, dia_table_file, skip_rows):
    if not sim_file:
        print("Open only mode not yet supported")
        sys.exit(1)
    if not dia_table_file:
        print("A path for the dia table must be supplied")
    if not path.exists(sim_file):
        print("input file does not exist")
    from .MPCORBFileTable import MCORBFileTable
    table = MCORBFileTable(sim_file, dia_table_file, skip_rows=skip_rows)
    table.make_file()


cli.add_command(DiaSourceTable)
cli.add_command(MCORBTable)
