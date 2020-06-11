import click
import sys
from os import path

from .base.SSTableBase import FileTable

@click.group(name="SSTableConvertMod")
def cli():
    pass


def make_function(name, fileTableClass):
    def wrapperFunc(input_file, output_file, skip_rows, stop_after):
        if not input_file:
            print("Open only mode not yet supported")
            sys.exit(1)
        if not output_file:
            print("A path for the dia table must be supplied")
        if not path.exists(input_file):
            print("input file does not exist")

        fileTableClass.builder(input_file, output_file, skip_rows=skip_rows,
                               stop_after=stop_after).run()
    wrapperFunc.__name__ = name
    return wrapperFunc


for fileTableClass in FileTable.__subclasses__():
    function = make_function(f"{fileTableClass.__name__}func", fileTableClass)
    function = click.command(name=fileTableClass.__name__.lower())(function)
    function = click.option("--input_file", help="Path to the file to read in")(function)
    function = click.option("--output_file", help="Path where the file should be saved")(function)
    function = click.option("--skip_rows", help="Number or rows to skip when building a"
                  " file", default=0)(function)
    function = click.option("--stop_after", help="stop after N rows have been converted",
                  default=None)(function)
    cli.add_command(function, name=fileTableClass.__name__.lower())
