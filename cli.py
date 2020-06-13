import click

from . import MPCORBFT, DiaSourceFT, SSObjectFT, SSSourceFT


@click.group(name="SSTableConvertMod")
def cli():
    pass


@click.command()
@click.option("--skip_rows", help="Number or rows to skip when building a"
              " file", default=0)
@click.option("--stop_after", help="stop after N rows have been converted",
              default=None)
@click.argument("input_fileglob")
@click.argument("output_filename")
def mpcorb(input_fileglob, output_filename, skip_rows, stop_after):
    MPCORBFT.builder(input_fileglob=input_fileglob,
                     output_filename=output_filename,
                     skip_rows=skip_rows,
                     stop_after=stop_after).run()


@click.command()
@click.option("--skip_rows", help="Number or rows to skip when building a"
              " file", default=0)
@click.option("--stop_after", help="stop after N rows have been converted",
              default=None)
@click.option("--do_index", help="Index the file as it is being created",
              default=True)
@click.argument("input_filename")
@click.argument("output_filename")
def dia(input_filename, output_filename, skip_rows, stop_after, do_index):
    DiaSourceFT.builder(input_filename=input_filename,
                        output_filename=output_filename,
                        skip_rows=skip_rows,
                        stop_after=stop_after,
                        do_index=do_index).run()


@click.command()
@click.option("--skip_rows", help="Number or rows to skip when building a"
              " file", default=0)
@click.option("--stop_after", help="stop after N rows have been converted",
              default=None)
@click.argument("input_dia_glob")
@click.argument("input_mpc_filename")
@click.argument("output_filename")
def ssobject(input_dia_glob, input_mpc_filename, output_filename,
             skip_rows, stop_after):
    SSObjectFT.builder(input_dia_glob=input_dia_glob,
                       input_mpc_filename=input_mpc_filename,
                       output_filename=output_filename,
                       skip_rows=skip_rows,
                       stop_after=stop_after).run()


@click.command()
@click.option("--skip_rows", help="Number or rows to skip when building a"
              " file", default=0)
@click.option("--stop_after", help="stop after N rows have been converted",
              default=None)
@click.argument("input_filename")
@click.argument("input_mpc_filename")
@click.argument("input_ssObject_filename")
@click.argument("output_filename")
def sssource(input_filename, input_mpc_filename, input_ssObject_filename,
             output_filename, skip_rows, stop_after):
    SSSourceFT.builder(input_filename=input_filename,
                       output_filename=output_filename,
                       input_mpc_filename=input_mpc_filename,
                       input_ssObject_filename=input_ssObject_filename,
                       skip_rows=skip_rows,
                       stop_after=stop_after).run()


cli.add_command(mpcorb)
cli.add_command(dia)
cli.add_command(ssobject)
cli.add_command(sssource)
