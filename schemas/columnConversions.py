from __future__ import annotations

__all__ = ()

from sys import maxsize
from hashlib import sha1
from itertools import count
from typing import MutableMapping, Mapping

from .DiaSourceSchema import DIASource
from .MPCORBSchema import MPCORB

from ..customTypes import ColumnName

DIA_SOURCE_ID_COUNTER = count()

DIASOURCE_SSID_CACHE: MutableMapping = {}


@DIASource.register(ColumnName("ssObjectId"))
def convert_objId_dia(row: Mapping) -> bytes:
    value = row['ObjID']
    return DIASOURCE_SSID_CACHE.setdefault(value, convert_objId_base(value))


@MPCORB.register(ColumnName("ssObjectId"))
def convert_objId_mcorb(row: Mapping) -> bytes:
    return convert_objId_base(row["S3MID"])


def convert_objId_base(value) -> bytes:
    """Convert a S3M object id into an integer to use for
    solarsystem object id in tables.

    Computes the hash of the s3m object id, and digests it as a
    hex value. This is then converted into a python int. Take the
    mode of this value with the max size of int we support.

    Note
    ----
    It is possible to get collisions with both the hash and the
    mod operation, but this seems unlikely in practice so it is
    not something to worry about as this is temporary anyway
    """
    return f"{int(sha1(value).hexdigest(), 16) % maxsize}".encode()


@DIASource.register(ColumnName("diaSourceId"))
def build_diaSourceId(row: Mapping) -> bytes:
    """This function simply returns the next number in a sequence
    each time that it is called
    """
    return f"{next(DIA_SOURCE_ID_COUNTER)}".encode()


@DIASource.register(ColumnName("ra"))
def return_ra(row: Mapping) -> bytes:
    return row["AstRA(deg)"]


@DIASource.register(ColumnName("decl"))
def return_decl(row: Mapping) -> bytes:
    return row["AstDec(deg)"]


@DIASource.register(ColumnName("totFlux"))
def build_totFlux(row: Mapping) -> bytes:
    """Converts from V in input file to nmgy

    This should be looked at again, as I dont fully know the meaning
    of the columns, but I suspect this is a visual mag, and I suspect
    the table should be in flux in that filter, but without colors I
    cant do that conversion.
    """
    value = f'{10**((22.5 - float(row["V"]))/2.5)}'.encode()
    return value


@MPCORB.register(ColumnName("mpcDesignation"))
def return_mpcDesignation(row: Mapping) -> bytes:
    return row["S3MID"]


@MPCORB.register(ColumnName("e"))
def return_e(row: Mapping) -> bytes:
    return row["e"]


@MPCORB.register(ColumnName("incl"))
def return_i(row: Mapping) -> bytes:
    return row["i"]


@MPCORB.register(ColumnName("mpcH"))
def return_h(row: Mapping) -> bytes:
    return row["H"]
