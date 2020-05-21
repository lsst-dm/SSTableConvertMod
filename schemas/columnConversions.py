from __future__ import annotations

__all__ = ()

from sys import maxsize
from itertools import count
from typing import Mapping

from .DiaSourceSchema import DIASource
from .MPCORBSchema import MPCORB

from ..customTypes import ColumnName

DIA_SOURCE_ID_COUNTER = count()


@DIASource.register(ColumnName("ssObjectId"))
@MPCORB.register(ColumnName("ssObjectId"))
def convert_objId(row: Mapping) -> int:
    """Convert a S3M object id into an integer to use for
    solarsystem object id in tables.

    Computes the hash of the s3m object id, which is an
    a positive or negative integer. Adds the maxsize
    supported on the platform if the result is negative.
    """
    hashed_value = hash(row["ObjID"])
    if hashed_value < 0:
        hashed_value += maxsize
        # Splitting this up is more efficient for how python
        # handles temporary big numbers. Adding one is needed
        # because maxsize is 2**n - 1
        hashed_value += 1

    return hashed_value


@DIASource.register(ColumnName("diaSourceId"))
def build_diaSourceId(row: Mapping) -> int:
    """This function simply returns the next number in a sequence
    each time that it is called
    """
    return next(DIA_SOURCE_ID_COUNTER)


@DIASource.register(ColumnName("ra"))
def return_ra(row: Mapping) -> float:
    return float(row["AstRA(deg)"])


@DIASource.register(ColumnName("decl"))
def return_decl(row: Mapping) -> float:
    return float(row["AstDec(deg)"])


@DIASource.register(ColumnName("totFlux"))
def build_totFlux(row: Mapping) -> float:
    """Converts from V in input file to nmgy

    This should be looked at again, as I dont fully know the meaning
    of the columns, but I suspect this is a visual mag, and I suspect
    the table should be in flux in that filter, but without colors I
    cant do that conversion.
    """
    return 10**((22.5 - row["V"])/2.5)


@MPCORB.register(ColumnName("mpcDesignation"))
def return_mpcDesignation(row: Mapping):
    return row["S3MID"]


@MPCORB.register(ColumnName("e"))
def return_e(row: Mapping):
    return row["e"]


@MPCORB.register(ColumnName("incl"))
def return_i(row: Mapping):
    return row["i"]


@MPCORB.register(ColumnName("mpcH"))
def return_h(row: Mapping):
    return row["H"]
