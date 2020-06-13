from __future__ import annotations

__all__ = ()

from sys import maxsize
import astropy.coordinates as apc
from hashlib import sha1
from typing import MutableMapping, Mapping
from functools import lru_cache

from .DiaSourceSchema import DIASource
from .MPCORBSchema import MPCORB
from .SSSourceSchema import SSSource

from ..customTypes import ColumnName

DIASOURCE_SSID_CACHE: MutableMapping = {}


@SSSource.register(ColumnName("ssObjectId"))
@DIASource.register(ColumnName("ssObjectId"))
def convert_objId_dia(row: Mapping) -> str:
    value = row['ObjID']
    return DIASOURCE_SSID_CACHE.setdefault(value, convert_objId_base(value))


@MPCORB.register(ColumnName("ssObjectId"))
def convert_objId_mcorb(row: Mapping) -> str:
    return convert_objId_base(row["S3MID"])


def convert_objId_base(value) -> str:
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
    return f"{int(sha1(value.encode()).hexdigest(), 16) % maxsize}"


@SSSource.register(ColumnName("diaSourceId"))
@DIASource.register(ColumnName("diaSourceId"))
def build_diaSourceId(row: Mapping) -> str:
    """This function simply returns the next number in a sequence
    each time that it is called
    """
    sub_string = f"{row['ObjID']}{row['AstRA(deg)']}{row['AstDec(deg)']}"
    return f"{int(sha1(sub_string.encode()).hexdigest(), 16) % maxsize}"


@DIASource.register(ColumnName("ra"))
def return_ra(row: Mapping) -> str:
    return row["AstRA(deg)"]


@DIASource.register(ColumnName("decl"))
def return_decl(row: Mapping) -> str:
    return row["AstDec(deg)"]


@DIASource.register(ColumnName("totFlux"))
def build_totFlux(row: Mapping) -> str:
    """Converts from V in input file to nmgy

    This should be looked at again, as I dont fully know the meaning
    of the columns, but I suspect this is a visual mag, and I suspect
    the table should be in flux in that filter, but without colors I
    cant do that conversion.
    """
    return f'{10**((22.5 - float(row["V"]))/2.5)}'


@MPCORB.register(ColumnName("mpcDesignation"))
def return_mpcDesignation(row: Mapping) -> str:
    return row["S3MID"]


@MPCORB.register(ColumnName("e"))
def return_e(row: Mapping) -> str:
    return row["e"]


@MPCORB.register(ColumnName("incl"))
def return_i(row: Mapping) -> str:
    return row["i"]


@MPCORB.register(ColumnName("mpcH"))
def return_h(row: Mapping) -> str:
    return row["H"]


@SSSource.register(ColumnName("eclipticLambda"))
def make_ecliptic_lamba(row: Mapping):
    return build_ecliptic_coord(row['ra'])


@lru_cache(maxsize=1000)
def build_ecliptic_coord(ra: float, dec: float) -> apc.SkyCoord:
    coord = build_astropy_coord(ra, dec)
    return coord.transform_to(apc.GeocentricMeanEcliptic)


@lru_cache(maxsize=1000)
def build_astropy_coord(ra: float, dec: float) -> apc.SkyCoord:
    return apc.SkyCoord(ra=ra, dec=dec, frame=apc.ICRS, unit='deg')
