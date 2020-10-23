from __future__ import annotations

__all__ = ()

from sys import maxsize
#import astropy.coordinates as apc
from hashlib import sha1
from typing import MutableMapping, Mapping, TYPE_CHECKING
from functools import lru_cache
import math

from .DiaSourceSchema import DIASource
from .MPCORBSchema import MPCORB
from .SSSourceSchema import SSSource
from .SSObjectSchema import SSObject
from ..base.SSTableBase import NoIndexError

from ..customTypes import ColumnName

if TYPE_CHECKING:
    from ..SSObjectFileTable import SSObjectRow


DIASOURCE_SSID_CACHE: MutableMapping = {}


@SSSource.register(ColumnName("ssObjectId"))
@DIASource.register(ColumnName("ssObjectId"))
def convert_objId_dia(row: Mapping) -> str:
    value = row['ObjID']
    if value == "FD" or value == "NS":
        return '\\N'
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


@DIASource.register(ColumnName("ccdVisitId"))
def return_ccdVisitId(row: Mapping) -> str:
    return row["observationId"]


@DIASource.register(ColumnName("ra"))
def return_ra(row: Mapping) -> str:
    return row["AstRA(deg)"]


@DIASource.register(ColumnName("decl"))
def return_decl(row: Mapping) -> str:
    return row["AstDec(deg)"]


@DIASource.register(ColumnName("mag"))
def build_totFlux(row: Mapping) -> str:
    return row["Filtermag"]


@DIASource.register(ColumnName("midPointTai"))
def build_mid_point_time(row: Mapping) -> str:
    return row["FieldMJD"]


@DIASource.register(ColumnName("filter"))
def build_dia_filter(row: Mapping) -> str:
    return row["Filter"]


@MPCORB.register(ColumnName("mpcDesignation"))
def return_mpcDesignation(row: Mapping) -> str:
    return row["S3MID"]


@MPCORB.register(ColumnName("mpcH"))
def return_h(row: Mapping) -> str:
    return row["H"]


@MPCORB.register(ColumnName("mpcG"))
def return_g(row: Mapping) -> str:
    return f"{0.15}"


@MPCORB.register(ColumnName("epoch"))
def return_epoch(row: Mapping) -> str:
    return row["t_0"]


@MPCORB.register(ColumnName("tPeri"))
def return_tPeri(row: Mapping) -> str:
    return row['t_p']


@MPCORB.register(ColumnName("peri"))
def return_peri(row: Mapping) -> str:
    return row["argperi"]


@MPCORB.register(ColumnName("node"))
def return_node(row: Mapping) -> str:
    return row['Omega']


@MPCORB.register(ColumnName("incl"))
def return_i(row: Mapping) -> str:
    return row["i"]


@MPCORB.register(ColumnName("e"))
def return_e(row: Mapping) -> str:
    return row["e"]


@MPCORB.register(ColumnName("q"))
def return_q(row: Mapping) -> str:
    return row["q"]


# ### SSObject ####
@SSObject.register(ColumnName("ssObjectId"))
def return_ssObjectId(row: SSObjectRow) -> str:
    return f"{row.ssobjectid}"


@SSObject.register(ColumnName("numObs"))
def count_obs(row: SSObjectRow) -> str:
    return f"{len(row.dia_list)}"


@SSObject.register(ColumnName("arc"))
def calculate_arc(row: SSObjectRow) -> str:
    midPointTai_list = [d['midPointTai'] for d in row.dia_list]
    if '\\N' in midPointTai_list:
        return '\\N'
    sort_dates = sorted(float(x) for x in midPointTai_list)
    return f"{sort_dates[-1] - sort_dates[0]}"


@SSObject.register(ColumnName("uH"))
@SSObject.register(ColumnName("gH"))
@SSObject.register(ColumnName("rH"))
@SSObject.register(ColumnName("iH"))
@SSObject.register(ColumnName("zH"))
@SSObject.register(ColumnName("yH"))
def pass_through_h(row: SSObjectRow) -> str:
    entry = row.mpc_entry
    if entry is None:
        return '\\N'
    if entry is NoIndexError:
        return '\\N'
    return f"{entry['mpcH']}"


# ### SSSource ####
#@SSSource.register(ColumnName("eclipticLambda"))
#def make_ecliptic_lamba(row: Mapping):
    #return f"{build_ecliptic_coord(float(row['AstRA(deg)']), float(row['AstDec(deg)'])).lon}"[:-4]


#@SSSource.register(ColumnName("eclipticBeta"))
#def make_ecliptic_beta(row: Mapping):
    #return f"{build_ecliptic_coord(float(row['AstRA(deg)']), float(row['AstDec(deg)'])).lat}"[:-4]


#@lru_cache(maxsize=1000)
#def build_ecliptic_coord(ra: float, dec: float) -> apc.SkyCoord:
    #coord = build_astropy_coord(ra, dec)
    #return coord.transform_to(apc.GeocentricMeanEcliptic)


#@SSSource.register(ColumnName("galacticL"))
#def make_galactic_l(row: Mapping) -> str:
    #return f"{build_galactic_coord(float(row['AstRA(deg)']), float(row['AstDec(deg)'])).l}"[:-4]


#@SSSource.register(ColumnName("galacticB"))
#def make_galactic_b(row: Mapping) -> str:
    #return f"{build_galactic_coord(float(row['AstRA(deg)']), float(row['AstDec(deg)'])).b}"[:-4]


#@lru_cache(maxsize=1000)
#def build_galactic_coord(ra: float, dec: float) -> apc.SkyCoord:
    #coord = build_astropy_coord(ra, dec)
    #return coord.transform_to(apc.Galactic)


#@lru_cache(maxsize=1000)
#def build_astropy_coord(ra: float, dec: float) -> apc.SkyCoord:
    #return apc.SkyCoord(ra=ra, dec=dec, frame=apc.ICRS, unit='deg')


@SSSource.register(ColumnName("phaseAngle"))
def return_phaseAngle(row: Mapping) -> str:
    return row['Sun-Ast-Obs(deg)']

@SSSource.register(ColumnName("heliocentricDist"))
def return_heliocentricDist(row: Mapping) -> str:
    if row['Ast-Sun(J2000x)(km)'] == '' or row['Ast-Sun(J2000y)(km)'] == ''\
            or row['Ast-Sun(J2000z)(km)'] == '':
        return '\\N'
    value = math.sqrt(float(row['Ast-Sun(J2000x)(km)'])**2 +
                      float(row['Ast-Sun(J2000y)(km)'])**2 +
                      float(row['Ast-Sun(J2000z)(km)'])**2)
    return f"{value}"


@SSSource.register(ColumnName("topocentricDist"))
def return_topocentricDist(row: Mapping) -> str:
    return row['AstRange(km)']


@SSSource.register(ColumnName("predictedMagnitude"))
def return_predicted_mag(row: Mapping) -> str:
    return row['V']


@SSSource.register(ColumnName("predictedMagnitudeSigma"))
def return_photometricSigma(row: Mapping) -> str:
    return row["PhotometricSigma(mag)"][:-1]


@SSSource.register(ColumnName("residualRa"))
def return_residual_ra(row: Mapping) -> str:
    return '0'


@SSSource.register(ColumnName("residualDec"))
def return_residual_dec(row: Mapping) -> str:
    return '0'


@SSSource.register(ColumnName("predictedRaSigma"))
def return_predicted_ra_sigma(row: Mapping) -> str:
    if row['AstRASigma(mas)'] == '':
        return '\\N'
    return f"{float(row['AstRASigma(mas)'])* 2.7777777777778E-7}"


@SSSource.register(ColumnName("predictedDecSigma"))
def return_predicted_dec_sigma(row: Mapping) -> str:
    if row['AstDecSigma(mas)'] == '':
        return '\\N'
    return f"{float(row['AstDecSigma(mas)'])* 2.7777777777778E-7}"


@SSSource.register(ColumnName("heliocentricX"))
def return_heliocentri_x(row: Mapping) -> str:
    return row['Ast-Sun(J2000x)(km)']


@SSSource.register(ColumnName("heliocentricY"))
def return_heliocentri_y(row: Mapping) -> str:
    return row['Ast-Sun(J2000y)(km)']


@SSSource.register(ColumnName("heliocentricZ"))
def return_heliocentri_z(row: Mapping) -> str:
    return row['Ast-Sun(J2000z)(km)']