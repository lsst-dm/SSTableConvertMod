from __future__ import annotations

__all__ = ()

from sys import maxsize
import astropy.units as u  # or use conversion factors to save compute time?
from coord import CelestialCoord, degrees, _Angle, util
from hashlib import sha1
from typing import MutableMapping, Mapping, TYPE_CHECKING, Tuple
from functools import lru_cache
import math
import numpy as np

from .DiaSourceSchema import DIASource
from .MPCORBSchema import MPCORB
from .SSSourceSchema import SSSource
from .SSObjectSchema import SSObject
from ..base.SSTableBase import NoIndexError

from ..customTypes import ColumnName

if TYPE_CHECKING:
    from ..SSObjectFileTable import SSObjectRow


DIASOURCE_SSID_CACHE: MutableMapping = {}

# conversions
KM_TO_AU = u.km.to(u.au)
KM_PER_SECOND_TO_AU_PER_DAY = (u.km/u.s).to(u.au/u.day)
MAS_TO_DEG = u.mas.to(u.deg)
DEG2RAD = np.deg2rad(1)
RAD2DEG = 1/DEG2RAD

# constants for galactic transform
el0 = 32.93191857 * degrees
r0 = 282.859481208 * degrees
d0 = 62.8717488056 * degrees
sind0, cosd0 = d0.sincos()

# constants for ecliptic
ep = util.ecliptic_obliquity(2000.0)
sin_ep, cos_ep = ep.sincos()


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
@SSSource.register(ColumnName("eclipticLambda"))
def make_ecliptic_lamba(row: Mapping):
    return f"{build_ecliptic_coord(float(row['AstRA(deg)']), float(row['AstDec(deg)']))[0]}"  # noqa: E501


@SSSource.register(ColumnName("eclipticBeta"))
def make_ecliptic_beta(row: Mapping):
    return f"{build_ecliptic_coord(float(row['AstRA(deg)']), float(row['AstDec(deg)']))[1]}"  # noqa: E501


@SSSource.register(ColumnName("galacticL"))
def make_galactic_l(row: Mapping) -> str:
    return f"{build_galactic_coord(float(row['AstRA(deg)']), float(row['AstDec(deg)']))[0]}"  # noqa: E501


@SSSource.register(ColumnName("galacticB"))
def make_galactic_b(row: Mapping) -> str:
    return f"{build_galactic_coord(float(row['AstRA(deg)']), float(row['AstDec(deg)']))[1]}"  # noqa: E501


@lru_cache(maxsize=1000)
def build_coord(ra: float, dec: float) -> CelestialCoord:
    coord = CelestialCoord.__new__(CelestialCoord)
    coord._ra = _Angle(ra*DEG2RAD)
    coord._dec = _Angle(dec*DEG2RAD)
    coord._x = None
    coord._sindec, coord._cosdec = (math.sin(coord._dec._rad),
                                    math.cos(coord._dec._rad))
    coord._sinra, coord._cosra = (math.sin(coord._ra._rad),
                                  math.cos(coord._ra._rad))
    coord._x = coord._cosdec * coord._cosra
    coord._y = coord._cosdec * coord._sinra
    coord._z = coord._sindec
    return coord


@lru_cache(maxsize=1000)
def build_galactic_coord(ra: float, dec: float) -> Tuple[float, float]:
    celestial = build_coord(ra, dec)
    sind, cosd = math.sin(celestial.dec._rad), math.cos(celestial.dec._rad)
    tmp = celestial.ra-r0
    sinr, cosr = math.sin(tmp._rad), math.cos(tmp._rad)

    cbcl = cosd*cosr
    cbsl = sind*sind0 + cosd*sinr*cosd0
    sb = sind*cosd0 - cosd*sinr*sind0
    el = (_Angle(math.atan2(cbsl, cbcl)) + el0).wrap(_Angle(math.pi))
    return (el.deg, math.asin(sb)*RAD2DEG)


@lru_cache(maxsize=1000)
def build_ecliptic_coord(ra: float, dec: float) -> Tuple[float, float]:
    celestial = build_coord(ra, dec)
    x, y, z = celestial.get_xyz()
    #return celestial.ecliptic()
    x_ecl = x
    y_ecl = cos_ep*y + sin_ep*z
    z_ecl = -sin_ep*y + cos_ep*z
    beta = math.asin(z_ecl)*RAD2DEG
    lam = _Angle(math.atan2(y_ecl, x_ecl))

    return (lam.wrap().deg, beta)


@SSSource.register(ColumnName("phaseAngle"))
def return_phaseAngle(row: Mapping) -> str:
    return row['Sun-Ast-Obs(deg)']


@SSSource.register(ColumnName("heliocentricDist"))
def return_heliocentricDist(row: Mapping) -> str:
    if not row['Ast-Sun(J2000x)(km)'] or not row['Ast-Sun(J2000y)(km)'] or\
            not row['Ast-Sun(J2000z)(km)']:
        return '\\N'
    value = math.sqrt(float(row['Ast-Sun(J2000x)(km)'])**2 +
                      float(row['Ast-Sun(J2000y)(km)'])**2 +
                      float(row['Ast-Sun(J2000z)(km)'])**2)
    return f"{value*KM_TO_AU}"


@SSSource.register(ColumnName("topocentricDist"))
def return_topocentricDist(row: Mapping) -> str:
    if not row['AstRange(km)']:
        return '\\N'
    return f"{float(row['AstRange(km)'])*KM_TO_AU}"


@SSSource.register(ColumnName("predictedMagnitude"))
def return_predicted_mag(row: Mapping) -> str:
    return row['V']


@SSSource.register(ColumnName("predictedMagnitudeSigma"))
def return_photometricSigma(row: Mapping) -> str:
    return row["PhotometricSigma(mag)"][:-1]


@SSSource.register(ColumnName("predictedRaSigma"))
def return_predicted_ra_sigma(row: Mapping) -> str:
    if row['AstRASigma(mas)'] == '':
        return '\\N'
    return f"{float(row['AstRASigma(mas)'])* MAS_TO_DEG}"


@SSSource.register(ColumnName("predictedDecSigma"))
def return_predicted_dec_sigma(row: Mapping) -> str:
    if row['AstDecSigma(mas)'] == '':
        return '\\N'
    return f"{float(row['AstDecSigma(mas)'])* MAS_TO_DEG}"


@SSSource.register(ColumnName('predictedMagnitude'))
def predMag(row: Mapping) -> str:
    return f'{float(row["Filtermag"])}'


@SSSource.register(ColumnName('predictedMagnitudeSigma'))
def predMagSig(row: Mapping) -> str:
    return f'{float(row["PhotometricSigma(mag)"])}'


@SSSource.register(ColumnName('residualRa'))
def residualRa(row: Mapping) -> str:
    if not row['AstRA(deg)'] or not row['AstRASigma(mas)']:
        return '\\N'
    ra = float(row['AstRA(deg)'])
    ras = float(row['AstRASigma(mas)'])*MAS_TO_DEG
    ran = np.random.normal(ra, ras)
    return f"{ran}"


@SSSource.register(ColumnName('residualDec'))
def residualDec(row: Mapping) -> str:
    if not row['AstDec(deg)'] or not row['AstDecSigma(mas)']:
        return '\\N'
    dec = float(row['AstDec(deg)'])
    decs = float(row['AstDecSigma(mas)'])*MAS_TO_DEG
    ran = np.random.normal(dec, decs)
    return f"{ran}"


# @SSSource.register(ColumnName('predictedRaDecCov'))
# add things

@SSSource.register(ColumnName('heliocentricX'))
def helioX(row: Mapping) -> str:
    if row['Ast-Sun(J2000x)(km)'] == '':
        return '\\N'
    return f"{float(row['Ast-Sun(J2000x)(km)'])*KM_TO_AU}"


@SSSource.register(ColumnName('heliocentricY'))
def helioY(row: Mapping) -> str:
    if row['Ast-Sun(J2000y)(km)'] == '':
        return '\\N'
    return f"{float(row['Ast-Sun(J2000y)(km)'])*KM_TO_AU}"


@SSSource.register(ColumnName('heliocentricZ'))
def helioZ(row: Mapping) -> str:
    if row['Ast-Sun(J2000z)(km)'] == '':
        return '\\N'
    return f"{float(row['Ast-Sun(J2000z)(km)'])*KM_TO_AU}"

# @SSSource.register(ColumnName('heliocentricVX'))
# add things

# @SSSource.register(ColumnName('heliocentricVY'))
# add things

# @SSSource.register(ColumnName('heliocentricVZ'))
# add things


@SSSource.register(ColumnName('topocentricX'))
def topoX(row: Mapping) -> str:
    if not row['AstRA(deg)'] or not row['AstDec(deg)'] or\
            not row['AstRange(km)']:
        return '\\N'
    ra = float(row['AstRA(deg)'])
    dec = float(row['AstDec(deg)'])
    dau = float(row['AstRange(km)'])*KM_TO_AU
    x = np.cos(dec*DEG2RAD)*np.cos(ra*DEG2RAD)*dau
    return f"{x}"


@SSSource.register(ColumnName('topocentricY'))
def topoY(row: Mapping) -> str:
    if not row['AstRA(deg)'] or not row['AstDec(deg)'] or\
            not row['AstRange(km)']:
        return '\\N'
    ra = float(row['AstRA(deg)'])
    dec = float(row['AstDec(deg)'])
    dau = float(row['AstRange(km)'])*KM_TO_AU
    y = np.cos(dec*DEG2RAD)*np.sin(ra*DEG2RAD)*dau
    return f"{y}"


@SSSource.register(ColumnName('topocentricZ'))
def topoZ(row: Mapping) -> str:
    if not row['AstDec(deg)'] or not row['AstRange(km)']:
        return '\\N'
    dec = float(row['AstDec(deg)'])
    dau = float(row['AstRange(km)'])*KM_TO_AU
    z = np.sin(dec*DEG2RAD)*dau
    return f"{z}"


@SSSource.register(ColumnName('topocentricVX'))
def topoVX(row: Mapping) -> str:
    if not row['AstRA(deg)'] or not row['AstDec(deg)'] or\
            not row['AstRange(km)'] or not row['AstRangeRate(km/s)'] or\
            not row['AstRARate(deg/day)'] or not row['AstDecRate(deg/day)']:
        return '\\N'
    ra = float(row['AstRA(deg)'])
    dec = float(row['AstDec(deg)'])
    dau = float(row['AstRange(km)'])*KM_TO_AU
    vda = float(row['AstRangeRate(km/s)'])*KM_PER_SECOND_TO_AU_PER_DAY
    vra = float(row['AstRARate(deg/day)'])
    vdec = float(row['AstDecRate(deg/day)'])
    vx = np.cos(dec*DEG2RAD)*np.cos(ra*DEG2RAD)*vda + dau *\
        np.cos(dec*DEG2RAD)*np.sin(ra*DEG2RAD)*(vra*DEG2RAD) +\
        dau * np.sin(dec*DEG2RAD)*np.cos(ra*DEG2RAD) *\
        (vdec * DEG2RAD)
    return f"{vx}"


@SSSource.register(ColumnName('topocentricVY'))
def topoVY(row: Mapping) -> str:
    if not row['AstRA(deg)'] or not row['AstDec(deg)'] or\
            not row['AstRange(km)'] or not row['AstRangeRate(km/s)'] or\
            not row['AstRARate(deg/day)'] or not row['AstDecRate(deg/day)']:
        return '\\N'
    ra = float(row['AstRA(deg)'])
    dec = float(row['AstDec(deg)'])
    dau = float(row['AstRange(km)'])*KM_TO_AU
    vda = float(row['AstRangeRate(km/s)'])*KM_PER_SECOND_TO_AU_PER_DAY
    vra = float(row['AstRARate(deg/day)'])
    vdec = float(row['AstDecRate(deg/day)'])
    vy = np.cos(dec*DEG2RAD)*np.sin(ra*DEG2RAD)*vda - dau *\
        np.cos(dec*DEG2RAD)*np.cos(ra*DEG2RAD)*(vra*DEG2RAD) +\
        dau * np.sin(dec*DEG2RAD)*np.sin(ra*DEG2RAD) *\
        (vdec * DEG2RAD)
    return f"{vy}"


@SSSource.register(ColumnName('topocentricVZ'))
def topoVZ(row: Mapping) -> str:
    if not row['AstRA(deg)'] or  not row['AstRange(km)'] or\
            not row['AstRangeRate(km/s)'] or\
            not row['AstDecRate(deg/day)']:
        return '\\N'
    dec = float(row['AstDec(deg)'])
    dau = float(row['AstRange(km)'])*KM_TO_AU
    vda = float(row['AstRangeRate(km/s)'])*KM_PER_SECOND_TO_AU_PER_DAY
    vdec = float(row['AstDecRate(deg/day)'])
    vz = np.sin(dec*DEG2RAD)*vda - dau * np.cos(dec*DEG2RAD) *\
        (vdec * DEG2RAD)
    return f"{vz}"
