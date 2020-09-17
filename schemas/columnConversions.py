from __future__ import annotations

__all__ = ()

from sys import maxsize
import astropy.coordinates as apc
import astropy.units as u # or use conversion factors to save compute time?
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
    return f"{build_ecliptic_coord(row['ra'], row['dec']).lon}"

@SSSource.register(ColumnName("eclipticBeta"))
def make_ecliptic_beta(row: Mapping):
    return f"{build_ecliptic_coord(row['ra'], row['dec']).lat}"

@SSSource.register(ColumnName('galacticL'))
def make_galactic_l(row: Mapping):
    return f"{build_galactic_coord(row['ra'], row['dec']).l}"

@SSSource.register(ColumnName('galacticB'))
def make_galactic_b(row: Mapping):
    return f"{build_galactic_coord(row['ra'], row['dec']).b}"

@SSSource.register(ColumnName('phaseAngle'))
def phaseAngle(row: Mapping) -> str:
    return f'{float(row["Sun-Ast-Obs(deg)"])}'

#Do all calculations have to happen inside f strings?
@SSSource.register(ColumnName('heliocentricDist'))
def helioDist(row: Mapping) -> str:
    x = float(row['Ast-Sun(J2000x)(km)'])
    y = float(row['Ast-Sun(J2000y)(km)'])
    z = float(row['Ast-Sun(J2000z)(km)'])
    d=np.sqrt((x)**2+(y)**2+(z)**2)*u.km
    dau=d.to(u.au)
    return f"{dau.value}"

@SSSource.register(ColumnName('topocentricDist'))
def topoDist(row: Mapping) -> str:
    d=float(row['AstRange(km)'])
    dkm=d*u.km
    dau=dkm.to(u.au)
    return f"{dau.value}"

@SSSource.register(ColumnName('predictedMagnitude'))
def predMag(row: Mapping) -> str:
    return f'{float(row["Filtermag"])}'

@SSSource.register(ColumnName('predictedMagnitudeSigma'))
def predMagSig(row: Mapping) -> str:
    return f'{float(row["PhotometricSigma(mag)"])}'

@SSSource.register(ColumnName('residualRa'))
def residualRa(row: Mapping) -> str:
    ra = float(row['AstRa(deg)'])
    rasm = float(row['AstRASigma(mas)'])*u.mas
    ras = rasm.to(u.deg).value
    ran = np.random.normal(ra,ras)
    return f"{ran}"

@SSSource.register(ColumnName('residualDec'))
def residualDec(row: Mapping) -> str:
    dec = float(row['AstDec(deg)'])
    decsm = float(row['AstDecSigma(mas)'])*u.mas
    decs = decsm.to(u.deg).value
    ran = np.random.normal(dec,decs)
    return f"{ran}"

@SSSource.register(ColumnName('predictedRaSigma'))
def predRaSigma(row: Mapping) -> str:
    ra = float(row['AstRaSigma(mas)'])*u.mas
    return f"{ra.to(u.deg).value}"

@SSSource.register(ColumnName('predictedDecSigma'))
def predDecSigma(row: Mapping) -> str:
    dec = float(row['AstDecSigma(mas)'])*u.mas
    return f"{dec.to(u.deg).value}"

@SSSource.register(ColumnName('predictedRaDecCov'))
#add things

@SSSource.register(ColumnName('heliocentricX'))
def helioX(row: Mapping) -> str:
    ex = float(row['Ast-Sun(J2000x)(km)'])*u.km
    return f"{ex.to(u.au).value}"

@SSSource.register(ColumnName('heliocentricY'))
def helioY(row: Mapping) -> str:
    wy = float(row['Ast-Sun(J2000y)(km)'])*u.km
    return f"{wy.to(u.au).value}"

@SSSource.register(ColumnName('heliocentricZ'))
def helioZ(row: Mapping) -> str:
    ze = float(row['Ast-Sun(J2000z)(km)'])*u.km
    return f"{ze.to(u.au).value}"

@SSSource.register(ColumnName('heliocentricVX'))
#add things

@SSSource.register(ColumnName('heliocentricVY'))
#add things

@SSSource.register(ColumnName('heliocentricVZ'))
#add things

@SSSource.register(ColumnName('topocentricX'))
def topoX(row: Mapping) -> str:
    ra = float(row['AstRa(deg)'])
    dec = float(row['AstDec(deg)'])
    dkm = float(row['AstRange(km)'])*u.km
    dau = dkm.to(u.au)
    x = np.cos(dec*np.pi/180.0)*np.cos(ra*np.pi/180.0)*dau.value
    return f"{x}"

@SSSource.register(ColumnName('topocentricY'))
def topoY(row: Mapping) -> str:
    ra = float(row['AstRa(deg)'])
    dec = float(row['AstDec(deg)'])
    dkm = float(row['AstRange(km)'])*u.km
    dau = dkm.to(u.au)
    y = np.cos(dec*np.pi/180.0)*np.sin(ra*np.pi/180.0)*dau.value
    return f"{y}"

@SSSource.register(ColumnName('topocentricZ'))
def topoZ(row: Mapping) -> str:
    dec = float(row['AstDec(deg)'])
    dkm = float(row['AstRange(km)'])*u.km
    dau = dkm.to(u.au)
    z=np.sin(dec*np.pi/180.0)*dau.value
    return f"{z}"

@SSSource.register(ColumnName('topocentricVX'))
def topoVX(row: Mapping) -> str:
    ra = float(row['AstRa(deg)'])
    dec = float(row['AstDec(deg)'])
    dkm = float(row['AstRange(km)'])*u.km
    dau = dkm.to(u.au)
    vr = float(row['AstRangeRate(km/s)'])*(u.km/u.s)
    vda = vr.to(u.au/u.day).value
    vra = float(row['AstRARate(deg/day)'])
    vdec = float(row['AstDecRate(deg/day)'])
    vx = np.cos(dec*np.pi/180.0)*np.cos(ra*np.pi/180.0)*vda + dau.value * np.cos(dec*np.pi/180.0)*np.sin(ra*np.pi/180.0)*(vra*np.pi/180.0) + dau.value * np.sin(dec*np.pi/180.0)*np.cos(ra*np.pi/180.0) * (vdec* np.pi/180.0)
    return f"{vx}"

@SSSource.register(ColumnName('topocentricVY'))
def topoVY(row: Mapping) -> str:
    ra = float(row['AstRa(deg)'])
    dec = float(row['AstDec(deg)'])
    dkm = float(row['AstRange(km)'])*u.km
    dau = dkm.to(u.au)
    vr = float(row['AstRangeRate(km/s)'])*(u.km/u.s)
    vda = vr.to(u.au/u.day).value
    vra = float(row['AstRARate(deg/day)'])
    vdec = float(row['AstDecRate(deg/day)'])
    vy = np.cos(dec*np.pi/180.0)*np.sin(ra*np.pi/180.0)*vda - dau.value * np.cos(dec*np.pi/180.0)*np.cos(ra*np.pi/180.0)*(vra*np.pi/180.0) + dau.value * np.sin(dec*np.pi/180.0)*np.sin(ra*np.pi/180.0)*(vdec* np.pi/180.0)
    return f"{vy}"

@SSSource.register(ColumnName('topocentricVZ'))
def topoVZ(row: Mapping) -> str:
    dec = float(row['AstDec(deg)'])
    dkm = float(row['AstRange(km)'])*u.km
    dau = dkm.to(u.au)
    vr = float(row['AstRangeRate(km/s)'])*(u.km/u.s)
    vda = vr.to(u.au/u.day).value
    vdec = float(row['AstDecRate(deg/day)'])
    vz = np.sin(dec*np.pi/180.0)*vda - dau.value * np.cos(dec*np.pi/180.0)*(vdec* np.pi/180.0)
    return f"{vz}"

@lru_cache(maxsize=1000)
def build_ecliptic_coord(ra: float, dec: float) -> apc.SkyCoord:
    coord = build_astropy_coord(ra, dec)
    return coord.transform_to(apc.GeocentricMeanEcliptic)

@lru_cache(maxsize=1000)
def build_galactic_coord(ra: float, dec: float) -> apc.SkyCoord:
    coord = build_astropy_coord(ra, dec)
    return coord.transform_to(apc.Galactic)

@lru_cache(maxsize=1000)
def build_astropy_coord(ra: float, dec: float) -> apc.SkyCoord:
    return apc.SkyCoord(ra=ra, dec=dec, frame=apc.ICRS, unit='deg')
