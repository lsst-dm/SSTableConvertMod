from __future__ import annotations

__all__ = ("MPCORB")

from datetime import datetime
from typing import Iterable
import itertools

from ..base import TableSchema


class MPCORB(TableSchema):
    """The orbit catalog produced by the Minor Planet Center. Ingested daily.
    O(10M) rows by survey end. The columns are described at
    https://minorplanetcenter.net//iau/info/MPOrbitFormat.html
    """
    mpcDesignation: str
    # MPCORB: Number or provisional designation (in packed form)

    mpcNumber: int
    # MPC number (if the asteroid has been numbered; NULL otherwise).
    # Provided for convenience.

    ssObjectId: int
    # LSST unique identifier (if observed by LSST)

    mpcH: float
    # MPCORB: Absolute magnitude, H

    mpcG: float
    # MPCORB: Slope parameter, G

    epoch: float
    # MPCORB: Epoch (in MJD, .0 TT)

    M: float
    # MPCORB: Mean anomaly at the epoch, in degrees

    peri: float
    # MPCORB: Argument of perihelion, J2000.0 (degrees)

    node: float
    # MPCORB: Longitude of the ascending node, J2000.0 (degrees)

    incl: float
    # MPCORB: Inclination to the ecliptic, J2000.0 (degrees)

    e: float
    # MPCORB: Orbital eccentricity

    n: float
    # MPCORB: Mean daily motion (degrees per day)

    a: float
    # MPCORB: Semimajor axis (AU)

    uncertaintyParameter: str
    # MPCORB: Uncertainty parameter, U

    reference: str
    # MPCORB: Reference

    nobs: int
    # MPCORB: Number of observations

    nopp: int
    # MPCORB: Number of oppositions

    arc: float
    # MPCORB: Arc (days), for single-opposition objects

    arcStart: datetime
    # MPCORB: Year of first observation (for multi-opposition objects)

    arcEnd: datetime
    # MPCORB: Year of last observation (for multi-opposition objects)

    rms: float
    # MPCORB: r.m.s residual (\")

    pertsShort: str
    # MPCORB: Coarse indicator of perturbers
    # (blank if unperturbed one-opposition object)

    pertsLong: str
    # MPCORB: Precise indicator of perturbers
    # (blank if unperturbed one-opposition object)

    computer: str
    # MPCORB: Computer name

    flags: int
    # MPCORB: 4-hexdigit flags.
    # See https://minorplanetcenter.net//iau/info/MPOrbitFormat.html
    # for details

    fullDesignation: str
    # MPCORB: Readable designation

    lastIncludedObservation: float
    # MPCORB: Date of last observation included in orbit solution

    covariance: Iterable[float]
    # MPCORB: Covariances, details TBD

    def byte_stream(self):
        #stream = []
        #for field in self._fields:
            #stream.append(f"{getattr(self, field)}".encode())
            #stream.append(b",")
        #stream.pop()
        #stream.append(b'\n')
        #return stream
        return (f"{field},".encode() for field in self)
