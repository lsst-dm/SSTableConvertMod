from __future__ import annotations

__all__ = ("MPCORB")

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from .SSSchemaBase import TableSchema


@dataclass
class MPCORB(TableSchema):
    """The orbit catalog produced by the Minor Planet Center. Ingested daily. O(10M) rows by survey end.
    The columns are described at https://minorplanetcenter.net//iau/info/MPOrbitFormat.html
    """
    mpcDesignation: str
    mpcNumber: int
    ssObjectId: int
    mpcH: float
    mpcG: float
    epoch: float
    M: float
    peri: float
    node: float
    incl: float
    e: float
    n: float
    a: float
    uncertaintyParameter: str
    reference: str
    nobs: int
    nopp: int
    arc: float
    arcStart: datetime
    arcEnd: datetime
    rms: float
    pertsShort: str
    pertsLong: str
    computer: str
    flags: int
    fullDesignation: str
    lastIncludedObservation: float
    covariance: Iterable[float]
