from __future__ import annotations

__all__ = ("DIASource")

from ..base import TableSchema


class DIASource(TableSchema):
    diaSourceId: int
    ccdVisitId: int
    diaObjectId: int
    ssObjectId: int
    parentDiaSourceId: int
    prv_procOrder: int
    ssObjectReassocTime: int
    midPointTai: float
    ra: float
    raSigma: float
    decl: float
    filter: str
    mag: float
    declSigma: float
    ra_decl_Cov: float
    x: float
    xSigma: float
    y: float
    ySigma: float
    x_y_Cov: float
    snr: float
