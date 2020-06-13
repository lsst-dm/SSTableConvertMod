from __future__ import annotations

__all__ = ("SSObject")


from ..base import TableSchema


class SSObject(TableSchema):
    """LSST-computed per-object quantities. 1:1 relationship with MPCORB (w.
    no entries in SSObject where there are no LSST observations). Recomputed
    daily, upon MPCORB ingestion.
    """
    ssObjectId: int
    discoverySubmissionDate: float
    firstObservationDate: float
    arc: float
    numObs: int
    lcPeriodic: bytes
    MOID: float
    MOIDTrueAnomaly: float
    MOIDEclipticLongitude: float
    MOIDDeltaV: float
    uH: float
    uG12: float
    uHErr: float
    uG12Err: float
    uH_uG12_Cov: float
    uChi2: float
    uNdata: int
    gH: float
    gG12: float
    gHErr: float
    gG12Err: float
    gH_gG12_Cov: float
    gChi2: float
    gNdata: int
    rH: float
    rG12: float
    rHErr: float
    rG12Err: float
    rH_rG12_Cov: float
    rChi2: float
    rNdata: int
    iH: float
    iG12: float
    iHErr: float
    iG12Err: float
    iH_iG12_Cov: float
    iChi2: float
    iNdata: int
    zH: float
    zG12: float
    zHErr: float
    zG12Err: float
    zH_zG12_Cov: float
    zChi2: float
    zNdata: int
    yH: float
    yG12: float
    yHErr: float
    yG12Err: float
    yH_yG12_Cov: float
    yChi2: float
    yNdata: int
    maxExtendedness: float
    minExtendedness: float
    medianExtendedness: float
    flags: int
