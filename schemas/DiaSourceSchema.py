from __future__ import annotations

__all__ = ("DIASource")

from datetime import datetime

from ..base import TableSchema


class DIASource(TableSchema):
    diaSourceId: int
    # Unique id.

    ccdVisitId: int
    # Id of the ccdVisit where this diaSource was measured. Note that we are
    # allowing a diaSource to belong to multiple amplifiers, but it may not
    # span multiple ccds.

    diaObjectId: int
    # 

    ssObjectId: int
    parentDiaSourceId: int
    prv_procOrder: int
    ssObjectReassocTime: datetime
    midPointTai: float
    ra: float
    raSigma: float
    decl: float
    declSigma: float
    ra_decl_Cov: float
    x: float
    xSigma: float
    y: float
    ySigma: float
    x_y_Cov: float
    apFlux: float
    apFluxErr: float
    snr: float
    psFlux: float
    psFluxSigma: float
    psRa: float
    psRaSigma: float
    psDecl: float
    psDeclSigma: float
    psFlux_psRa_Cov: float
    psFlux_psDecl_Cov: float
    psRa_psDecl_Cov: float
    psLnL: float
    psChi2: float
    psNdata: int
    trailFlux: float
    trailFluxSigma: float
    trailRa: float
    trailRaSigma: float
    trailDecl: float
    trailDeclSigma: float
    trailLength: float
    trailLengthSigma: float
    trailAngle: float
    trailAngleSigma: float
    trailFlux_trailRa_Cov: float
    trailFlux_trailDecl_Cov: float
    trailFlux_trailLength_Cov: float
    trailFlux_trailAngle_Cov: float
    trailRa_trailDecl_Cov: float
    trailRa_trailLength_Cov: float
    trailRa_trailAngle_Cov: float
    trailDecl_trailLength_Cov: float
    trailDecl_trailAngle_Cov: float
    trailLength_trailAngle_Cov: float
    trailLnL: float
    trailChi2: float
    trailNdata: int
    dipMeanFlux: float
    dipMeanFluxSigma: float
    dipFluxDiff: float
    dipFluxDiffSigma: float
    dipRa: float
    dipRaSigma: float
    dipDecl: float
    dipDeclSigma: float
    dipLength: float
    dipLengthSigma: float
    dipAngle: float
    dipAngleSigma: float
    dipMeanFlux_dipFluxDiff_Cov: float
    dipMeanFlux_dipRa_Cov: float
    dipMeanFlux_dipDecl_Cov: float
    dipMeanFlux_dipLength_Cov: float
    dipMeanFlux_dipAngle_Cov: float
    dipFluxDiff_dipRa_Cov: float
    dipFluxDiff_dipDecl_Cov: float
    dipFluxDiff_dipLength_Cov: float
    dipFluxDiff_dipAngle_Cov: float
    dipRa_dipDecl_Cov: float
    dipRa_dipLength_Cov: float
    dipRa_dipAngle_Cov: float
    dipDecl_dipLength_Cov: float
    dipDecl_dipAngle_Cov: float
    dipLength_dipAngle_Cov: float
    dipLnL: float
    dipChi2: float
    dipNdata: int
    totFlux: float
    totFluxErr: float
    diffFlux: float
    diffFluxErr: float
    fpBkgd: float
    fpBkgdErr: float
    ixx: float
    ixxSigma: float
    iyy: float
    iyySigma: float
    ixy: float
    ixySigma: float
    ixx_iyy_Cov: float
    ixx_ixy_Cov: float
    iyy_ixy_Cov: float
    ixxPSF: float
    iyyPSF: float
    ixyPSF: float
    extendedness: float
    spuriousness: float
    flags: int
    htmId20: int
