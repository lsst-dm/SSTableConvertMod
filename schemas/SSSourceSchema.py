from __future__ import annotations

__all__ = ("SSSource")

from ..base import TableSchema


class SSSource(TableSchema):
    ssObjectId: int
    diaSourceId: int
    eclipticLambda: float
    eclipticBeta: float
    galacticL: float
    galacticB: float
    phaseAngle: float
    heliocentricDist: float
    topocentricDist: float
    predictedMagnitude: float
    predictedMagnitudeSigma: float
    residualRa: float
    residualDec: float
    predictedRaSigma: float
    predictedDecSigma: float
    predictedRaDecCov: float
    heliocentricX: float
    heliocentricY: float
    heliocentricZ: float
