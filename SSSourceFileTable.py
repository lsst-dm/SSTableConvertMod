from __future__ import annotations

__all__ = ("SSSourceFileTable",)

from .base import FileTable, FileTableBuilder
from .schemas import SSSource
from .customTypes import ColumnName


class SSSourceBuilder(FileTableBuilder):
    input_schema = ("ObjID", "observationId", "FieldMJD", "AstRange(km)",
                    "AstRangeRate(km/s)", "AstRA(deg)", "AstRARate(deg/day)",
                    "AstDec(deg)", "AstDecRate(deg/day)",
                    "Ast-Sun(J2000x)(km)", "Ast-Sun(J2000y)(km)",
                    "Ast-Sun(J2000z)(km)", "Sun-Ast-Obs(deg)",
                    "V", "Filtermag", "V(H=0)", "Filter", "AstRASigma(mas)",
                    "AstDecSigma(mas)", "PhotometricSigma(mag)")


class SSSourceFileTable(FileTable):
    schema = SSSource
    index_columns = tuple(ColumnName(x) for x in ("ssObjectId",))
    builder = SSSourceBuilder
