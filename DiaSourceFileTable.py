__all__ = ("DiaSourceFileTable",)

from typing import Mapping

from .base import FileTable
from .schemas import DIASource
from .customTypes import ColumnName


class DiaSourceFileTable(FileTable):
    schema = DIASource
    index_columns = (ColumnName(x) for x in ("diaSourceId",))
    input_schema = ("ObjID", "observationId", "FieldMJD", "AstRange(km)",
                    "AstRangeRate(km/s)", "AstRA(deg)", "AstRARate(deg/day)",
                    "AstDec(deg)", "AstDecRate(deg/day)",
                    "Ast-Sun(J2000x)(km)", "Ast-Sun(J2000y)(km)",
                    "Ast-Sun(J2000z)(km)", "Sun-Ast-Obs(deg)",
                    "V", "FiltermagV(H=0)", "Filter")

    def _intrepret_row(self, interp_row: str) -> Mapping:
        return {k: v for k, v in zip(self.input_schema,  # type: ignore
                                     interp_row.split(','))}
