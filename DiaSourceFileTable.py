__all__ = ("DiaSourceFileTable",)

from dataclasses import dataclass
from typing import Iterable

from .base import FileTable
from .schemas import DIASource
from .customTypes import ColumnName


@dataclass
class DiaSourceFileTable(FileTable):
    schema = DIASource
    index_columns = (ColumnName(x) for x in ("ssObjectId",))

    def _intrepret_row(self, row: str) -> Iterable:
        row_split = row.split()
        schema = ("ObjID FieldID FieldMJD AstRange(km) AstRangeRate(km/s) "
                  "AstRA(deg) AstRARate(deg/day) AstDec(deg)"
                  "AstDecRate(deg/day) Ast-Sun(J2000x)(km) "
                  "Ast-Sun(J2000y)(km) Ast-Sun(J2000z)(km) Sun-Ast-Obs(deg) "
                  "V FiltermagV(H=0) Filter ".split())
        return {k: v for k, v in zip(schema, row_split)}
