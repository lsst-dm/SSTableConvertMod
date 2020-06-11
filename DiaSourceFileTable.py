__all__ = ("DiaSourceFileTable",)

from .base import FileTable, FileTableBuilder
from .schemas import DIASource
from .customTypes import ColumnName


class DiaSourceBuilder(FileTableBuilder):
    input_schema = ("ObjID", "observationId", "FieldMJD", "AstRange(km)",
                    "AstRangeRate(km/s)", "AstRA(deg)", "AstRARate(deg/day)",
                    "AstDec(deg)", "AstDecRate(deg/day)",
                    "Ast-Sun(J2000x)(km)", "Ast-Sun(J2000y)(km)",
                    "Ast-Sun(J2000z)(km)", "Sun-Ast-Obs(deg)",
                    "V", "FiltermagV(H=0)", "Filter")


class DiaSourceFileTable(FileTable):
    schema = DIASource
    index_columns = (ColumnName(x) for x in ("diaSourceId", "ssObjectId"))
    builder = DiaSourceBuilder
