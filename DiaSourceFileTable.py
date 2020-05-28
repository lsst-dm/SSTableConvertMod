__all__ = ("DiaSourceFileTable",)

from .base import FileTable
from .schemas import DIASource
from .customTypes import ColumnName


class DiaSourceFileTable(FileTable):
    schema = DIASource
    index_columns = (ColumnName(x) for x in ("diaSourceId",))
    input_schema = ("ObjID", "FieldID", "FieldMJD", "AstRange(km)",
                    "AstRangeRate(km/s)", "AstRA(deg)", "AstRARate(deg/day)",
                    "AstDec(deg)", "AstDecRate(deg/day)",
                    "Ast-Sun(J2000x)(km)", "Ast-Sun(J2000y)(km)",
                    "Ast-Sun(J2000z)(km)", "Sun-Ast-Obs(deg)",
                    "V", "FiltermagV(H=0)", "Filter")
