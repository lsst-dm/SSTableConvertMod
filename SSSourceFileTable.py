from __future__ import annotations

__all__ = ("SSSourceFileTable",)

from typing import (Optional, Iterable, Dict, cast)
import warnings

from .base import (FileTable, FileTableBuilder, NoIndexError)
from .schemas import SSSource
from .customTypes import ColumnName
from . import MPCORBFT, SSObjectFT


class SSSourceBuilder(FileTableBuilder):
    input_schema = ("ObjID", "observationId", "FieldMJD", "AstRange(km)",
                    "AstRangeRate(km/s)", "AstRA(deg)", "AstRARate(deg/day)",
                    "AstDec(deg)", "AstDecRate(deg/day)",
                    "Ast-Sun(J2000x)(km)", "Ast-Sun(J2000y)(km)",
                    "Ast-Sun(J2000z)(km)", "Sun-Ast-Obs(deg)",
                    "V", "Filtermag", "V(H=0)", "Filter", "AstRASigma(mas)",
                    "AstDecSigma(mas)", "PhotometricSigma(mag)")

    def __init__(self, parent: FileTable, input_filename: str,
                 output_filename: str, input_mpc_filename: str,
                 input_ssObject_filename: str,
                 skip_rows: int,
                 stop_after: Optional[int] = None,
                 columns: Optional[Iterable[ColumnName]] = None,
                 do_index: Optional[bool] = False):
        super().__init__(parent, input_filename, output_filename, skip_rows,
                         stop_after, columns, do_index)
        self.mpc_file = MPCORBFT(filename=input_mpc_filename)
        self.ssobject_file =\
            SSObjectFT(filename=input_ssObject_filename)

    def _intrepret_row(self, row: str) -> Dict:
        parsed = super()._intrepret_row(row)
        identifier = ('ssObjectId', parsed['ssObjectId'])
        extra = self.mpc_file.get_with_index(identifier)  # type: ignore
        if extra.__class__ is NoIndexError:
            warnings.warn(f"Cannot file s3m object with ssObjectId "
                          "{parsed['ssObjectId']}")
            extra = cast(NoIndexError, extra).row

        parsed.update(cast(Dict, extra))
        return parsed


class SSSourceFileTable(FileTable):
    schema = SSSource
    index_columns = (ColumnName(x) for x in ("diaSourceId", "ssObjectId"))
    builder = SSSourceBuilder
