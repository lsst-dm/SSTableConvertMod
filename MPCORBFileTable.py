from __future__ import annotations

__all__ = ("MPCORBFileTable",)

from typing import Dict

from .base import FileTable, FileTableBuilder
from .schemas import MPCORB
from .customTypes import ColumnName


class MPCORBBuilder(FileTableBuilder):
    input_schema = ("S3MID", "FORMAT", "q", "e", "i", "Omega",
                    "argperi", "t_p", "H", "t_0", "INDEX",
                    "N_PAR", "MOID", "COMPCODE")

    def _intrepret_row(self, interp_row: str) -> Dict:
        return {k: v for k, v in zip(self.input_schema,
                                     interp_row.split())}


class MPCORBFileTable(FileTable):
    schema = MPCORB
    index_columns = tuple(ColumnName(x) for x in ("mpcDesignation",
                                                  "ssObjectId",))
    builder = MPCORBBuilder
