__all__ = ("MCORBFileTable",)

from typing import Mapping, Tuple

from .base import FileTable
from .schemas import MPCORB
from .customTypes import ColumnName


class MCORBFileTable(FileTable):
    schema = MPCORB
    index_columns = (ColumnName(x) for x in ("mpcDesignation", "ssObjectId",))
    input_schema: Tuple[str, ...] = ("S3MID", "FORMAT", "q", "e", "i", "Omega",
                                     "argperi", "t_p", "H", "t_0", "INDEX",
                                     "N_PAR", "MOID", "COMPCODE")

    def _intrepret_row(self, interp_row: bytes) -> Mapping:
        return {k: v for k, v in zip(self.input_schema, interp_row.split())}
