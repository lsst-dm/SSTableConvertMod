__all__ = ("MCORBFileTable",)

from .base import FileTable
from .schemas import MPCORB
from .customTypes import ColumnName


class MCORBFileTable(FileTable):
    schema = MPCORB
    index_columns = tuple(ColumnName(x) for x in ("mpcDesignation",
                                                  "ssObjectId",))
    input_schema = ("S3MID", "FORMAT", "q", "e", "i", "Omega",
                    "argperi", "t_p", "H", "t_0", "INDEX",
                    "N_PAR", "MOID", "COMPCODE")
