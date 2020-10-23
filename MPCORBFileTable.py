from __future__ import annotations

__all__ = ("MPCORBFileTable",)

import csv
from glob import glob
from itertools import islice
from typing import Dict, Optional, Iterable, Generator

from .base import FileTableBuilder, FileTable, Indexer
from .schemas import MPCORB
from .customTypes import ColumnName


class MPCORBBuilder(FileTableBuilder):
    input_schema = ("S3MID", "FORMAT", "q", "e", "i", "Omega",
                    "argperi", "t_p", "H", "t_0", "INDEX",
                    "N_PAR", "MOID", "COMPCODE")

    def __init__(self, parent: FileTable, input_fileglob: str,
                 output_filename: str,
                 skip_rows: int,
                 stop_after: Optional[int] = None,
                 columns: Optional[Iterable[ColumnName]] = None):
        self.parent = parent
        self.output_filename = output_filename
        self.skip_rows = 0
        self.stop_after = None
        self.columns = columns
        self.input_fileglob = input_fileglob
        self._mpc_skip_start = skip_rows
        self._mpc_stop_after = stop_after
        self.do_index = True

        self.index_pos = {self.parent.schema.field_pos[column]: column
                          for column in
                          self.parent.index_columns}

    def _get_input_rows(self) -> Generator:
        if self._mpc_stop_after is not None:
            stop = self._mpc_stop_after + self._mpc_stop_after
        else:
            stop = None  # type: ignore
        for path in glob(self.input_fileglob):
            with open(path, 'rb') as in_file:
                yield from islice(in_file.readlines(), self._mpc_skip_start,
                                  stop)

    def _intrepret_row(self, interp_row: str) -> Dict:
        return {k: v for k, v in zip(self.input_schema,
                                     interp_row.split())}

    def run(self):
        with open(self.output_filename, 'w+', newline='') as out_file:
            indexer = Indexer(self.do_index,
                              self.output_filename+".sidecar",
                              self.parent.index_columns)
            writer = csv.writer(out_file, quoting=csv.QUOTE_NONE,
                                lineterminator="\n")
            writer.writerow(self.parent.schema.fields.keys())
            rows_generator = self._get_input_rows()
            rows = self._make_rows(rows_generator, self.columns,
                                   self.skip_rows,
                                   self.stop_after)
            writer.writerows(indexer.insert(
                (b, i in self.index_pos) for i, b in enumerate(row_gen))
                for row_gen in rows)


class MPCORBFileTable(FileTable):
    schema = MPCORB
    index_columns = tuple(ColumnName(x) for x in ("ssObjectId", "mpcH"))
    builder = MPCORBBuilder
