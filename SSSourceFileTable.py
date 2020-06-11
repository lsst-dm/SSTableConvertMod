from __future__ import annotations

__all__ = ("SSSourceFileTable",)

from typing import (Tuple, ClassVar, Optional, Iterable,
                    Generator, Type, Dict)
import csv
from dataclasses import dataclass, InitVar

from .base import FileTable, IndexDict, TableSchema
from .schemas import SSSource
from .customTypes import ColumnName
from . import MPCORBFileTable


@dataclass
class FileTableStub:
    mpc_file_table: InitVar[MPCORBFileTable]

    _file_table: ClassVar[Type[FileTable]] = FileTable

    def _make_rows(self, input_rows: Iterable[bytes],
                   columns: Optional[Iterable[ColumnName]] = None,
                   skip_rows=0, stop_after=None) ->\
            Generator[Generator[str, None, None], None, None]:
        return self._file_table._make_rows(self,  # type: ignore
                                           input_rows, columns,
                                           skip_rows, stop_after)

    def _intrepret_row(self, row: Dict) -> Dict:
        identifier = ('ssObjectId', row[''])
        extra = self.dia_file_table.get_with_index(identifier)  # type: ignore
        row.update(extra)
        return row


@dataclass
class SSSourceFileTable:
    schema: ClassVar[Type[TableSchema]] = SSSource
    index_columns: ClassVar[Tuple[ColumnName]] = (ColumnName("ssObjectId"),)

    def __post_init__(self):
        self.index_pos = {self.schema.field_pos[column]: column for column in
                          self.index_columns}

    @classmethod
    def make_file(cls, input_mpc: str, input_csv: str,
                  output_filename: str,
                  csv_skip_rows: int = 0,
                  csv_stop_after: Optional[int] = None,
                  columns: Optional[Iterable[ColumnName]] = None,
                  do_index=False) -> None:
        mpc_file_table = MPCORBFileTable(filename=input_mpc)
        indexes = IndexDict()
        stub = FileTableStub(mpc_file_table, dia_file_table)
        fileTable = cls()

        with open(output_filename, 'w+') as out_file,\
                open(input_csv, 'rb') as input_file:
            writer = csv.writer(out_file, quoting=csv.QUOTE_NONE)
            writer.writerow(cls.schema.fields.keys())
            with mmap(input_file.fileno(), 0) as mm_in:
                rows_generator = 
                rows = stub._make_rows(rows_generator, columns, mpc_skip_rows,
                                    mpc_stop_after)
                if do_index:
                    writer.writerows((indexes.insert((fileTable.index_pos[i],
                                                    b),
                                                    out_file.tell())
                                    if i in fileTable.index_pos else b
                                    for i, b
                                    in enumerate(row_gen)) for row_gen in
                                    rows)
                else:
                    writer.writerows(rows)
