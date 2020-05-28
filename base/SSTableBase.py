from __future__ import annotations

__all__ = ("FileTableMode", "FileTable")

from abc import ABC
from dataclasses import dataclass
from enum import Enum, auto
from itertools import islice
from mmap import mmap
from typing import (Iterable, Generator, ClassVar, Optional, Type,
                    Mapping, Tuple, Union)
import pickle
import csv

from .SSSchemaBase import TableSchema

from ..customTypes import ColumnName  # type: ignore


class FileTableMode(Enum):
    CREATE = auto()
    OPEN = auto()


class CustomDict(dict):
    def insert(self, name, value):
        dict.__setitem__(self, name, value)
        return name[1]


@dataclass
class FileTable(ABC):
    schema: ClassVar[Type[TableSchema]]
    # Schema object to use when building the output table

    index_columns: ClassVar[Iterable[ColumnName]]
    # Column names to generate indexes for when creating table

    input_filename: str
    # Path to the file to use as input when buildint an ouput table

    output_filename: str
    # Path the output table will be saved to

    input_schema: ClassVar[Union[Type[TableSchema], Tuple[str, ...]]]
    # This is the schema of the input file

    mode: FileTableMode = FileTableMode.CREATE
    # When initializing the object, should it do a creation step or just a load

    skip_rows: int = 0
    # The number of rows to skip when processing the file, useful for skipping
    # header info when creating a file

    stop_after: Optional[int] = None
    # Only process a given number of rows when creating a file. i.e. stop will
    # be skip_rows + stop_after

    def __init_subclass__(cls):
        if not hasattr(cls, "schema"):
            raise NotImplementedError("Subclasses of FileTable must"
                                      "implement class attribute "
                                      "schema")
        if not hasattr(cls, "index_columns"):
            raise NotImplementedError("Subclasses of FileTable must"
                                      "implement class attribute "
                                      "index_columns")
        cls.index_columns = tuple(cls.index_columns)
        cls.index_pos = {cls.schema._pos_field[column]: column for column in
                         cls.index_columns}
        if not hasattr(cls, "input_schema"):
            raise NotImplementedError("Subclasses of FileTable must"
                                      "implement class attribute "
                                      "input_schema")
        cls = dataclass(cls, init=False)

    def __post_init__(self):
        self._file_handle = None
        self._mmap: Optional[mmap] = None

        if self.mode is FileTableMode.OPEN:
            pass

    def _make_rows(self, input_rows,
                   columns: Optional[Iterable[ColumnName]] = None) ->\
            Generator[Generator[str, None, None], None, None]:
        if columns is None:
            registry = self.schema.registry
        else:
            registry = self.schema.registry_subset(columns)
        # make the generator objects for each column
        if self.stop_after is not None:
            stop: Optional[int] = self.skip_rows + self.stop_after
        else:
            stop = None
        for file_row in islice(input_rows, self.skip_rows, stop):
            if file_row == '\n':
                return
            file_row = self._intrepret_row(file_row.decode())
            yield (registry[column](file_row) if column in registry else
                   "null"
                   for column in self.schema._fields)  # type: ignore

    def _intrepret_row(self, interp_row: str) -> Mapping:
        return {k: v for k, v in zip(self.input_schema,  # type: ignore
                                     interp_row.split())}

    def make_file(self, columns: Optional[Iterable[ColumnName]] = None,
                  do_index=False) -> None:
        indexes = CustomDict()
        with open(self.input_filename, "r+b") as in_file,\
                open(self.output_filename, 'w+') as out_file:
            writer = csv.writer(out_file, quoting=csv.QUOTE_NONE)
            with mmap(in_file.fileno(), 0) as mm_in:
                rows_generator = (row_raw for row_raw in iter(mm_in.readline,
                                  b""))
                rows = self._make_rows(rows_generator, columns)
                if do_index:
                    writer.writerows((indexes.insert((self.index_pos[i], b),
                                                     out_file.tell())
                                      if i in self.index_pos else b for i, b
                                      in enumerate(row_gen)) for row_gen in
                                     rows)
                else:
                    writer.writerows(rows)
        if do_index and self.index_columns and indexes:
            with open(self.output_filename+".sidecar", "w+b") as sidecar:
                pickle.dump(indexes, sidecar)

    def __del__(self):
        # These should be done in this order
        if self._mmap is not None:
            self._mmap.close()
        if self._file_handle is not None:
            self._file_handle.close()
