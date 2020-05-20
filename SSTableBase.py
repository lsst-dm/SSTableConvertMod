from __future__ import annotations
from abc import ABC, abstractclassmethod
from dataclasses import dataclass, field
from typing import Iterable, Generator, ClassVar, Optional, Dict, Any
from enum import Enum, auto
from mmap import mmap
import yaml

from .SSSchemaBase import TableSchema, NullValue

from .customTypes import ColumnName


class FileTableMode(Enum):
    CREATE = auto()
    OPEN = auto()


@dataclass
class FileTable(ABC):
    schema: ClassVar[TableSchema]
    index_columns: ClassVar[Iterable[str]]
    input_filename: str
    output_filename: str
    mode: FileTableMode
    indexes: Dict[ColumnName, Any] = field(default_factory=dict, init=False)

    def __init_subclass__(cls):
        if not hasattr(cls, "index_columns"):
            raise NotImplementedError("Subclasses of FileTable must"
                                      "implement class attribute "
                                      "index_columns")
        if not hasattr(cls, "schema"):
            raise NotImplementedError("Subclasses of FileTable must"
                                      "implement class attribute "
                                      "schema")

    def __init__(self, input_filename, output_filename, mode):
        self.input_filename = input_filename
        self.output_filename = output_filename
        self.mode = mode

        self._file_handle = None
        self._mmap: Optional[mmap] = None

        if self.mode is FileTableMode.OPEN:
            pass

    def _make_rows(self, input_file,
                   columns: Optional[Iterable[ColumnName]] = None) ->\
            Generator[TableSchema, None, None]:
        if columns is None:
            registry = self.schema.registry
        else:
            registry = self.schema.registry_subset(columns)
        # make the generator objects for each column
        for row in iter(input_file.readline, b""):
            row = row.decode()
            row = self._intrepret_row(row)
            yield self.schema(func(row) for func in registry.values())

    @abstractclassmethod
    def _intrepret_row(self, row: str) -> Iterable:
        pass

    def make_file(self, columns: Optional[Iterable[ColumnName]] = None) ->\
            None:
        bytes_counter = 0
        with open(self.input_filename, "r+b") as in_file,\
                open(self.output_filename, "w+b") as out_file:
            with mmap(in_file.fileno(), 0) as mm_in,\
                 mmap(out_file.fileno(), 0) as mm_out:
                rows = self._make_rows(mm_in, columns)
                for row in rows:
                    line = f"{','.join(f'{x}' for x in row)}\n"
                    line_bytes = line.encode()
                    if self.index_columns:
                        for index in self.index_columns:
                            if getattr(row, index) is not NullValue:
                                self.indexes[index] = bytes_counter
                    bytes_counter += len(line_bytes)
                    mm_out.write(line_bytes)
                mm_out.flush()
        if self.index_columns and self.indexes:
            with open(self.output_filename+".sidecar", "w") as sidecar:
                yaml.dump(self.indexes, sidecar, sort_keys=False)

    def __del__(self):
        # These should be done in this order
        if self._mmap is not None:
            self._mmap.close()
        if self._file_handle is not None:
            self._file_handle.close()
