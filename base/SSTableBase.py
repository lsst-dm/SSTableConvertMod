from __future__ import annotations
from abc import ABC, abstractclassmethod
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from itertools import islice, chain, repeat
from mmap import mmap
from typing import (Iterable, Generator, ClassVar, Optional, Dict, Any, Type,
                    Mapping)
import yaml
import pickle

from .SSSchemaBase import TableSchema

from ..customTypes import ColumnName  # type: ignore


class FileTableMode(Enum):
    CREATE = auto()
    OPEN = auto()


def index_dict_creator():
    return defaultdict(dict)


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

    mode: FileTableMode = FileTableMode.CREATE
    # When initializing the object, should it do a creation step or just a load

    indexes: Dict[ColumnName, Any] = field(default_factory=index_dict_creator,
                                           init=False)
    # A dict of indexes corresponding to the column names in index_columns
    # This is only populated after a create operation, or on an open operation

    skip_rows: int = 0
    # The number of rows to skip when processing the file, useful for skipping
    # header info when creating a file

    stop_after: Optional[int] = None
    # Only process a given number of rows when creating a file. i.e. stop will
    # be skip_rows + stop_after

    def __init_subclass__(cls):
        if not hasattr(cls, "index_columns"):
            raise NotImplementedError("Subclasses of FileTable must"
                                      "implement class attribute "
                                      "index_columns")
        cls.index_columns = tuple(cls.index_columns)
        if not hasattr(cls, "schema"):
            raise NotImplementedError("Subclasses of FileTable must"
                                      "implement class attribute "
                                      "schema")
        cls = dataclass(cls, init=False)

    def __post_init__(self):
        self._file_handle = None
        self._mmap: Optional[mmap] = None

        if self.mode is FileTableMode.OPEN:
            pass

    def _make_rows(self, input_rows,
                   columns: Optional[Iterable[ColumnName]] = None) ->\
            Generator[Generator[bytes, None, None], None, None]:
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
            file_row = self._intrepret_row(file_row)
            yield (registry[column](file_row)
                   for column in self.schema._fields)  # type: ignore

    @abstractclassmethod
    def _intrepret_row(self, row: bytes) -> Mapping:
        pass

    def make_file(self, columns: Optional[Iterable[ColumnName]] = None) ->\
            None:
        with open(self.input_filename, "r+b") as in_file,\
                open(self.output_filename, 'w+b') as out_file:
            with mmap(in_file.fileno(), 0) as mm_in:
                rows_generator = (row_raw for row_raw in iter(mm_in.readline,
                                  b""))
                rows = self._make_rows(rows_generator, columns)
                for row_gen in rows:
                    #line = str(row)
                    #line_bytes = line.encode()
                    #if self.index_columns:
                        # for index in self.index_columns:
                            # row_value = getattr(row, index)
                            # if row_value is not NullValue:
                                # self.indexes[index][row_value] = bytes_counter
                    #bytes_counter += len(line_bytes)
                    #intermediate_chain = chain.from_iterable(zip(row_gen,
                                                                 #repeat(b',')))
                    #out_file.writelines(chain(intermediate_chain, (b'\n',)))
                    position = out_file.tell()
                    for pos, b in enumerate(row_gen):
                        if False and self.index_columns and\
                           self.schema._field_pos[pos] in self.index_columns:
                            self.indexes[self.schema._field_pos[pos]][b] =\
                                position
                        out_file.write(b)
                        out_file.write(b',')
                        out_file.write(b'\n')
                    #bytes_counter += sub_counter
        if False and self.index_columns and self.indexes:
            with open(self.output_filename+".sidecar", "w+b") as sidecar:
                # yaml.dump(self.indexes, sidecar, sort_keys=False)
                pickle.dump(self.indexes, sidecar)

    def __del__(self):
        # These should be done in this order
        if self._mmap is not None:
            self._mmap.close()
        if self._file_handle is not None:
            self._file_handle.close()
