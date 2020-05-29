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
    """This is the base class for all file based tables. It is not intended
    to be initialized itself, so its base class is an abstract base class.

    This class implements efficient conversion of an input file to an output
    file by making use of a memory map around a file handle. This allows one
    row of a file to be loaded into memory, processed, and written out at a
    time. It is possible that for small files a more efficient converter be
    constructed operating on the entire file at one time, but that solution
    does not scale to larger files. This solution is a very efficient byte
    stream conversion, ballencing cpu utalization with memory pressure. An
    invocation of this class should require very little memory, and allow
    many such instances to be run simultaniously.

    Subclasses need to define 3 class attributes:
    * schema - An derived class of TableSchema that describes the format of
               the object post conversion, and has functions registred to
               convert columns.
    * index_columns - This needs to be an iterable of ColumnNames. ColumnNames
                      is an alias of str, but it helps the type checker keep
                      track of what should be expected, and makes typing hints
                      clearer. This iterable corresponds to what columns should
                      be indexed if make_file is run with do_index = True.
                      These indexes allow efficient searching for particular
                      rows if the file is re-loaded as a python object. Set to
                      an empty tuple if no columns are to be indexed.
    * input_schema - Iterable of str. This is the schema of the input file to
                     convert. This is spelled out rather than reading the first
                     line of a file to a) decrease runtime, as this is not
                     expected to change, and b) support files that do not have
                     headers.

    Subclasses may need to implement the _intrepret_row method if rows in their
    input file are not deliniated by a space, or if there is a more complicated
    mapping of schema name to column. The default simply splits a line on a
    space character, and creates a dictionary mapping between the column name
    and value.
    """
    schema: ClassVar[Type[TableSchema]]
    # Schema object to use when building the output table

    index_columns: ClassVar[Iterable[ColumnName]]
    # Column names to generate indexes for when creating table

    input_schema: ClassVar[Union[Type[TableSchema], Tuple[str, ...]]]
    # This is the schema of the input file

    mode: FileTableMode = FileTableMode.CREATE
    # When initializing the object, should it do a creation step or just a load

    filename: Optional[str] = None
    # If mode is set to FileTableMode.Open, this should be set to the path of
    # on disk representation of the class.

    def __init_subclass__(cls):
        """This handles adding all the appropriate attributes and validates that
        a subclass has implemented the required fields.

        Additionally this automatically converts a subclass into a dataclass,
        to save on keeping track of an extra class decorator.
        """
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
        """Fires after the automatically generated __init__ method so that
        additional processing can happen for dataclass initialization.
        """
        self._file_handle = None
        self._mmap: Optional[mmap] = None

        if self.mode is FileTableMode.OPEN:
            pass

    def _make_rows(self, input_rows: Generator[bytes, None, None],
                   columns: Optional[Iterable[ColumnName]] = None,
                   skip_rows=0, stop_after=None) ->\
            Generator[Generator[str, None, None], None, None]:
        """This funciton is responsible for creating generators that apply
        schema conversions to a generator of input row mappings. If a schema
        does not have a conversion function registered for a given column,
        null is yielded instead.

        The function itself yields a generator for each row yielded from
        the input_rows generator, making it a generator of generators.
        """
        if columns is None:
            registry = self.schema.registry
        else:
            registry = self.schema.registry_subset(columns)
        # make the generator objects for each column
        if stop_after is not None:
            stop: Optional[int] = skip_rows + stop_after
        else:
            stop = None
        for file_row in islice(input_rows, skip_rows, stop):
            if file_row == '\n':
                return
            file_row = self._intrepret_row(file_row.decode())
            yield (registry[column](file_row) if column in registry else
                   "null"
                   for column in self.schema._fields)  # type: ignore

    def _intrepret_row(self, interp_row: str) -> Mapping:
        """A method responsible for converting a string representation of
        a row in an input file into a mapping of input schema to value.
        """
        return {k: v for k, v in zip(self.input_schema,  # type: ignore
                                     interp_row.split())}

    @classmethod
    def make_file(cls, input_filename: str, output_filename: str,
                  skip_rows: int, stop_after=None,
                  columns: Optional[Iterable[ColumnName]] = None,
                  do_index=False) -> None:
        """
        Parameters
        ----------
        input_filename : `str`
            Path to the file to use as input when buildint an ouput table
        output_filename : `str`
            Path the output table will be saved to
        skip_rows : `int`
            The number of rows to skip when processing the file, useful for
            skipping header info when creating a file
        stop_after : `int`
            Only process a given number of rows when creating a file. i.e.
            stop will be skip_rows + stop_after
        columns : `Iterable of str`
            List of columns in cls.schema to convert, incase a subset of
            columns are to be converted.
        do_index : `bool`
            When making an input file, should the columns defined in
            cls.index_columns be indexed? This is useful for reopening the
            file later, but doubles the conversion time.
        """
        fileTable = cls()
        indexes = CustomDict()
        with open(input_filename, "r+b") as in_file,\
                open(output_filename, 'w+') as out_file:
            writer = csv.writer(out_file, quoting=csv.QUOTE_NONE)
            with mmap(in_file.fileno(), 0) as mm_in:
                rows_generator = (row_raw for row_raw in iter(mm_in.readline,
                                  b""))
                rows = fileTable._make_rows(rows_generator, columns, skip_rows,
                                            stop_after)
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
        if do_index and cls.index_columns and indexes:
            with open(output_filename+".sidecar", "w+b") as sidecar:
                pickle.dump(indexes, sidecar)

    def __del__(self):
        """Once this class has been expanded to support loading in already
        proccessed files, this method makes sure the file handlers and
        memory mapping are properly closed.
        """
        # These should be done in this order
        if self._mmap is not None:
            self._mmap.close()
        if self._file_handle is not None:
            self._file_handle.close()
