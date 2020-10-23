from __future__ import annotations

__all__ = ("FileTable", "Indexer", "FileTableBuilder", "NoIndexError",
           "FileTableInMem", "Indexer")

from abc import ABC
from dataclasses import dataclass, InitVar
from itertools import islice
from mmap import mmap, PROT_READ
import pandas as pd
import sqlite3
import os
from typing import (Iterable, Generator, ClassVar, Optional, Type,
                    Mapping, Tuple, Union, Any, Dict, List)
import pickle
import csv
import sys
import time

from .SSSchemaBase import TableSchema

from ..customTypes import ColumnName


@dataclass
class BuilderDescriptor:
    shadowed: FileTableBuilder
    # The value of the shadowed builder

    def __get__(self, inst, typ):
        def wrapper(*args, **kwargs):
            return self.shadowed(typ, *args, **kwargs)
        return wrapper


class Indexer:
    def __init__(self, do_index: bool, filename: str, columns: Iterable[str]):
        self.do_index = do_index
        self.filename = filename
        if do_index:
            self.accumulate_len = 5000
            self.columns = tuple(columns)
            if os.path.exists(filename):
                os.remove(filename)
            self.db = sqlite3.connect(filename, timeout=10)
            self.c = self.db.cursor()
            command = f"create table IF NOT EXISTS ind " +\
                f"{tuple(f'{c} text ' for c in columns)}"
            command = command.replace("'", "").rstrip(',)')+')'
            self.c.execute(command)
            self.iteration = 0
            self.insert_command = "insert into ind values " +\
                f"{tuple('? ' for _ in range(len(columns)))}"
            self.insert_command =\
                self.insert_command.replace("'", "").rstrip(',)')+')'
            self.tracker = [None]*self.accumulate_len
            self.tracker_len = 0

    def insert(self, generator) -> Generator:
        if self.do_index:
            values = tuple(generator)
            self.tracker[self.tracker_len] = tuple(v for v, truth in
                                                   values if truth)
            self.tracker_len += 1
            if self.tracker_len == self.accumulate_len:
                with self.db:
                    while True:
                        try:
                            self.c.executemany(self.insert_command,
                                               self.tracker)
                            break
                        except sqlite3.OperationalError as e:
                            if "locked" in str(e):
                                time.sleep(0.1)
                self.tracker_len = 0
            yield from (value for value, _ in values)
        else:
            yield from (value for value, _ in generator)

    def __del__(self):
        if self.do_index:
            while True:
                try:
                    if self.tracker_len:
                        with self.db:
                            self.c.executemany(self.insert_command,
                                            self.tracker[:self.tracker_len])
                            self.tracker_len = 0
                    #self.c.execute("CREATE INDEX objid on ind(ssObjectId)")
                    self.db.commit()
                    self.db.close()
                    #self.c.executemany(self.insert_command,
                    #                    self.tracker)
                    break
                except sqlite3.OperationalError as e:
                    if "locked" in str(e):
                        time.sleep(1)


@dataclass
class NoIndexError:
    __slots__ = ("row",)
    row: Dict

    def __len__(self):
        return len(self.row)


class FileTableBuilder(ABC):
    """
    * input_schema - Iterable of str. This is the schema of the input file to
                     convert. This is spelled out rather than reading the first
                     line of a file to a) decrease runtime, as this is not
                     expected to change, and b) support files that do not have
                     headers.
    """
    # This is the schema of the input file
    input_schema: ClassVar[Tuple[Union[Type[TableSchema], str], ...]]
    INDEXER: Optional[str] = None

    def __init__(self, parent: FileTable, input_filename: str,
                 output_filename: str, skip_rows: int,
                 stop_after: Optional[int] = None,
                 columns: Optional[Iterable[ColumnName]] = None,
                 do_index: Optional[bool] = True):
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
        self.parent = parent
        self.input_filename = input_filename
        self.output_filename = output_filename
        self.skip_rows = skip_rows
        self.stop_after = stop_after
        self.columns = columns
        self.do_index = do_index

        self.index_pos = {self.parent.schema.field_pos[column]: column
                          for column in
                          self.parent.index_columns}

    def __init_subclass__(cls):
        """This handles adding all the appropriate attributes and validates that
        a subclass has implemented the required fields.

        Additionally this automatically converts a subclass into a dataclass,
        to save on keeping track of an extra class decorator.
        """
        if not hasattr(cls, "input_schema"):
            raise NotImplementedError("Subclasses of FileTable must"
                                      "implement class attribute "
                                      "input_schema")

    def _make_rows(self, input_rows: Iterable[bytes],
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
        registry = self.parent.schema.registry
        if columns is None:
            fields = self.parent.schema.fields
        else:
            fields = columns
        # make the generator objects for each column
        if stop_after is not None:
            stop: Optional[int] = skip_rows + stop_after
        else:
            stop = None
        for i, file_row in enumerate(islice(input_rows, skip_rows, stop)):
            if file_row == '\n':
                return
            try:
                file_row_interp = self._intrepret_row(file_row.decode())
            except UnicodeDecodeError:
                print(f"Error processing {self.input_filename}")
                sys.exit(1)
            yield (registry[column](file_row_interp) if column in registry else
                   "\\N"
                   for column in fields)

    def _intrepret_row(self, interp_row: str) -> Dict:
        """A method responsible for converting a string representation of
        a row in an input file into a mapping of input schema to value.
        """
        return {k: v for k, v in zip(self.input_schema,  # type: ignore
                                     interp_row.split(','))}

    def run(self):
        if self.INDEXER is not None:
            indexer = self.INDEXER
        else:
            indexer = Indexer
        indexes = indexer(self.do_index,
                          self.output_filename+".sidecar",
                          tuple(self.parent.index_columns)
                          )
        with open(self.input_filename, "rb") as in_file,\
                open(self.output_filename, 'w+', newline='') as out_file:
            writer = csv.writer(out_file, quoting=csv.QUOTE_NONE,
                                lineterminator="\n")
            writer.writerow(self.parent.schema.fields.keys())
            with mmap(in_file.fileno(), 0, prot=PROT_READ) as mm_in:
                rows_generator = iter(mm_in.readline, b"")
                rows = self._make_rows(rows_generator, self.columns,
                                       self.skip_rows,
                                       self.stop_after)
                writer.writerows(indexes.insert(
                    (b,
                     i in self.index_pos
                     )
                    for i, b in enumerate(row_gen)) for row_gen in rows)


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

    Subclasses may need to implement the _intrepret_row method if rows in their
    input file are not deliniated by a space, or if there is a more complicated
    mapping of schema name to column. The default simply splits a line on a
    comma character, and creates a dictionary mapping between the column name
    and value.
    """
    schema: ClassVar[Type[TableSchema]]
    # Schema object to use when building the output table

    index_columns: ClassVar[Iterable[ColumnName]]
    # Column names to generate indexes for when creating table

    builder: ClassVar[Type[FileTableBuilder]]
    # The builder class associated with this FileTable class

    filename: Optional[str] = None
    # If mode is set to FileTableMode.Open, this should be set to the path of
    # on disk representation of the class.

    do_index: InitVar[bool] = True
    # When opening a file containing with the table information, create an
    # index if a side-car is not present

    indexes: Optional[sqlite3.Connection] = None
    # Object that defines the indexes available to do lookups based on
    # index_columns

    def __init_subclass__(cls):
        """This handles adding all the appropriate attributes and validates that
        a subclass has implemented the required fields.

        Additionally this automatically converts a subclass into a dataclass,
        to save on keeping track of an extra class decorator.
        """
        if cls.__name__ != "FileTableInMem":
            if not hasattr(cls, "schema"):
                raise NotImplementedError("Subclasses of FileTable must"
                                          "implement class attribute "
                                          "schema")
            if not hasattr(cls, "index_columns"):
                raise NotImplementedError("Subclasses of FileTable must"
                                          "implement class attribute "
                                          "index_columns")
            if not hasattr(cls, "builder"):
                raise NotImplementedError("Subclasses of FileTable must"
                                          "implement class attribute "
                                          "builder")
            cls.builder = BuilderDescriptor(cls.builder)
        cls = dataclass(cls, init=False)

    def __post_init__(self, do_index):
        """Fires after the automatically generated __init__ method so that
        additional processing can happen for dataclass initialization.
        """
        self.index_columns = tuple(self.index_columns)
        self.index_pos = {self.schema.field_pos[column]: column for column in
                          self.index_columns}
        self._file_handle = None
        self._mmap: Optional[mmap] = None

        self._open(do_index)

    def _open(self, do_index: bool):
        if self.filename is not None:
            self._file_handle = open(self.filename, 'rb')
            self._mmap = mmap(self._file_handle.fileno(), 0,
                              prot=PROT_READ)

            sidecar_path = f"{self.filename}.sidecar"
            if os.path.exists(sidecar_path):
                with open(sidecar_path, 'rb') as f:
                    self.indexes = pickle.load(f)
            elif do_index:
                self.indexes = IndexDict()
                start_loc = self._mmap.tell()
                for line in self:
                    for pos, column in self.index_pos.items():
                        self.indexes.insert((column, str(line[column])),
                                            start_loc)
                    start_loc = self._mmap.tell()
                with open(sidecar_path, 'w+b') as f:
                    pickle.dump(self.indexes, f)
                self._seek(0)

    def get_with_index(self, identifier: Tuple[ColumnName, Any]) ->\
            Union[List[Mapping[ColumnName, Any]], NoIndexError]:
        if self.indexes is None:
            raise AttributeError("Can only seek if an index is built")
        location = self.indexes.get(identifier, None)
        if location is None:
            return NoIndexError([{column: '\\N'
                                 for column in self.schema.fields.items()}])
        if self._mmap is None:
            raise AttributeError("file was never opened, was a filename"
                                 "supplied")
        results = []
        for loc in location:
            self._seek(loc)
            line = self._mmap.readline().decode().split(',')
            results.append(self._load_line(line))
        return results

    def _load_line(self, line: Iterable[str]) -> Mapping[ColumnName, Any]:
        d = {}
        for (name, columnType), item in zip(self.schema.fields.items(), line):
            if item == '\\N' or item == '\\N\n':
                value = '\\N'
            else:
                value = eval(columnType)(item)  # type: ignore
            d[ColumnName(name)] = value
        return d

    def _seek(self, value):
        if self._mmap is not None:
            self._mmap.seek(value)

    def __iter__(self):
        self._seek(0)
        generator = (row.decode().split(',')
                     for row in iter(self._mmap.readline, b""))
        # always skip the first row
        next(generator)
        generator = (self._load_line(r) for r in generator)
        return generator

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


class FileTableInMem(FileTable):
    def _open(self, _):
        if self.filename is not None:
            self.df: pd.DataFrame = pd.read_csv(self.filename)

    def get_with_index(self, identifier: Tuple[ColumnName, Any]) ->\
            Union[List[Mapping[ColumnName, Any]], NoIndexError]:
        result =\
            self.df.query(f"{identifier[0]} == {identifier[1]}").to_dict('r')
        if result:
            return result
        else:
            return NoIndexError({column: '\\N'
                                 for column in self.schema.fields.items()})

    def _load_line(self, line: Iterable[str]) -> Mapping[ColumnName, Any]:
        pass

    def _seek(self, _):
        pass

    def __iter__(self):
        return self.df.iterrows()
