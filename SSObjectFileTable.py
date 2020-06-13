from __future__ import annotations

__all__ = ("SSObjectFileTable",)

import csv
from dataclasses import dataclass
from glob import glob
from typing import (Optional, Iterable, Dict, Generator, List, Set,
                    Union)

from .base import (FileTable, FileTableInMem, FileTableBuilder, NoIndexError)
from .schemas import SSObject, DIASource, MPCORB
from .customTypes import ColumnName
from . import MPCORBFT, DiaSourceFT


@dataclass
class SSObjectRow:
    dia_list: List
    mpc_entry: Union[Dict, NoIndexError]


class SSObjectTuple(tuple):
    def decode(self):
        return self


class SSObjectBuilder(FileTableBuilder):
    input_schema = (DIASource, MPCORB)

    def __init__(self, parent: FileTable, input_dia_glob: str,
                 output_filename: str, input_mpc_filename: str,
                 skip_rows: int,
                 stop_after: Optional[int] = None,
                 columns: Optional[Iterable[ColumnName]] = None):
        self.parent = parent
        self.output_filename = output_filename
        self.skip_rows = skip_rows
        self.stop_after = stop_after
        self.columns = columns
        self.dia_files = []
        for name in glob(input_dia_glob):
            self.dia_files.append(DiaSourceFT(filename=name))
        self.mpc_file = MPCORBFT(filename=input_mpc_filename)

    def _get_objects_list_generator(self) -> Generator:
        objects_set: Set = set()
        for dia in self.dia_files:
            objects_set.update(dia.indexes)  # type: ignore
        return (SSObjectTuple(obj) for obj in objects_set)

    def _intrepret_row(self, object_id):
        rows = []
        for dia in self.dia_files:
            if object_id in dia.indexes:
                rows.append(dia.get_with_index(object_id))
        mpc_entry = self.mpc_file.get_with_index(object_id)
        return SSObjectRow(rows, mpc_entry)

    def run(self):
        with open(self.output_filename, 'w+') as out_file:
            writer = csv.writer(out_file, quoting=csv.QUOTE_NONE)
            writer.writerow(self.parent.schema.fields.keys())
            row_generator = self._get_objects_list_generator()
            rows = self._make_rows(row_generator)
            writer.writerows(rows)


class SSObjectFileTable(FileTableInMem):
    schema = SSObject
    index_columns = (ColumnName(x) for x in ("diaSourceId", "ssObjectId"))
    builder = SSObjectBuilder
