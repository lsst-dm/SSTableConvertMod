from __future__ import annotations

__all__ = ("SSObjectFileTable",)

import csv
from dataclasses import dataclass
from itertools import islice
import time
from typing import (Optional, Iterable, Dict, Generator, List, Set,
                    Union, Any)
import sqlite3

from .base import (FileTable, FileTableInMem, FileTableBuilder, NoIndexError)
from .schemas import SSObject, DIASource, MPCORB
from .customTypes import ColumnName


@dataclass
class SSObjectRow:
    ssobjectid: Any
    dia_list: List
    mpc_entry: Union[Dict, NoIndexError]


class SSObjectTuple(tuple):
    def decode(self):
        return self


class SSObjectKey(str):
    def decode(self):
        return self


class JointIndex:
    def __init__(self, dia_sidecar: str, mpc_sidecar: str):
        self.dia_db = sqlite3.connect(dia_sidecar)
        self.dia_cursor = self.dia_db.cursor()
        self.dia_cursor.execute("select * from ind limit 1") #<- this shit right here
        self.dia_schema = [description[0] for description in
                           self.dia_cursor.description]

        self.mpc_db = sqlite3.connect(mpc_sidecar) #<- this shit right here
        self.mpc_cursor = self.mpc_db.cursor()
        self.mpc_cursor.execute("select * from ind limit 1")
        self.mpc_schema = [description[0] for description in
                           self.mpc_cursor.description]
        self.count = 0
        self.start = time.time()

    def get_ssobject_keys(self) -> Generator[SSObjectKey, None, None]:
        seen: Set[str] = set()
        for entry in self.dia_db.execute('select ssObjectId from ind'):
            if entry[0] not in seen:
                seen.add(entry[0])
                yield SSObjectKey(entry)

    def build_SSObjectRow(self, key: SSObjectKey) -> SSObjectRow:
        print(f"building object {self.count} "
              f"{self.count/(time.time() - self.start)}", end='\r')
        self.count += 1
        dia_list = []
        key = key[2:-3]
        for entry in self.dia_db.execute('select * from ind where '
                                         'ssObjectId = ?', (key,)):
            dia_list.append({k: v for k, v in
                             zip(self.dia_schema, entry)})
        mpc_row = self.mpc_cursor.execute('select * from ind '
                                          'where ssObjectId = ?',
                                          (key,))
        try:
            mpc_entry = {k: v for k, v in
                         zip(self.mpc_schema, next(mpc_row))}
        except Exception as e:
            mpc_entry = NoIndexError
        return SSObjectRow(key, dia_list, mpc_entry)

    def __del__(self):
        self.dia_db.close()
        self.mpc_db.close()


class SSObjectBuilder(FileTableBuilder):
    input_schema = (DIASource, MPCORB)

    def __init__(self, parent: FileTable, input_dia_filename: str,
                 output_filename: str, input_mpc_filename: str,
                 skip_rows: int,
                 stop_after: Optional[int] = None,
                 columns: Optional[Iterable[ColumnName]] = None):
        self.parent = parent
        self.output_filename = output_filename
        self.skip_rows = skip_rows
        self.stop_after = stop_after
        self.columns = columns
        self.indexer = JointIndex(input_dia_filename, input_mpc_filename)

    def _get_objects_list_generator(self) -> Generator:
        return self.indexer.get_ssobject_keys()

    def _intrepret_row(self, object_id):
        return self.indexer.build_SSObjectRow(object_id)

    def run(self):
        with open(self.output_filename, 'w+', newline="") as out_file:
            writer = csv.writer(out_file, quoting=csv.QUOTE_NONE,
                                lineterminator="\n")
            writer.writerow(self.parent.schema.fields.keys())
            row_generator = self._get_objects_list_generator()
            rows = self._make_rows(islice(row_generator, self.skip_rows,
                                   self.stop_after))
            writer.writerows(rows)


class SSObjectFileTable(FileTableInMem):
    schema = SSObject
    index_columns = (ColumnName(x) for x in ("diaSourceId", "ssObjectId"))
    builder = SSObjectBuilder
