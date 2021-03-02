__all__ = ("DiaSourceFileTable",)

from .base import FileTable, FileTableBuilder
from .schemas import DIASource
from .customTypes import ColumnName

from typing import Generator, Iterable
import pickle
import zmq


class ZMQ_Indexer:
    def __init__(self, do_index: bool, _: str, _columns: Iterable[str]):
        self.do_index = do_index

        if do_index:
            self.accumulate_len = 5000
            self.tracker_len = 0
            self.tracker = [None]*self.accumulate_len
            context = zmq.Context()
            try:
                self.socket = context.socket(zmq.PUSH)
                self.socket.connect("tcp://127.0.0.1:8391")
            except Exception:
                raise Exception("cound no connect to server, did you start "
                                "index process")

    def insert(self, generator) -> Generator:
        if self.do_index:
            values = tuple(generator)
            self.tracker[self.tracker_len] = tuple(v for v, truth in
                                                   values if truth)
            self.tracker_len += 1
            if self.tracker_len == self.accumulate_len:
                self.socket.send(pickle.dumps(self.tracker))
                self.tracker_len = 0
            yield from (value for value, _ in values)
        else:
            yield from (value for value, _ in generator)


class DiaSourceBuilder(FileTableBuilder):
    input_schema = ("ObjID", "observationId", "FieldMJD", "AstRange(km)",
                    "AstRangeRate(km/s)", "AstRA(deg)", "AstRARate(deg/day)",
                    "AstDec(deg)", "AstDecRate(deg/day)",
                    "Ast-Sun(J2000x)(km)", "Ast-Sun(J2000y)(km)",
                    "Ast-Sun(J2000z)(km)", "Sun-Ast-Obs(deg)",
                    "V", "Filtermag", "V(H=0)", "Filter", "AstRASigma(mas)",
                    "AstDecSigma(mas)", "PhotometricSigma(mag)")
    INDEXER = ZMQ_Indexer


class DiaSourceFileTable(FileTable):
    schema = DIASource
    index_columns = tuple(ColumnName(x) for x in ("ssObjectId", "midPointTai","mag","filter","phaseAngle","magSigma"))
    builder = DiaSourceBuilder
