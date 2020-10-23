from typing import Iterable
import pickle
import zmq

from .base import Indexer
from .DiaSourceFileTable import DiaSourceFileTable


class ZMQ_indexer_server(Indexer):
    def __init__(self, do_index: bool, filename: str, columns: Iterable[str]):
        super().__init__(do_index, filename, columns)
        self.num_messages = 0

        context = zmq.Context()
        self.socket = context.socket(zmq.PULL)
        self.socket.bind("tcp://127.0.0.1:8391")

    def serve(self):
        while True:
            msg = pickle.loads(self.socket.recv())
            self.num_messages += len(msg)
            self.c.executemany(self.insert_command, msg)
            if self.num_messages % 100000 == 0:
                print(f"added {self.num_messages} total", end='\r')


def run_server(filename: str):
    zmq_server = ZMQ_indexer_server(True, filename,
                                    DiaSourceFileTable.index_columns)
    zmq_server.serve()
