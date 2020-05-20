from __future__ import annotations

__all__ = ("TableSchema", "NullValue")

from abc import ABC
from dataclasses import dataclass, fields
from typing import Callable, Sequence, Dict
from collections import defaultdict

from .customTypes import ColumnName


class NullMeta(type):
    def __str__(self):
        return "null"


class NullValue(metaclass=NullMeta):
    pass


def null_column(row: Sequence):
    return NullValue


def null_column_inserter():
    return null_column


@dataclass
class TableSchema(ABC):
    def __init_subclass__(cls):
        cls.registry = defaultdict(null_column_inserter)
        cls._fields = {field.name: field.type for field in fields(cls)}
        return super().__init_subclass__()

    @classmethod
    def register(cls, column_name: ColumnName) ->\
            Callable[[Callable], Callable]:
        if column_name not in cls._fields:
            raise AttributeError(f"No column named {column_name} in {cls}")

        def inner(function: Callable) -> Callable:
            cls.registry[column_name] = function
            return function
        return inner

    @classmethod
    def registry_subset(cls, columns: Sequence[ColumnName]) -> Dict:
        subset = defaultdict(null_column_inserter)
        subset.update({column: cls.registry[column] for column in columns})
        return subset

    def __iter__(self):
        for field in self._fields:
            yield getattr(self, field)
