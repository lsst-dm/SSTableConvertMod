from __future__ import annotations

__all__ = ("TableSchema", "NullValue",)

from abc import ABC
from dataclasses import dataclass, fields
from typing import Callable, Iterable, Dict, MutableMapping, ClassVar
from collections import defaultdict

from ..customTypes import ColumnName  # type: ignore


class NullMeta(type):
    def __str__(self):
        return "null"

    def __bytes__(self):
        return b"null"


class NullValue(metaclass=NullMeta):
    pass


def null_column(row: Iterable):
    return b"null"


def null_column_inserter():
    return null_column


@dataclass
class TableSchema(ABC):
    registry: ClassVar[MutableMapping[ColumnName, Callable]]

    def __init_subclass__(cls):
        super().__init_subclass__()
        cls.registry = defaultdict(null_column_inserter)
        cls = dataclass(cls)
        cls._fields = {field.name: field.type for field in fields(cls)
                       if field not in fields(TableSchema)}
        cls._field_pos = {pos: field for pos, field in enumerate(cls._fields)}
        cls._str = "{},"*len(cls._fields)
        cls._str = cls._str[:-1] + '\n'

    @classmethod
    def register(cls, column_name: ColumnName) ->\
            Callable[[Callable], Callable]:
        if column_name not in cls._fields:  # type: ignore
            raise AttributeError(f"No column named {column_name} in {cls}")

        def inner(function: Callable) -> Callable:
            cls.registry[column_name] = function
            return function
        return inner

    @classmethod
    def registry_subset(cls, columns: Iterable[ColumnName]) -> Dict:
        subset: Dict[ColumnName, Callable] = defaultdict(null_column_inserter)
        subset.update({column: cls.registry[column] for column in columns})
        return subset

    def __iter__(self):
        return (getattr(self, field) for field in self._fields)

    def __str__(self):
        return self._str.format(*(x for x in self))
