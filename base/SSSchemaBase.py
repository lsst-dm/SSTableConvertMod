from __future__ import annotations

__all__ = ("TableSchema", "NullValue", "schemaclass")

from abc import ABC
from dataclasses import dataclass, fields
from typing import Callable, Iterable, Dict, MutableMapping, ClassVar
from collections import defaultdict

from ..customTypes import ColumnName  # type: ignore


class NullMeta(type):
    def __str__(self):
        return "null"


class NullValue(metaclass=NullMeta):
    pass


def null_column(row: Iterable):
    return NullValue


def null_column_inserter():
    return null_column


@dataclass
class TableSchema(ABC):
    registry: ClassVar[MutableMapping[ColumnName, Callable]]

    def __init_subclass__(cls):
        super().__init_subclass__()
        cls.registry = defaultdict(null_column_inserter)
        #import pdb; pdb.set_trace()
        #cls._fields = {field.name: field.type for field in fields(cls)}

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
        for field in self._fields:
            yield getattr(self, field)


def schemaclass(klass):
    new_klass = dataclass(klass)
    new_klass._fields = {field.name: field.type for field in fields(klass)
                         if field not in fields(TableSchema)}
    return new_klass
