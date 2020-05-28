from __future__ import annotations

__all__ = ("TableSchema", "NullValue",)

from abc import ABC
from dataclasses import dataclass, fields
from typing import Callable, Iterable, Dict, MutableMapping, ClassVar
from collections import defaultdict

from ..customTypes import ColumnName  # type: ignore


class NullMeta(type):
    """This metaclass controls the represenstation of the singleton
    "NullValue" class.
    """
    def __str__(self):
        """Returns the string null when str is called with NullValue
        as an argument.
        """
        return "null"


class NullValue(metaclass=NullMeta):
    """This is a singleton class to represent when a column is null.
    This saves memory and makes is comparisons possible.
    """
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
        #cls.registry = defaultdict(null_column_inserter)
        cls.registry = {}
        cls = dataclass(cls)
        cls._fields = {field.name: field.type for field in fields(cls)
                       if field not in fields(TableSchema)}
        cls._field_pos = {pos: field for pos, field in enumerate(cls._fields)}
        cls._pos_field = {field: pos for pos, field in enumerate(cls._fields)}

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

    def __iter__(self) -> Iterable[ColumnName]:
        return (getattr(self, field) for field in self._fields)  # type: ignore

    def __str__(self):
        return self._str.format(*(x for x in self))
