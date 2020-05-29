from __future__ import annotations

__all__ = ("TableSchema",)

from abc import ABC
from dataclasses import dataclass, fields
from typing import Callable, Iterable, Dict, MutableMapping, ClassVar

from ..customTypes import ColumnName  # type: ignore


@dataclass
class TableSchema(ABC):
    """Base class for all FileTable schemas. This class is not intended to be
    initialized itself, so its base class is an abstract base class.

    This class adds a registry and various helper attributes to subclasses, as
    well as automatically converting subclasses into dataclasses.

    Each subclass will define, using class attributes, the schema representing
    a FileTable. The types associated with the attributes will be used to
    convert the string based representation on disk into python objects.

    Once created each subclass has a register method that can be used as a
    funciton decorator to register a function to handle a particular column
    defined in the schema during the conversion process. The handler function
    must be a callable that takes one argument, a mapping of input file schema
    to input value. See SSTableConvertMod.schemas.columnConversion for
    examples.

    Any module where this decorator is used must be imported before a
    sublcass is to be used, otherwise the registration process will not happen.
    The easiest way to ensure this will happen is to put new handler functions
    inside SSTableConvertMod.schemas.columnConversion, or in a file that will
    be imported inside SSTableConvertMod.schemas.__init__. 
    """
    registry: ClassVar[MutableMapping[ColumnName, Callable]]

    def __init_subclass__(cls):
        super().__init_subclass__()
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
        subset: Dict[ColumnName, Callable] = {}
        subset.update({column: cls.registry[column] for column in columns})
        return subset

    def __iter__(self) -> Iterable[ColumnName]:
        return (getattr(self, field) for field in self._fields)  # type: ignore

    def __str__(self):
        return self._str.format(*(x for x in self))
