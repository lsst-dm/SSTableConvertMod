from __future__ import annotations

__all__ = ("MPCORBDESIGMAP")

from dataclasses import dataclass

from ..base import TableSchema, schemaclass


@schemaclass
class MPCORBDESIGMAP(TableSchema):
    mpcDesignation: str
    mpcNumber: int
    otherDesignation: str
    ssObjectId: int
