from __future__ import annotations

__all__ = ("MPCORBDESIGMAP")

from ..base import TableSchema


class MPCORBDESIGMAP(TableSchema):
    mpcDesignation: str
    mpcNumber: int
    otherDesignation: str
    ssObjectId: int
