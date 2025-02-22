from __future__ import annotations

from abc import ABC, abstractmethod

from typing import TYPE_CHECKING, Any
from typing import Type
from duckdb import DuckDBPyConnection

if TYPE_CHECKING:
    import pyarrow as pa


class _SQLConnectionProtocol(ABC):
    @staticmethod
    @abstractmethod
    def create(con: Any, query: str, table: str) -> None:
        """Create a connection. Must be implemented by subclasses."""

    @staticmethod
    @abstractmethod
    def exec(con: Any, query: str) -> None:
        """Execute a command or operation. Must be implemented by subclasses."""

    @staticmethod
    @abstractmethod
    def as_arrow(con: Any, query: str) -> pa.Table:
        """Return a query as arrow. Must be implemented by subclasses."""


class _DuckDBConnectionProtocol(_SQLConnectionProtocol):
    @staticmethod
    def create(con: DuckDBPyConnection, query: str, table: str) -> None:
        con.sql(query).create(table)

    @staticmethod
    def exec(con: DuckDBPyConnection, query: str) -> None:
        con.sql(query)

    @staticmethod
    def as_arrow(con: DuckDBPyConnection, query: str) -> pa.Table:
        return con.sql(query).arrow()


CONNECTIONS: dict[Any, Type[_SQLConnectionProtocol]] = {
    DuckDBPyConnection: _DuckDBConnectionProtocol
}

