from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Any

import pyarrow as pa

if TYPE_CHECKING:
    from collections.abc import Collection

    import psycopg2
    from duckdb import DuckDBPyConnection
    from narwhals.typing import IntoFrame


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
    def materialize(con: Any, query: str, cols: Collection[str]) -> IntoFrame:
        """Return a query as arrow. Must be implemented by subclasses."""


class _DuckDBConnectionProtocol(_SQLConnectionProtocol):
    @staticmethod
    def create(con: DuckDBPyConnection, query: str, table: str) -> None:
        con.sql(query).create(table)

    @staticmethod
    def exec(con: DuckDBPyConnection, query: str) -> None:
        con.sql(query)

    @staticmethod
    def materialize(con: DuckDBPyConnection, query: str, cols: Collection[str]) -> IntoFrame:
        return con.sql(query).arrow()


class _Psycopg2ConnectionProtocol(_SQLConnectionProtocol):
    @staticmethod
    def create(con: psycopg2.connect, query: str, table: str) -> None:
        # prefix the select query with a create
        create_stmt = f"CREATE TABLE {table} AS " + query
        con.execute(create_stmt)

    @staticmethod
    def exec(con: psycopg2.connect, query: str) -> None:
        con.execute(query)

    @staticmethod
    def materialize(
        con: psycopg2.extensions.cursor, query: str, cols: Collection[str]
    ) -> IntoFrame:
        con.execute(query)
        raw: tuple[Any, ...] = con.fetchall()
        data: dict[str, Any] = {col: [] for col in cols}
        for row in raw:
            for col, value in zip(cols, row, strict=False):
                data[col].append(value)
        return pa.Table.from_pydict(data)


class SQLConnections(Enum):
    PSYCOPG2 = _Psycopg2ConnectionProtocol
    DUCKDB = _DuckDBConnectionProtocol
