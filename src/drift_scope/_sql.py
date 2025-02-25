from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa

if TYPE_CHECKING:
    from collections.abc import Collection

    import psycopg2
    from duckdb import DuckDBPyConnection
    from narwhals.typing import IntoFrame


class _SQLConnectionProtocol(ABC):
    @classmethod
    @abstractmethod
    def get_tables(cls, con: Any) -> tuple[str, ...]:
        """Retrieve list of tables in the connection."""

    @classmethod
    @abstractmethod
    def create(cls, con: Any, query: str, table: str) -> None:
        """Create a connection. Must be implemented by subclasses."""

    @staticmethod
    @abstractmethod
    def exec(con: Any, query: str) -> None:
        """Execute a command or operation. Must be implemented by subclasses."""

    @classmethod
    @abstractmethod
    def materialize(cls, con: Any, query: str, cols: Collection[str]) -> IntoFrame:
        """Return a query as arrow. Must be implemented by subclasses."""


class _DuckDBConnectionProtocol(_SQLConnectionProtocol):
    @classmethod
    def get_tables(cls, con: DuckDBPyConnection) -> tuple[Any, ...]:
        table_data: pa.Table = cls.materialize(con, "SHOW TABLES", "name")
        return tuple(table_data["name"].to_pylist())

    @classmethod
    def create(cls, con: DuckDBPyConnection, query: str, table: str) -> None:
        con.sql(query).create(table)

    @staticmethod
    def exec(con: DuckDBPyConnection, query: str) -> None:
        con.sql(query)

    @classmethod
    def materialize(cls, con: DuckDBPyConnection, query: str, cols: Collection[str]) -> pa.Table:
        return con.sql(query).arrow()


class _Psycopg2ConnectionProtocol(_SQLConnectionProtocol):
    @classmethod
    def get_tables(cls, con: psycopg2.connect) -> tuple[Any, ...]:
        table_data: pa.Table = cls.materialize(
            con, "SELECT table_name FROM information_schema.tables", ("table_name",)
        )
        return tuple(table_data["table_name"].to_pylist())

    @classmethod
    def create(cls, con: psycopg2.connect, query: str, table: str) -> None:
        # prefix the select query with a create
        create_stmt = f"CREATE TABLE {table} AS " + query
        con.execute(create_stmt)

    @staticmethod
    def exec(con: psycopg2.connect, query: str) -> None:
        con.execute(query)

    @classmethod
    def materialize(
        cls, con: psycopg2.extensions.cursor, query: str, cols: Collection[str]
    ) -> pa.Table:
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


SQL_CONNECTIONS = Literal["PSYCOPG2", "DUCKDB"]
