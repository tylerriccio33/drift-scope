from __future__ import annotations

import contextlib
import copy
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import duckdb
import polars as pl
import psycopg2
import pytest
from testcontainers.postgres import PostgresContainer

from drift_scope._sql import SQL_CONNECTIONS, SQLConnections
from drift_scope.dataframe import DataFrameCompare
from drift_scope.results import FreqResults
from drift_scope.sql import SQLComparator

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from typing import Any, Literal

    from drift_scope.base import BaseComparator

    type GenReturn = Generator[Any, None, None]
    type ConYielder = Callable[[], GenReturn]


@contextlib.contextmanager
def _get_postgres() -> GenReturn:
    with PostgresContainer("postgres:16", driver=None) as postgres:
        psql_url = postgres.get_connection_url()
        with psycopg2.connect(psql_url) as connection:
            with connection.cursor() as cursor:
                yield cursor


@contextlib.contextmanager
def _get_duckdb() -> GenReturn:
    with duckdb.connect() as con:
        yield con


@dataclass
class _Args:  # TODO: Change the name of this
    con_type: SQL_CONNECTIONS | Literal["NARWHALS"]  # TODO : rename to engine
    comparator: BaseComparator
    yielder: Any = contextlib.nullcontext  # TODO: Need better name
    work_schema: str | None = None


args: tuple[_Args, ...] = (
    _Args(con_type="NARWHALS", comparator=DataFrameCompare),
    _Args(con_type="DUCKDB", comparator=SQLComparator, yielder=_get_duckdb),
    _Args(con_type="PSYCOPG2", comparator=SQLComparator, yielder=_get_postgres),
)

schema_opts = (None, "fooschema")
expanded_args: list[_Args] = []
for arg in args:
    if arg.con_type in SQL_CONNECTIONS:
        for schema in schema_opts:  # expand by schema
            args_copy: _Args = copy.copy(arg)
            args_copy.work_schema = schema
            expanded_args.append(args_copy)
        continue
    expanded_args.append(arg)


@pytest.mark.parametrize("arg", expanded_args)
def test_manual1(arg: _Args) -> None:
    """Test categorical comparison case."""
    with arg.yielder() as con:
        try:
            protocol = SQLConnections[arg.con_type].value
            is_sql: bool = True  # TODO: again, this is a terrible solution
        except KeyError:
            is_sql: bool = False  # type: ignore[no-redef]
            table1 = pl.DataFrame(
                {"city": ["New York", "Los Angeles", "Chicago"], "state": ["NY", "CA", "IL"]}
            )
            table2 = pl.DataFrame(
                {"city": ["New York", "Phoenix", "Philadelphia"], "state": ["NY", "AZ", "PA"]}
            )
            comp = arg.comparator(table1, table2)
        else:
            tables_at_start: tuple[str, ...] = protocol.get_tables(con)

            con.execute("BEGIN TRANSACTION;")

            if arg.work_schema:
                con.execute(f"CREATE SCHEMA {arg.work_schema}")

            con.execute("CREATE TABLE table1 (city VARCHAR, state VARCHAR)")
            con.execute(
                "INSERT INTO table1 VALUES ('New York', 'NY'), ('Los Angeles', 'CA'), ('Chicago', 'IL')"
            )

            con.execute("CREATE TABLE table2 (city VARCHAR, state VARCHAR)")
            con.execute(
                "INSERT INTO table2 VALUES ('New York', 'NY'), ('Phoenix', 'AZ'), ('Philadelphia', 'PA')"
            )

            con.execute("COMMIT;")  # ensure the tables exist before the comp

            comp = arg.comparator(
                df1="table1",
                df2="table2",
                con=con,
                con_type=arg.con_type,
                work_schema=arg.work_schema,
            )

        comp.comp_freq(vars=("city", "state"))

        if is_sql:
            ## Assert Integrity of Connection:
            tables: tuple[str, ...] = protocol.get_tables(con)
            msg = "Additional tables were persisted against the original connection."
            new_tables = set(tables) - set(tables_at_start)
            assert set(new_tables) == {"table1", "table2"}, msg

    ## Baseline Assertions:
    msg = "Results must be filled out"
    assert len(comp.results) == 1, msg

    ## Check Data Manually:
    comp_results: FreqResults = cast(FreqResults, comp.results[0])
    comp_data = comp_results.data
    res_pl = pl.DataFrame(pl.from_arrow(comp_data))  # TODO: Not a good solution

    msg = "n1 is incorrectly calculated"
    assert res_pl.filter(pl.col("city") == "New York").select("n1").item() == 1, msg
    assert res_pl.filter(pl.col("city") == "Los Angeles").select("n1").item() == 1, msg
    assert res_pl.filter(pl.col("city") == "Chicago").select("n1").item() == 1, msg

    msg = "n2 is incorrectly calculated"
    assert res_pl.filter(pl.col("city") == "New York").select("n2").item() == 1, msg
    assert res_pl.filter(pl.col("city") == "Phoenix").select("n2").item() == 1, msg
    assert res_pl.filter(pl.col("city") == "Philadelphia").select("n2").item() == 1, msg

    msg = "real_diff is incorrect"
    assert res_pl.filter(pl.col("city") == "New York").select("real_diff").item() == 0, msg
    assert res_pl.filter(pl.col("city") == "Los Angeles").select("real_diff").item() == -1, msg
    assert res_pl.filter(pl.col("city") == "Chicago").select("real_diff").item() == -1, msg
    assert res_pl.filter(pl.col("city") == "Phoenix").select("real_diff").item() == 1, msg
    assert res_pl.filter(pl.col("city") == "Philadelphia").select("real_diff").item() == 1, msg

    msg = "abs_diff is incorrect"
    assert res_pl.filter(pl.col("city") == "New York").select("abs_diff").item() == 0, msg
    assert res_pl.filter(pl.col("city") == "Los Angeles").select("abs_diff").item() == 1, msg
    assert res_pl.filter(pl.col("city") == "Chicago").select("abs_diff").item() == 1, msg
    assert res_pl.filter(pl.col("city") == "Phoenix").select("abs_diff").item() == 1, msg
    assert res_pl.filter(pl.col("city") == "Philadelphia").select("abs_diff").item() == 1, msg

    msg = "pct_diff is incorrect"
    assert res_pl.filter(pl.col("city") == "New York").select("pct_diff").item() == 0.0, msg
    assert res_pl.filter(pl.col("city") == "Los Angeles").select("pct_diff").item() == -1.0, msg
    assert res_pl.filter(pl.col("city") == "Chicago").select("pct_diff").item() == -1.0, msg
    assert res_pl.filter(pl.col("city") == "Phoenix").select("pct_diff").item() == 1.0, msg
    assert res_pl.filter(pl.col("city") == "Philadelphia").select("pct_diff").item() == 1.0, msg

    ## Check Report Compilation:
    comp.compile_report()  # prints reports


if __name__ == "__main__":
    for param in expanded_args:
        test_manual1(param)
