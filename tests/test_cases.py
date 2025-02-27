from __future__ import annotations

import contextlib
import itertools
from typing import TYPE_CHECKING, NamedTuple, cast

import duckdb
import polars as pl
import psycopg2
import pytest
from testcontainers.postgres import PostgresContainer

from drift_scope._sql import SQLConnections
from drift_scope.results import FreqResults
from drift_scope.sql import SQLComparator

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from typing import Any

    from drift_scope._sql import SQL_CONNECTIONS

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


class _Args(NamedTuple):  # TODO: Change the name of this
    con_type: SQL_CONNECTIONS
    yielder: Any
    work_schema: str | None = None


args: tuple[_Args, ...] = (
    _Args(con_type="DUCKDB", yielder=_get_duckdb),
    _Args(con_type="PSYCOPG2", yielder=_get_postgres),
)

schema_opts = (None, "fooschema")
expanded_args = tuple(
    _Args(con_type=arg.con_type, yielder=arg.yielder, work_schema=schema)
    for arg, schema in itertools.product(args, schema_opts)
)


@pytest.mark.parametrize("arg", expanded_args)
def test_manual1(arg: _Args) -> None:
    """Test categorical comparison case."""
    with arg.yielder() as con:
        protocol = SQLConnections[arg.con_type].value

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

        comp = SQLComparator(
            df1="table1", df2="table2", con=con, con_type=arg.con_type, work_schema=arg.work_schema
        )
        comp.comp_freq(vars=("city", "state"))

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
