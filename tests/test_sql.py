from __future__ import annotations
import duckdb
from ddrift.sql import SQLComparator
import polars as pl


def test_manual1() -> None:
    with duckdb.connect() as con:
        con.execute("CREATE TABLE table1 (city VARCHAR, state VARCHAR)")
        con.execute(
            "INSERT INTO table1 VALUES ('New York', 'NY'), ('Los Angeles', 'CA'), ('Chicago', 'IL')"
        )

        con.execute("CREATE TABLE table2 (city VARCHAR, state VARCHAR)")
        con.execute(
            "INSERT INTO table2 VALUES ('New York', 'NY'), ('Phoenix', 'AZ'), ('Philadelphia', 'PA')"
        )

        sql = SQLComparator(df1="table1", df2="table2", con=con)
        sql.comp_freq(vars=("city", "state"))

        ## Baseline Assertions:
        msg = "Results must be filled out"
        assert len(sql.results) == 1, msg

        ## Check Data Manually:
        res = sql.results[0].data
        res_pl = pl.DataFrame(pl.from_arrow(res))

        msg = "n1 is incorrectly calculated"
        assert res_pl.filter(pl.col("city") == "New York").select("n1").item() == 1, msg
        assert (
            res_pl.filter(pl.col("city") == "Los Angeles").select("n1").item() == 1
        ), msg
        assert res_pl.filter(pl.col("city") == "Chicago").select("n1").item() == 1, msg

        msg = "n2 is incorrectly calculated"
        assert res_pl.filter(pl.col("city") == "New York").select("n2").item() == 1, msg
        assert res_pl.filter(pl.col("city") == "Phoenix").select("n2").item() == 1, msg
        assert (
            res_pl.filter(pl.col("city") == "Philadelphia").select("n2").item() == 1
        ), msg

        msg = "real_diff is incorrect"
        assert (
            res_pl.filter(pl.col("city") == "New York").select("real_diff").item() == 0
        ), msg
        assert (
            res_pl.filter(pl.col("city") == "Los Angeles").select("real_diff").item()
            == -1
        ), msg
        assert (
            res_pl.filter(pl.col("city") == "Chicago").select("real_diff").item() == -1
        ), msg
        assert (
            res_pl.filter(pl.col("city") == "Phoenix").select("real_diff").item() == 1
        ), msg
        assert (
            res_pl.filter(pl.col("city") == "Philadelphia").select("real_diff").item()
            == 1
        ), msg

        msg = "abs_diff is incorrect"
        assert (
            res_pl.filter(pl.col("city") == "New York").select("abs_diff").item() == 0
        ), msg
        assert (
            res_pl.filter(pl.col("city") == "Los Angeles").select("abs_diff").item()
            == 1
        ), msg
        assert (
            res_pl.filter(pl.col("city") == "Chicago").select("abs_diff").item() == 1
        ), msg
        assert (
            res_pl.filter(pl.col("city") == "Phoenix").select("abs_diff").item() == 1
        ), msg
        assert (
            res_pl.filter(pl.col("city") == "Philadelphia").select("abs_diff").item()
            == 1
        ), msg

        msg = "pct_diff is incorrect"
        assert (
            res_pl.filter(pl.col("city") == "New York").select("pct_diff").item() == 0.0
        ), msg
        assert (
            res_pl.filter(pl.col("city") == "Los Angeles").select("pct_diff").item()
            == -1.0
        ), msg
        assert (
            res_pl.filter(pl.col("city") == "Chicago").select("pct_diff").item() == -1.0
        ), msg
        assert (
            res_pl.filter(pl.col("city") == "Phoenix").select("pct_diff").item() == 1.0
        ), msg
        assert (
            res_pl.filter(pl.col("city") == "Philadelphia").select("pct_diff").item()
            == 1.0
        ), msg

if __name__ == "__main__":
    test_manual1()
