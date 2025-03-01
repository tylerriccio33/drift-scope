`ddrift` analyzes the differences between data sources, i.e. how much has dataset a drifted from dataset b. The framework is engine agnostic. Each  engine is required to comply with simple abstract protocols in order to enable the standard reporting.

Engines Supported:
- DuckDB
- Postgres

Engines to be Supported:
- Narwhals dispatches

Install with `pip install ddrift` or (prefferebly) `uv pip install ddrift`

## Usage

Let's create 2 simple tables and compare them to one another. The fundamental question we're asking is "How much has table2 drifted from table1?"

```python
>>> import duckdb

... with duckdb.connect() as con:
...     con.execute("CREATE TABLE table1 (city VARCHAR, state VARCHAR)")
...     con.execute("INSERT INTO table1 VALUES ('New York', 'NY'), ('Los Angeles', 'CA'), ('Chicago', 'IL')")

...     con.execute("CREATE TABLE table2 (city VARCHAR, state VARCHAR)")
...     con.execute("INSERT INTO table2 VALUES ('New York', 'NY'), ('Phoenix', 'AZ'), ('Philadelphia', 'PA')")

...     sql = SQLComparator(df1="table1", df2="table2", con=con)
...     sql.comp_freq(vars=("city", "state"))

...     comp.compile_report()  # prints reports to console

...     res_pl = pl.from_arrow(sql.data) # pull out the comparison summary

... msg = "n1 is incorrectly calculated"
... assert res_pl.filter(pl.col("city") == "New York").select("n1").item() == 1, msg
... assert res_pl.filter(pl.col("city") == "Los Angeles").select("n1").item() == 1, msg
... assert res_pl.filter(pl.col("city") == "Chicago").select("n1").item() == 1, msg

... msg = "n2 is incorrectly calculated"
... assert res_pl.filter(pl.col("city") == "New York").select("n2").item() == 1, msg
... assert res_pl.filter(pl.col("city") == "Phoenix").select("n2").item() == 1, msg
... assert res_pl.filter(pl.col("city") == "Philadelphia").select("n2").item() == 1, msg

... msg = "real_diff is incorrect"
... assert res_pl.filter(pl.col("city") == "New York").select("real_diff").item() == 0, msg
... assert res_pl.filter(pl.col("city") == "Los Angeles").select("real_diff").item() == -1, msg
... assert res_pl.filter(pl.col("city") == "Chicago").select("real_diff").item() == -1, msg
... assert res_pl.filter(pl.col("city") == "Phoenix").select("real_diff").item() == 1, msg
... assert res_pl.filter(pl.col("city") == "Philadelphia").select("real_diff").item() == 1, msg

... msg = "abs_diff is incorrect"
... assert res_pl.filter(pl.col("city") == "New York").select("abs_diff").item() == 0, msg
... assert res_pl.filter(pl.col("city") == "Los Angeles").select("abs_diff").item() == 1, msg
... assert res_pl.filter(pl.col("city") == "Chicago").select("abs_diff").item() == 1, msg
... assert res_pl.filter(pl.col("city") == "Phoenix").select("abs_diff").item() == 1, msg
... assert res_pl.filter(pl.col("city") == "Philadelphia").select("abs_diff").item() == 1, msg

... msg = "pct_diff is incorrect"
... assert res_pl.filter(pl.col("city") == "New York").select("pct_diff").item() == 0.0, msg
... assert res_pl.filter(pl.col("city") == "Los Angeles").select("pct_diff").item() == -1.0, msg
... assert res_pl.filter(pl.col("city") == "Chicago").select("pct_diff").item() == -1.0, msg
... assert res_pl.filter(pl.col("city") == "Phoenix").select("pct_diff").item() == 1.0, msg
... assert res_pl.filter(pl.col("city") == "Philadelphia").select("pct_diff").item() == 1.0, msg
```