from __future__ import annotations

from drift.base import BaseComparator
from drift.results import FreqResults
from drift._utils import stringify_container

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any
    from collections.abc import Collection
    from drift.results import Results


class SQLComparator(BaseComparator):
    def __init__(self, df1: str, df2: str, con: Any, exec_attr: str = "sql") -> None:
        self.df1 = df1
        self.df2 = df2
        self.con = con
        self.exec_attr = exec_attr
        self.results: list[Results] = []

    def query(self, query: str) -> Any:
        return getattr(self.con, self.exec_attr)(query)

    def comp_freq(self, vars: Collection[str]) -> None:
        groupkey_stmt = stringify_container(vars)

        statement1 = f"SELECT {groupkey_stmt}, count(*) as n1 FROM {self.df1!s} GROUP BY {groupkey_stmt}"
        statement2 = f"SELECT {groupkey_stmt}, count(*) as n2 FROM {self.df2!s} GROUP BY {groupkey_stmt}"

        # TODO: This create statement will need to become paramaterized
        self.query(statement1).create("agg1")
        self.query(statement2).create("agg2")

        join_query: str = f"FROM agg1 FULL JOIN agg2 USING ({groupkey_stmt})"
        self.query(join_query).create("joined")

        ## Fill Nulls as 0s:
        fill_nulls_query: str = """--sql
        UPDATE joined
        SET n1 = COALESCE(n1, 0),
            n2 = COALESCE(n2, 0)
        """
        self.query(fill_nulls_query)

        ## Compute Diffs:
        diff_query: str = """--sql
        SELECT *,
            n2 - n1 AS real_diff,
            abs(n1 - n2) AS abs_diff,
            (n2 - n1) * 100.0 / NULLIF(n1 + n2, 0) / 100 AS pct_diff
        FROM joined
        """
        res = self.query(diff_query).arrow()

        ## Arrange Results:
        self.results.append(FreqResults(vars=vars, data=res))
