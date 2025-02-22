from __future__ import annotations

from drift_scope.base import BaseComparator
from drift_scope.results import FreqResults
from drift_scope._utils import stringify_container
from drift_scope._sql import CONNECTIONS

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Collection
    from drift_scope.results import Results
    from drift_scope._sql import _SQLConnectionProtocol
    from typing import Any, Type


class SQLComparator(BaseComparator):
    def __init__(self, df1: str, df2: str, con: Any) -> None:
        self.df1 = df1
        self.df2 = df2
        self.con = con
        self.results: list[Results] = []

        con_type = type(con)
        try:
            self.protocol: Type[_SQLConnectionProtocol] = CONNECTIONS[con_type]
        except KeyError:
            raise NotImplementedError

    def comp_freq(self, vars: Collection[str]) -> None:
        groupkey_stmt = stringify_container(vars)

        statement1 = f"SELECT {groupkey_stmt}, count(*) as n1 FROM {self.df1!s} GROUP BY {groupkey_stmt}"
        statement2 = f"SELECT {groupkey_stmt}, count(*) as n2 FROM {self.df2!s} GROUP BY {groupkey_stmt}"

        self.protocol.create(self.con, statement1, "agg1")
        self.protocol.create(self.con, statement2, "agg2")

        join_query: str = f"FROM agg1 FULL JOIN agg2 USING ({groupkey_stmt})"
        self.protocol.create(self.con, join_query, "joined")

        ## Fill Nulls as 0s:
        fill_nulls_query: str = """--sql
        UPDATE joined
        SET n1 = COALESCE(n1, 0),
            n2 = COALESCE(n2, 0)
        """
        self.protocol.exec(self.con, fill_nulls_query)

        ## Compute Diffs:
        diff_query: str = """--sql
        SELECT *,
            n2 - n1 AS real_diff,
            abs(n1 - n2) AS abs_diff,
            (n2 - n1) * 100.0 / NULLIF(n1 + n2, 0) / 100 AS pct_diff
        FROM joined
        """
        res = self.protocol.as_arrow(self.con, diff_query)

        ## Arrange Results:
        self.results.append(FreqResults(vars=vars, data=res))
