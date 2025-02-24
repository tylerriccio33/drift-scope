from __future__ import annotations

from typing import TYPE_CHECKING

from drift_scope._sql import SQLConnections
from drift_scope._utils import stringify_container
from drift_scope.base import BaseComparator
from drift_scope.results import FreqResults

if TYPE_CHECKING:
    from collections.abc import Collection
    from typing import Any

    from drift_scope._sql import _SQLConnectionProtocol
    from drift_scope.results import Results


class SQLComparator(BaseComparator):
    """Compare SQL Tables."""

    def __init__(self, df1: str, df2: str, con: Any, con_type: str) -> None:
        self.df1 = df1
        self.df2 = df2
        self.con = con
        self.results: list[Results] = []

        try:
            self.protocol: type[_SQLConnectionProtocol] = SQLConnections[con_type].value
        except KeyError as ke:
            raise NotImplementedError from ke

    def comp_freq(self, vars: Collection[str]) -> None:
        """Compare the frequency between two tables among variables `vars`."""
        groupkey_stmt = stringify_container(vars)

        statement1 = (
            f"SELECT {groupkey_stmt}, count(*) as n1 FROM {self.df1!s} GROUP BY {groupkey_stmt}"
        )
        statement2 = (
            f"SELECT {groupkey_stmt}, count(*) as n2 FROM {self.df2!s} GROUP BY {groupkey_stmt}"
        )

        self.protocol.create(self.con, statement1, "agg1")
        self.protocol.create(self.con, statement2, "agg2")

        join_query: str = f"SELECT * FROM agg1 FULL JOIN agg2 USING ({groupkey_stmt})"
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
            CAST(ABS(pct_diff) AS FLOAT) AS abs_pct_diff
        FROM (
            SELECT *,
                n2 - n1 AS real_diff,
                ABS(n1 - n2) AS abs_diff,
                CAST((n2 - n1) * 100.0 / NULLIF(n1 + n2, 0) / 100 AS FLOAT) AS pct_diff
            FROM joined
        ) subquery
        """
        cols = list(vars) + list(self.comp_freq_analysis_cols)
        res = self.protocol.materialize(self.con, diff_query, cols=cols)

        ## Arrange Results:
        self.results.append(
            FreqResults(vars=vars, analysis_cols=self.comp_freq_analysis_cols, data=res)
        )
