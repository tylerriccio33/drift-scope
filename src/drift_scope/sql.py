from __future__ import annotations

import random
import string
from typing import TYPE_CHECKING

from drift_scope._sql import SQLConnections
from drift_scope._utils import stringify_container
from drift_scope.base import BaseComparator
from drift_scope.results import FreqResults

if TYPE_CHECKING:
    from collections.abc import Collection
    from typing import Any

    from narwhals.typing import IntoFrame

    from drift_scope._sql import SQL_CONNECTIONS, _SQLConnectionProtocol
    from drift_scope.results import Results


class SQLComparator(BaseComparator):
    """Compare SQL Tables."""

    def __init__(
        self,
        df1: str,
        df2: str,
        con: Any,
        con_type: SQL_CONNECTIONS,
        work_schema: str | None = None,
    ) -> None:
        super().__init__()
        self.df1 = df1
        self.df2 = df2
        self.con = con
        self.work_schema = work_schema
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

        interim_schema_ref = ""
        if self.work_schema:
            interim_schema_ref = self.work_schema + "."

        agg1_table: str = interim_schema_ref + "".join(random.choices(string.ascii_lowercase, k=8))
        agg2_table = interim_schema_ref + "".join(random.choices(string.ascii_lowercase, k=8))

        self.protocol.exec(self.con, "BEGIN TRANSACTION")
        self.protocol.create(self.con, statement1, f"{agg1_table}")
        self.protocol.create(self.con, statement2, f"{agg2_table}")

        join_query: str = (
            f"SELECT * FROM {agg1_table} FULL JOIN {agg2_table} USING ({groupkey_stmt})"
        )
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
        res: IntoFrame = self.protocol.materialize(self.con, diff_query, cols=cols)

        ## Rollback post-materialization:
        ## Rolling back isn't necessary for some engines that already
        ## require an explicit commit in the first place.
        self.protocol.exec(self.con, "ROLLBACK")

        ## Arrange Results:
        self.results.append(
            FreqResults(vars=vars, analysis_cols=self.comp_freq_analysis_cols, data=res)
        )
