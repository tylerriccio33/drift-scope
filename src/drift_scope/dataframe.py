from __future__ import annotations

from typing import TYPE_CHECKING

import narwhals as nw

from drift_scope.base import BaseComparator
from drift_scope.results import FreqResults

if TYPE_CHECKING:
    from collections.abc import Collection

    from narwhals.typing import IntoFrame

    from drift_scope.results import Results


class DataFrameCompare(BaseComparator):
    """Compare dataframes per narwals dispatch methods."""

    def __init__(self, df1: IntoFrame, df2: IntoFrame) -> None:
        super().__init__()
        self.df1 = nw.from_native(df1)
        self.df2 = nw.from_native(df2)

        self.results: list[Results] = []

    def comp_freq(self, vars: Collection[str]) -> None:  # noqa: D102
        agg1 = self.df1.group_by(*vars).agg(n1=nw.len().cast(nw.Int64))
        agg2 = self.df2.group_by(*vars).agg(n2=nw.len().cast(nw.Int64))

        ## TODO: narwhals has not implemented full-join
        left_join = agg1.join(agg2, on=list(vars), how="left")
        right_only = (
            agg2.join(agg1, on=list(vars), how="left")
            .filter(nw.col("n1").is_null())
            .select(*vars, "n1", "n2")  # need to rearrange columns to vstack
        )

        concatted = (
            nw.concat([left_join, right_only], how="vertical")
            .with_columns(nw.col("n1", "n2").fill_null(0))
            .with_columns(real_diff=nw.col("n2") - nw.col("n1"))
            .with_columns(
                abs_diff=nw.col("real_diff").abs(),
                pct_diff=((nw.col("n2") - nw.col("n1")) * 100)
                / (nw.col("n1") + nw.col("n2"))
                / 100,
            )
            .with_columns(abs_pct_diff=nw.col("pct_diff").abs())
        )

        res = FreqResults(vars=vars, analysis_cols=self.comp_freq_analysis_cols, data=concatted)

        self.results.append(res)
