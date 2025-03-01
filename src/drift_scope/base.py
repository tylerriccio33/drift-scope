from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import rich

if TYPE_CHECKING:
    from collections.abc import Collection

    from drift_scope.results import Results


class BaseComparator(ABC):
    """Base class for comparing data."""

    @abstractmethod
    def __init__(self) -> None:
        self.results: list[Results]

    comp_freq_analysis_cols: tuple[str, ...] = (
        "n1",
        "n2",
        "real_diff",
        "abs_diff",
        "pct_diff",
        "abs_pct_diff",
    )

    @abstractmethod
    def comp_freq(self, vars: Collection[str]) -> None:
        """Compare frequencies of variables between datasets.

        Parameters
        ----------
            vars (Collection[str]): Variables to compare.

        Returns
        -------
            None. Appends results.

        Examples
        --------
        >>> import polars as pl
        >>> from drift_scope import DataFrameComparator
        >>> df1 = pl.DataFrame({'product': ['Apple', 'Banana', 'Orange']})
        >>> df2 = pl.DataFrame({'product': ['Apple', 'Banana', 'Orange', 'Grapes']})
        >>> comp = DataFrameComparator(df1, df2)
        >>> comp.comp_freq(vars=('product',))
        >>> comp_results = comp.results[0]
        >>> comp_data = comp_results.data.to_native()
        >>> expected = pl.DataFrame({
        ... 'product': ['Banana','Orange','Apple','Grapes'],
        ... 'n1': [1, 1, 1, 0],  # Count in basedf
        ... 'n2': [1, 1, 1, 1],  # Count in comparedf
        ... 'real_diff': [0, 0, 0, 1],
        ... 'abs_diff': [0, 0, 0, 1],
        ... 'abs_pct_diff': [0.0, 0.0, 0.0, 1.0],
        ... 'pct_diff': [0.0, 0.0, 0.0, 1.0]})
        >>> import polars.testing as pt
        >>> pt.assert_frame_equal(comp_data, expected,
        ... check_column_order=False,check_row_order=False)
        """

    def compile_report(self) -> None:
        """Compile results and print to the console."""
        console = rich.console.Console()

        for result in self.results:
            console.rule(result.name)
            # TODO: Add description
            console.print(result.report_as_table())
