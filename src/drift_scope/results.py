from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic

import narwhals as nw
from narwhals.typing import IntoFrameT
from rich.console import Console
from rich.table import Table

from drift_scope._utils import extract_rows

if TYPE_CHECKING:
    from collections.abc import Collection


class Results(ABC):
    """Results base class."""

    @abstractmethod
    def __init__(self) -> None:
        self.name: str

    @abstractmethod
    def report_as_table(self, *, abs_pct_diff_threshold: float = 0) -> None:
        """Report findings by printing a markdown table."""


@dataclass
class FreqResults(Results, Generic[IntoFrameT]):
    """Frequency results."""

    vars: Collection[str]
    analysis_cols: Collection[str]
    data: IntoFrameT
    name: str = "Categorical Frequency Results"

    def _filter_freq_threshold(self, abs_pct_diff_threshold: float) -> IntoFrameT:
        return (
            nw.from_native(self.data)
            .filter(nw.col("pct_diff").abs() >= nw.lit(abs_pct_diff_threshold))
            .to_native()
        )

    def report_as_table(self, abs_pct_diff_threshold: float = 0) -> None:
        """Report the findings as a console printed table.

        Args:
            abs_pct_diff_threshold (float, optional): The threshold to display absolute
            percent differences as red, symbolizing a problem. Defaults to 0.
        """
        filtered_data = self._filter_freq_threshold(abs_pct_diff_threshold)

        tab_title: str = f"Dimensions: {self.vars!s}"
        table = Table(title=tab_title, title_justify="left")

        for var in self.vars:
            table.add_column(style="magenta", header=var.title(), no_wrap=True)

        for var in self.analysis_cols:
            table.add_column(header=var.title(), no_wrap=True, justify="right")

        ## Condionally format rows:
        index_of_abs_pct_diff = len(self.vars) + list(self.analysis_cols).index("abs_pct_diff")

        for row in extract_rows(filtered_data):
            abs_pct_diff = row[index_of_abs_pct_diff]
            if abs(abs_pct_diff) == 0:
                style = "green"
            elif abs(abs_pct_diff) < 0.25:
                style = "yellow"
            else:
                style = "red"

            row_clean = tuple(str(elem) for elem in row)
            table.add_row(*row_clean, style=style)

        console = Console()
        console.print(table)
