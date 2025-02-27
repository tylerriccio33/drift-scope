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
        """Compare frequency between tables among variables `vars`."""

    def compile_report(self) -> None:
        """Compile results and print to the console."""
        console = rich.console.Console()

        for result in self.results:
            console.rule(result.name)
            # TODO: Add description
            console.print(result.report_as_table())
