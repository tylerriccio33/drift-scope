from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Collection


class BaseComparator(ABC):
    """Base class for comparing data."""

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
