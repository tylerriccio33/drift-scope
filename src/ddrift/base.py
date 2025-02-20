from __future__ import annotations

from abc import ABC, abstractmethod

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Collection


class BaseComparator(ABC):
    """Base class for comparing data."""

    @abstractmethod
    def comp_freq(self, vars: Collection[str]) -> None:
        pass
