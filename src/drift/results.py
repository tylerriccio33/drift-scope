from __future__ import annotations

from typing import TYPE_CHECKING
import narwhals
from dataclasses import dataclass

if TYPE_CHECKING:
    from collections.abc import Collection


class Results:
    pass


@dataclass
class FreqResults(Results):
    vars: Collection[str]
    data: narwhals.DataFrame # type: ignore[type-arg]
