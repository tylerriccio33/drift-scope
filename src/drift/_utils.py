from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

def stringify_container(x : Iterable[str]) -> str:
    
    return ",".join(x)
