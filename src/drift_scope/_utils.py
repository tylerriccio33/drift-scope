from __future__ import annotations

from typing import TYPE_CHECKING

import narwhals as nw

if TYPE_CHECKING:
    from collections.abc import Iterable
    from typing import Any

    from narwhals.typing import IntoDataFrameT


def stringify_container(x: Iterable[str]) -> str:
    return ",".join(x)


def extract_rows(data: IntoDataFrameT) -> tuple[Any, ...]:
    native = nw.from_native(data)
    # TODO: If named were true, it could be better
    return tuple(native.iter_rows(named=False))
