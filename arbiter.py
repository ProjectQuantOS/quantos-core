from typing import Any


def arbitrate(candidates: list[Any] | None = None) -> Any:
    if not candidates:
        return None
    return candidates[0]
