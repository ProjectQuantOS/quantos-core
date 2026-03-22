from typing import Any, List, Optional


def arbitrate(candidates: Optional[List[Any]] = None) -> Any:
    if not candidates:
        return None
    return candidates[0]
