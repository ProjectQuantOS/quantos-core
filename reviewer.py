from typing import Any


class Reviewer:
    def review(self, task: Any, prompt: str = "") -> dict[str, Any]:
        return {"approved": True, "reason": "stub"}


def review(task: Any, prompt: str = "") -> dict[str, Any]:
    return {"approved": True, "reason": "stub"}
