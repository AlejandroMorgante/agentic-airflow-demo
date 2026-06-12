from __future__ import annotations

from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

try:
    from strands import tool as strands_tool
except ImportError:

    def tool(func: F) -> F:
        return func

else:

    def tool(func: F) -> F:
        return strands_tool(func)
