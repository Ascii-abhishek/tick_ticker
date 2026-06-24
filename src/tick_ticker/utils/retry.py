"""Small retry helper for network calls."""

from __future__ import annotations

import time
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


def retry(*, attempts: int, base_delay_seconds: float) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Retry a callable with linear backoff."""

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_exc: Exception | None = None
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    if attempt == attempts:
                        break
                    time.sleep(base_delay_seconds * attempt)
            assert last_exc is not None
            raise last_exc

        return wrapper

    return decorator
