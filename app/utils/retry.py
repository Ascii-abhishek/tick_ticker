"""Retry helpers."""

from __future__ import annotations

import functools
import time
from collections.abc import Callable
from typing import ParamSpec, TypeVar

from app.utils.logger import get_logger

P = ParamSpec("P")
R = TypeVar("R")

logger = get_logger(__name__)


def retry(
    *,
    attempts: int = 3,
    base_delay_seconds: float = 1.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Retry a function with exponential backoff."""

    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            last_error: Exception | None = None
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_error = exc
                    if attempt >= attempts:
                        break
                    delay = base_delay_seconds * (2 ** (attempt - 1))
                    logger.warning(
                        "retrying_after_error",
                        extra={
                            "function": func.__name__,
                            "attempt": attempt,
                            "max_attempts": attempts,
                            "delay_seconds": delay,
                            "error": str(exc),
                        },
                    )
                    time.sleep(delay)
            assert last_error is not None
            raise last_error

        return wrapper

    return decorator

