"""Logging setup for scripts."""

from __future__ import annotations

import logging


def configure_logging(level: str = "INFO") -> None:
    """Configure a compact root logger."""

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def get_logger(name: str) -> logging.Logger:
    """Return a named logger."""

    return logging.getLogger(name)
