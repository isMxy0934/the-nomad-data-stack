"""Lightweight stub of freezegun.freeze_time for offline testing."""

from __future__ import annotations

import importlib
import unittest.mock as mock
from collections.abc import Callable, Iterator
from contextlib import ContextDecorator
from datetime import UTC, datetime


class _FrozenDateTime(datetime):
    _frozen: datetime

    @classmethod
    def with_value(cls, frozen: datetime) -> type:
        subclass = type("FrozenDateTime", (cls,), {})
        subclass._frozen = frozen
        return subclass

    @classmethod
    def now(cls, tz=None):  # noqa: ANN001
        base = cls._frozen
        if tz is None:
            return base
        return base.astimezone(tz)

    @classmethod
    def utcnow(cls):
        return cls._frozen.astimezone(UTC)


class freeze_time(ContextDecorator):
    def __init__(self, time_to_freeze: str) -> None:
        self.time_to_freeze = datetime.fromisoformat(time_to_freeze)
        if self.time_to_freeze.tzinfo is None:
            self.time_to_freeze = self.time_to_freeze.replace(tzinfo=UTC)
        self._patchers: list[Callable[[], Iterator[mock._patch]]] = []

    def __enter__(self):
        frozen_cls = _FrozenDateTime.with_value(self.time_to_freeze)
        target_module = importlib.import_module("lakehouse_core.io.time")
        patcher = mock.patch.object(target_module, "datetime", frozen_cls)
        self._patchers.append(patcher)
        patcher.start()
        return self

    def __exit__(self, *exc):  # noqa: ANN002, ANN003
        while self._patchers:
            patcher = self._patchers.pop()
            patcher.stop()
        return False

    def __call__(self, func: Callable):  # noqa: ANN001
        return super().__call__(func)
