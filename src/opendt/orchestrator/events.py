"""Lightweight observer helpers for orchestrator events."""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable, DefaultDict, List


class EventBus:
    def __init__(self) -> None:
        self._subscribers: DefaultDict[str, List[Callable[[Any], None]]] = defaultdict(list)

    def subscribe(self, event: str, handler: Callable[[Any], None]) -> None:
        self._subscribers[event].append(handler)

    def publish(self, event: str, payload: Any) -> None:
        for handler in self._subscribers.get(event, []):
            handler(payload)
