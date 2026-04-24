"""Threadsafe cancel-token shared between the asyncio dispatcher and DB worker.

Per spec §5.1 / §5.3: when `token.set()` is called, registered callbacks fire
synchronously *from the dispatcher thread* — used to invoke `cursor.cancel()`
while the worker thread is blocked in `cursor.execute()` or
`cursor.fetchmany()`. snowflake-connector-python's `cursor.cancel()` is
documented as safe to call cross-thread.

**v1 scope.** `token.set()` is not called from any production path in v1 (see
Task 20's "Cancellation scope — v1" note). The primitive is kept + tested so
v1.1 can wire a `notifications/cancelled` handler to drive it. In v1, owned
calls are bounded by Snowflake's driver-level query timeout and by explicit
`cursor.cancel()` on row/byte-cap exceedance in `run_snowflake_query`.
"""
from __future__ import annotations

import logging
import threading
from typing import Callable

log = logging.getLogger(__name__)


class CancelToken:
    def __init__(self) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._callbacks: list[Callable[[], None]] = []

    def set(self) -> None:
        with self._lock:
            already = self._event.is_set()
            self._event.set()
            callbacks = list(self._callbacks)
            self._callbacks.clear()
        if already:
            return
        for cb in callbacks:
            try:
                cb()
            except Exception as e:
                log.warning("cancel_callback_failed", extra={"error": str(e)})

    def is_set(self) -> bool:
        return self._event.is_set()

    def register_cancel(self, cb: Callable[[], None]) -> None:
        """Register a cancel callback. Fires from `set()`'s thread.

        If the token is already set, fires the callback immediately on the
        registering thread.
        """
        with self._lock:
            if self._event.is_set():
                fire_now = True
            else:
                fire_now = False
                self._callbacks.append(cb)
        if fire_now:
            try:
                cb()
            except Exception as e:
                log.warning("cancel_callback_failed", extra={"error": str(e)})
