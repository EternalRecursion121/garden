"""Cron loop. Reads `schedule = "..."` from each function's manifest entry.

Single in-process scheduler. On each tick: refresh the registry, recompute next
fire times, and call any function whose fire time has passed. Misfires (process
down at fire time) are not replayed — agents that need catch-up should track it
themselves.
"""

from __future__ import annotations

import time
from typing import Optional

from croniter import croniter

from .dispatcher import Dispatcher


class Scheduler:
    def __init__(self, dispatcher: Dispatcher, *, poll_interval: float = 30.0):
        self.dispatcher = dispatcher
        self.poll_interval = poll_interval
        self._next_fire: dict[str, float] = {}
        self._cron_for: dict[str, str] = {}

    def _refresh_schedule(self, now: float) -> None:
        live = dict(self.dispatcher.registry.all_scheduled())
        # add new
        for qualified, cron in live.items():
            if (
                qualified not in self._next_fire
                or self._cron_for.get(qualified) != cron
            ):
                self._cron_for[qualified] = cron
                self._next_fire[qualified] = croniter(cron, now).get_next(float)
        # drop removed
        for qualified in list(self._next_fire):
            if qualified not in live:
                self._next_fire.pop(qualified, None)
                self._cron_for.pop(qualified, None)

    def tick(self, now: Optional[float] = None) -> None:
        now = now if now is not None else time.time()
        self.dispatcher.registry.refresh()
        self._refresh_schedule(now)
        for qualified, fire_at in list(self._next_fire.items()):
            if now >= fire_at:
                print(f"[scheduler] firing {qualified}")
                try:
                    self.dispatcher.call(qualified, params={})
                except Exception as e:
                    print(f"[scheduler] {qualified} failed: {e}")
                cron = self._cron_for[qualified]
                self._next_fire[qualified] = croniter(cron, now).get_next(float)

    def run(self) -> None:
        print(f"[scheduler] starting; polling every {self.poll_interval}s")
        while True:
            self.tick()
            time.sleep(self.poll_interval)
