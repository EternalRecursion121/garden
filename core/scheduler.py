"""Cron loop. Reads `schedule = "..."` from each function's manifest entry.

Single in-process scheduler. On each tick: refresh the registry, recompute next
fire times, and submit any function whose fire time has passed to a shared
thread pool. Misfires (process down at fire time) are not replayed — agents
that need catch-up should track it themselves.

Concurrency model
-----------------
Fires run on a `ThreadPoolExecutor`. The scheduler thread itself only computes
fire times and submits; it never blocks on a function. Overlap is controlled
per-function via the manifest's `overlap` field:

  * "skip"     — if the previous fire is still running, this fire is dropped
                 (next_fire still advances, so we keep clock alignment). Default.
  * "parallel" — submit anyway; multiple copies may run concurrently.

Tick cadence
------------
Between ticks the scheduler sleeps until the earliest pending fire, capped at
`poll_interval` so we still pick up newly-added schedules.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Optional

from croniter import croniter

from .dispatcher import Dispatcher


class Scheduler:
    def __init__(
        self,
        dispatcher: Dispatcher,
        *,
        poll_interval: float = 30.0,
        max_workers: int = 8,
    ):
        self.dispatcher = dispatcher
        self.poll_interval = poll_interval
        self._next_fire: dict[str, float] = {}
        self._cron_for: dict[str, str] = {}
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="garden-sched"
        )
        # qualified → set of in-flight Futures. Read/written from both the
        # tick thread and worker-completion callbacks, so guard with a lock.
        self._inflight: dict[str, set[Future]] = {}
        self._inflight_lock = threading.Lock()

    def _refresh_schedule(self, now: float) -> None:
        live = dict(self.dispatcher.registry.all_scheduled())
        for qualified, cron in live.items():
            if (
                qualified not in self._next_fire
                or self._cron_for.get(qualified) != cron
            ):
                self._cron_for[qualified] = cron
                self._next_fire[qualified] = croniter(cron, now).get_next(float)
        for qualified in list(self._next_fire):
            if qualified not in live:
                self._next_fire.pop(qualified, None)
                self._cron_for.pop(qualified, None)

    def _is_inflight(self, qualified: str) -> bool:
        with self._inflight_lock:
            return bool(self._inflight.get(qualified))

    def _track(self, qualified: str, fut: Future) -> None:
        with self._inflight_lock:
            self._inflight.setdefault(qualified, set()).add(fut)

        def _done(f: Future) -> None:
            with self._inflight_lock:
                pool = self._inflight.get(qualified)
                if pool is not None:
                    pool.discard(f)
                    if not pool:
                        self._inflight.pop(qualified, None)
            exc = f.exception()
            if exc is not None:
                print(f"[scheduler] {qualified} failed: {exc}")

        fut.add_done_callback(_done)

    def tick(self, now: Optional[float] = None) -> None:
        now = now if now is not None else time.time()
        self.dispatcher.registry.refresh()
        self._refresh_schedule(now)
        for qualified, fire_at in list(self._next_fire.items()):
            if now < fire_at:
                continue
            cron = self._cron_for[qualified]
            self._next_fire[qualified] = croniter(cron, now).get_next(float)

            try:
                _, fn = self.dispatcher.registry.lookup(qualified)
            except (KeyError, ValueError):
                continue

            overlap = getattr(fn, "overlap", "skip")
            if overlap == "skip" and self._is_inflight(qualified):
                print(f"[scheduler] skip {qualified}: previous fire still running")
                continue

            print(f"[scheduler] firing {qualified}")
            fut = self._executor.submit(self.dispatcher.call, qualified, {})
            self._track(qualified, fut)

    def _sleep_for(self) -> float:
        if not self._next_fire:
            return self.poll_interval
        wait = min(self._next_fire.values()) - time.time()
        # Floor at a small positive value so a tight `now >= fire_at` check
        # doesn't busy-loop; cap so newly-added schedules still get noticed.
        return max(0.1, min(wait, self.poll_interval))

    def run(self, shutdown_event: Optional[threading.Event] = None) -> None:
        print(
            f"[scheduler] starting; poll<={self.poll_interval}s, "
            f"workers={self._executor._max_workers}"
        )
        try:
            while not (shutdown_event and shutdown_event.is_set()):
                self.tick()
                wait = self._sleep_for()
                if shutdown_event is not None:
                    shutdown_event.wait(timeout=wait)
                else:
                    time.sleep(wait)
        finally:
            self._executor.shutdown(wait=False, cancel_futures=True)
