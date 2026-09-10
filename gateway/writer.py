"""Group-commit writer thread.

Many collector threads call :meth:`WriteWorker.submit`; a single writer thread
batches pending frames and performs one ``write() + fsync()`` per group. A
submit() returns only AFTER its group's fsync has completed (that is the sole
definition of "persisted").

Flash quota is enforced before queueing (reservation model): producers block
inside submit() when live bytes + reserved bytes reach the configured quota.
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
from dataclasses import dataclass

from .config import Config
from .models import ShutdownError
from .segments import SEQ_OVERHEAD, Store

log = logging.getLogger("gateway.writer")

_SENTINEL = object()


@dataclass
class _Item:
    reserved: int
    ts: float
    raw_payload: bytes
    event: threading.Event
    seq_out: int = 0
    error: BaseException | None = None


class WriteWorker(threading.Thread):
    def __init__(self, cfg: Config, store: Store, clock=time,
                 group_event: threading.Event | None = None) -> None:
        super().__init__(name="gateway-writer", daemon=True)
        self._cfg = cfg
        self._store = store
        self._clock = clock
        self._queue: queue.Queue[_Item | object] = queue.Queue(
            maxsize=cfg.queue_max_records)
        self._shutdown = False
        self._drop_on_stop = False
        # Set whenever a group has been fsynced (tests / uploader may wait).
        self.flushed = group_event if group_event is not None else threading.Event()

    # ------------------------------------------------------------ producers

    def submit(self, payload: dict, ts: float | None = None) -> int:
        """Persist one frame; returns its assigned seq.

        Blocks on flash quota (before queueing) and until fsync completes.
        Raises ShutdownError if the writer is shutting down; a frame whose
        fsync fails surfaces the original exception here.
        """
        if self._shutdown:
            raise ShutdownError("writer is shutting down")
        raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        ts = self._clock.time() if ts is None else ts
        reserved = len(raw) + SEQ_OVERHEAD
        self._store.reserve(reserved)  # may block on quota, or raise ShutdownError
        item = _Item(reserved=reserved, ts=ts, raw_payload=raw,
                     event=threading.Event())
        if self._shutdown:
            self._store.release_reservation(reserved)
            raise ShutdownError("writer is shutting down")
        self._queue.put(item)
        item.event.wait()
        if item.error is not None:
            raise item.error
        return item.seq_out

    # --------------------------------------------------------------- thread

    def run(self) -> None:
        q = self._queue
        while True:
            try:
                item = q.get(timeout=self._store.idle_wait_s())
            except queue.Empty:
                # Idle: rotate a non-empty segment past its age limit so its
                # tail becomes uploadable even when no new frames arrive.
                self._store.rotate_if_due()
                continue
            if item is _SENTINEL:
                return
            if self._drop_on_stop:
                self._store.release_reservation(item.reserved)
                self._fail_item(item, ShutdownError("writer stopped without draining"))
                continue
            group: list[_Item] = []
            try:
                self._take_group(item, group)
            except _Stop:
                if group:
                    self._commit(group)
                return
            self._commit(group)

    @staticmethod
    def _fail_item(item: _Item, exc: BaseException) -> None:
        item.error = exc
        item.event.set()

    def _take_group(self, first: _Item, group: list[_Item]) -> None:
        """Block up to group_max_wait_s for the first frame, then gather."""
        group.append(first)
        deadline = self._clock.time() + self._cfg.group_max_wait_s
        while len(group) < self._cfg.group_max_records:
            remaining = deadline - self._clock.time()
            if remaining <= 0:
                return
            try:
                item = self._queue.get(timeout=remaining)
            except queue.Empty:
                return
            if item is _SENTINEL:
                raise _Stop()
            group.append(item)

    def _commit(self, group: list[_Item]) -> None:
        try:
            commits = [(it.reserved, it.ts, it.raw_payload) for it in group]
            seqs = self._store.commit_group(commits)
        except BaseException as exc:
            # A failed fsync leaves the on-disk tail unknown; fail this group
            # and everything still queued rather than append past the fault.
            self._shutdown = True
            self._drop_on_stop = True
            self._store.begin_shutdown()
            for it in group:
                self._store.release_reservation(it.reserved)
                self._fail_item(it, exc)
            return
        for it, seq in zip(group, seqs):
            it.seq_out = seq
            it.event.set()
        self.flushed.set()

    # -------------------------------------------------------------- control

    def shutdown(self, drain: bool = True) -> None:
        """Stop accepting submits and stop the thread.

        drain=True (default): queued frames are fsynced before exit.
        drain=False: queued-but-uncommitted frames fail with ShutdownError.
        """
        self._shutdown = True
        if not drain:
            self._drop_on_stop = True
        self._store.begin_shutdown()  # wakes producers blocked in reserve()
        self._queue.put(_SENTINEL)
        self.join()


class _Stop(Exception):
    """Internal: sentinel observed while gathering a group."""
