"""Uploader thread: batched, at-least-once cloud delivery.

Loop:
  1. assemble the next batch of unacked frames from *closed* segments;
  2. POST the same immutable batch bytes (same batch_id) until a 2xx ACK;
  3. advance the local watermark (durable state before any segment unlink);
  4. on IO error / 5xx / lost response, back off exponentially and resend —
     the cloud dedupes by seq, so retries never create duplicates.

Only a single uploader thread exists, and batches are sent strictly in seq
order, so the cloud's "frames start at watermark+1" contract always holds.
"""

from __future__ import annotations

import http.client
import json
import logging
import threading
import time
from urllib.parse import urlsplit

from .config import Config
from .segments import Batch, Store

log = logging.getLogger("gateway.uploader")


class UploadError(Exception):
    """Any non-ACK outcome; the batch is retried unchanged."""


class UploadWorker(threading.Thread):
    def __init__(self, cfg: Config, store: Store, clock=time) -> None:
        super().__init__(name="gateway-uploader", daemon=True)
        self._cfg = cfg
        self._store = store
        self._clock = clock
        self._stop = threading.Event()
        self._wake = threading.Event()
        parts = urlsplit(cfg.cloud_url)
        if parts.scheme != "http":
            raise ValueError(f"unsupported cloud scheme: {parts.scheme!r}")
        self._host = parts.hostname or "127.0.0.1"
        self._port = parts.port or 80
        self._path = cfg.cloud_path
        # Set after every successfully applied ACK (tests wait on it).
        self.acked = threading.Event()

    def wake(self) -> None:
        self._wake.set()

    def drain(self, timeout: float = 30.0) -> bool:
        """Best-effort flush of all currently closed unacked data.

        Used during graceful shutdown after the writer force-rotates its
        active segment. Returns True when the backlog reached zero; a cloud
        outage makes it time out with data safely retained on flash.
        """
        deadline = self._clock.time() + timeout
        backoff = self._cfg.backoff_initial_s
        while self._clock.time() < deadline:
            batch = self._store.next_batch()
            if batch is None:
                return self._store.snapshot().backlog == 0
            try:
                acked = self._post_once(batch)
            except UploadError as exc:
                log.info("drain upload failed (%s)", exc)
                remaining = deadline - self._clock.time()
                if remaining <= 0:
                    return False
                self._stop.wait(timeout=min(backoff, remaining))
                backoff = min(backoff * 2, self._cfg.backoff_max_s)
                continue
            backoff = self._cfg.backoff_initial_s
            if acked > self._store.acked_seq:
                self._store.apply_ack(acked)
        return False

    def shutdown(self) -> None:
        self._stop.set()
        self._wake.set()
        self.join(timeout=self._cfg.http_timeout_s + 1)

    # ------------------------------------------------------------------ loop

    def run(self) -> None:
        backoff = self._cfg.backoff_initial_s
        while not self._stop.is_set():
            batch = self._store.next_batch()
            if batch is None:
                # Nothing uploadable (empty or only data in the active segment).
                if self._wait_idle():
                    return
                continue
            try:
                acked = self._post_once(batch)
            except UploadError as exc:
                log.info("upload failed (%s); retry in %.1fs", exc, backoff)
                if self._wait(backoff):
                    return
                backoff = min(backoff * 2, self._cfg.backoff_max_s)
                continue
            backoff = self._cfg.backoff_initial_s
            if acked > self._store.acked_seq:
                self._store.apply_ack(acked)
                log.debug("acked up to %d", acked)
                self.acked.set()
            else:  # stale ACK to an already-advanced watermark; data retried
                log.debug("stale acked_seq=%d ignored", acked)

    def _wait_idle(self) -> bool:
        """Wait until new closed data may exist; True means stop requested."""
        self._store.segments_ready.clear()
        if self._store.next_batch() is not None:
            return False
        return self._wait(min(self._cfg.backoff_max_s, 1.0))

    def _wait(self, seconds: float) -> bool:
        self._wake.wait(timeout=seconds)
        self._wake.clear()
        return self._stop.is_set()

    # ------------------------------------------------------------------ HTTP

    def _post_once(self, batch: Batch) -> int:
        conn = http.client.HTTPConnection(
            self._host, self._port, timeout=self._cfg.http_timeout_s)
        try:
            try:
                conn.request(
                    "POST", self._path, body=batch.body,
                    headers={"Content-Type": "application/json",
                             "X-Batch-Id": batch.batch_id},
                )
                resp = conn.getresponse()
                body = resp.read()
            except (OSError, http.client.HTTPException) as exc:
                raise UploadError(str(exc)) from exc
            if resp.status != 200:
                raise UploadError(f"HTTP {resp.status}")
            try:
                obj = json.loads(body)
                acked = int(obj["acked_seq"])
            except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                raise UploadError(f"bad ACK body: {exc}") from exc
            if acked < batch.first_seq - 1 or acked < self._store.acked_seq:
                raise UploadError(f"implausible acked_seq={acked}")
            return acked
        finally:
            conn.close()
