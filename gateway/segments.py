"""On-flash segment store.

Layout under ``data_dir``::

    segment-000001.log   JSONL, one record per line, binary mode
    segment-000002.log
    state.json           {"acked_seq": N, "crc32": ...} (atomically replaced)

Invariants:
  * seq is a global dense increasing counter assigned by the single writer.
  * Records are only ever *appended*; segments are immutable once closed.
  * Reclamation granularity is a whole closed segment whose max_seq <= acked.
  * state.json is fsynced BEFORE any segment file is unlinked.

Recovery: every segment except the highest-numbered must be a chain of valid
records contiguous in seq; any fault there is DataCorruptionError (refuse to
start). The highest segment may carry a torn tail (power loss mid-write); it is
truncated at the last valid newline boundary.
"""

from __future__ import annotations

import bisect
import json
import logging
import os
import re
import threading
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from .config import Config
from .durability import atomic_replace_write, fsync_dir
from .models import DataCorruptionError, FrameTooLargeError, Sample, ShutdownError, Stats

log = logging.getLogger("gateway.store")

SEGMENT_RE = re.compile(r"^segment-(\d+)\.log$")
STATE_NAME = "state.json"
SAMPLE_INTERVAL = 128 * 1024
# Allowance for the {"seq":..,"ts":..,"payload":} envelope added after the
# producer reserves its serialized payload bytes.
SEQ_OVERHEAD = 64

# A commit handed from writer to store: (reserved_bytes, ts, raw_payload_json)
Commit = tuple[int, float, bytes]


def segment_path(data_dir: Path, index: int) -> Path:
    return data_dir / f"segment-{index:06d}.log"


@dataclass
class Segment:
    index: int
    path: Path
    size: int = 0
    min_seq: int = 0
    max_seq: int = 0
    count: int = 0
    first_ts: float | None = None
    last_ts: float | None = None
    samples: list[Sample] = field(default_factory=list)
    closed: bool = False
    create_time: float = 0.0
    fh: BinaryIO | None = None  # open only on the active segment


def _sample_seqs(seg: Segment) -> list[int]:
    return [s.seq for s in seg.samples]


def _scan_open_segment(seg: Segment, is_highest: bool) -> int:
    """Validate segment contents and populate metadata.

    Returns the byte offset just past the last valid record and sets
    ``seg.size``. Raises DataCorruptionError on a bad record anywhere except a
    torn tail of the highest segment.
    """
    data = seg.path.read_bytes()
    offset = 0
    expected_seq: int | None = None
    while offset < len(data):
        nl = data.find(b"\n", offset)
        if nl == -1:
            break  # torn final line
        line = data[offset:nl]
        try:
            rec = json.loads(line)
            seq = rec["seq"]
            ts = rec["ts"]
            if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
                raise ValueError("bad seq")
            if not isinstance(ts, (int, float)) or isinstance(ts, bool):
                raise ValueError("bad ts")
            if "payload" not in rec:
                raise ValueError("missing payload")
            if expected_seq is not None and seq != expected_seq + 1:
                raise ValueError("seq gap inside segment")
        except (ValueError, TypeError, KeyError, json.JSONDecodeError):
            if is_highest:
                break
            raise DataCorruptionError(
                f"invalid record at byte {offset} in closed segment {seg.path.name}"
            )
        ts = float(ts)
        if seg.count == 0:
            seg.min_seq = seq
            seg.first_ts = ts
            seg.samples.append(Sample(offset, seq, ts))
        elif offset - seg.samples[-1].offset >= SAMPLE_INTERVAL:
            seg.samples.append(Sample(offset, seq, ts))
        seg.max_seq = seq
        seg.last_ts = ts
        seg.count += 1
        expected_seq = seq
        offset = nl + 1
    seg.size = offset
    if offset != len(data) and not is_highest:
        raise DataCorruptionError(f"trailing garbage in closed segment {seg.path.name}")
    return offset


class Store:
    """Thread-safe segment store. One writer thread, many producer/reader
    threads. Flash quota is enforced on producers via :meth:`reserve`."""

    def __init__(self, cfg: Config, clock, segments: list[Segment],
                 acked_seq: int, warnings: list[str]) -> None:
        self._cfg = cfg
        self._clock = clock
        self._segments = segments
        self._acked_seq = acked_seq
        self.warnings = warnings
        self._cv = threading.Condition(threading.Lock())
        self._reserved = 0
        self._closing = False
        # Highest seq ever durably written; survives segment reclamation
        # (unlike max over the surviving segment list).
        self._max_seq = max((s.max_seq for s in segments if s.count), default=0)
        # Set on rotation / startup so the uploader scans for new closed data.
        self.segments_ready = threading.Event()
        self.segments_ready.set()
        active = segments[-1] if segments else None
        if active is not None:
            if active.create_time == 0.0:
                active.create_time = clock.time()
            if active.fh is None:
                active.fh = open(active.path, "ab")

    # ============================================================== recovery

    @classmethod
    def open(cls, cfg: Config, clock) -> "Store":
        data_dir = cfg.data_dir
        data_dir.mkdir(parents=True, exist_ok=True)

        for stale in data_dir.glob("*.tmp"):
            log.warning("removing stale temp file %s", stale.name)
            stale.unlink()

        warnings: list[str] = []
        acked_seq = cls._load_state(data_dir, warnings)

        names = [p for p in data_dir.iterdir() if SEGMENT_RE.match(p.name)]
        names.sort(key=lambda p: int(SEGMENT_RE.match(p.name).group(1)))
        segments: list[Segment] = []
        for i, path in enumerate(names):
            seg = Segment(index=int(SEGMENT_RE.match(path.name).group(1)),
                          path=path, closed=(i != len(names) - 1))
            good = _scan_open_segment(seg, is_highest=(i == len(names) - 1))
            if good != path.stat().st_size:
                with open(path, "r+b") as fh:  # torn tail on highest segment
                    fh.truncate(good)
                    os.fsync(fh.fileno())
                log.warning("truncated torn tail of %s at byte %d", path.name, good)
            segments.append(seg)

        # The first surviving segment may start > 1 (earlier acked segments
        # were reclaimed before); adjacent surviving segments must be dense.
        for prev, cur in zip(segments, segments[1:]):
            if prev.count and cur.count and cur.min_seq != prev.max_seq + 1:
                raise DataCorruptionError(
                    f"seq gap between {prev.path.name} and {cur.path.name}"
                )

        max_seq = max((s.max_seq for s in segments if s.count), default=0)
        if acked_seq > max_seq:
            warnings.append(
                f"state.json acked_seq={acked_seq} > recovered max_seq={max_seq}; "
                "resetting watermark to 0"
            )
            log.warning(warnings[-1])
            acked_seq = 0

        store = cls(cfg, clock, segments, acked_seq, warnings)
        if not segments:
            now = clock.time()
            with store._cv:
                store._add_active_segment_locked(index=1, now=now)
        # Startup reclamation (state.json already durable with this watermark).
        with store._cv:
            victims = store._reclaim_locked()
        for seg in victims:
            try:
                seg.path.unlink()
            except FileNotFoundError:
                pass
        if victims:
            fsync_dir(data_dir)
        return store

    @staticmethod
    def _load_state(data_dir: Path, warnings: list[str]) -> int:
        path = data_dir / STATE_NAME
        if not path.exists():
            return 0
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
            acked = obj["acked_seq"]
            if not isinstance(acked, int) or isinstance(acked, bool) or acked < 0:
                raise ValueError("bad acked_seq")
            canonical = json.dumps({"acked_seq": acked}, sort_keys=True).encode()
            if obj.get("crc32") != (zlib.crc32(canonical) & 0xFFFFFFFF):
                raise ValueError("crc mismatch")
            return acked
        except (ValueError, TypeError, KeyError, json.JSONDecodeError, OSError) as exc:
            warnings.append(f"unreadable state.json ({exc}); starting at watermark 0")
            log.warning(warnings[-1])
            return 0

    def _write_state_locked(self) -> None:
        canonical = json.dumps({"acked_seq": self._acked_seq}, sort_keys=True).encode()
        payload = json.dumps(
            {"acked_seq": self._acked_seq,
             "crc32": zlib.crc32(canonical) & 0xFFFFFFFF},
            sort_keys=True,
        ).encode()
        atomic_replace_write(self._cfg.data_dir / STATE_NAME, payload)

    # ============================================================== segments

    def _add_active_segment_locked(self, index: int, now: float) -> Segment:
        path = segment_path(self._cfg.data_dir, index)
        seg = Segment(index=index, path=path,
                      size=path.stat().st_size if path.exists() else 0,
                      closed=False, create_time=now)
        seg.fh = open(path, "ab")
        self._segments.append(seg)
        fsync_dir(self._cfg.data_dir)
        return seg

    def _rotate_locked(self, now: float) -> None:
        """Close the active segment and open the next one (between groups)."""
        old = self._segments[-1]
        if old.fh is not None:
            old.fh.close()
            old.fh = None
        old.closed = True
        self._add_active_segment_locked(old.index + 1, now)
        self.segments_ready.set()

    def _reclaim_locked(self) -> list[Segment]:
        """Remove fully-acked closed segments from the registry.

        The caller unlinks the files; state.json must already be durable.
        """
        victims = [s for s in self._segments
                   if s.closed and s.count > 0 and s.max_seq <= self._acked_seq]
        if victims:
            victim_ids = {id(s) for s in victims}
            self._segments = [s for s in self._segments if id(s) not in victim_ids]
        return victims

    def _max_seq_locked(self) -> int:
        return self._max_seq

    def _bytes_on_disk_locked(self) -> int:
        return sum(s.size for s in self._segments)

    # ============================================================== producer

    def reserve(self, nbytes: int) -> None:
        """Block until flash quota covers ``nbytes`` more buffered bytes.

        Raises FrameTooLargeError immediately if one frame alone exceeds the
        whole quota: no amount of cloud ACK could ever free room for it, so
        blocking would hang every collector forever.
        """
        if nbytes > self._cfg.quota_bytes:
            raise FrameTooLargeError(
                f"frame needs {nbytes} bytes but quota is "
                f"{self._cfg.quota_bytes}")
        with self._cv:
            while not self._closing:
                if self._bytes_on_disk_locked() + self._reserved + nbytes <= self._cfg.quota_bytes:
                    self._reserved += nbytes
                    return
                self._cv.wait()
            raise ShutdownError("gateway shutting down")

    def release_reservation(self, nbytes: int) -> None:
        with self._cv:
            self._reserved -= nbytes

    def idle_wait_s(self) -> float:
        """Suggested poll interval: capped so shutdown/idle rotation react
        promptly even when the active segment is fresh or empty."""
        with self._cv:
            active = self._segments[-1]
            if active.count == 0:
                left = self._cfg.segment_max_age_s
            else:
                left = (active.create_time + self._cfg.segment_max_age_s
                        - self._clock.time())
        return min(max(0.05, left), 0.5)

    def rotate_active(self) -> bool:
        """Rotate the active segment immediately if it holds any records."""
        with self._cv:
            if self._segments[-1].count == 0:
                return False
            self._rotate_locked(self._clock.time())
            return True

    def rotate_if_due(self) -> None:
        """Rotate the non-empty active segment once past size/age limits.
        Writer thread only; serialized against commit_group by being the
        sole caller of both."""
        with self._cv:
            now = self._clock.time()
            active = self._segments[-1]
            if active.count > 0 and (
                active.size >= self._cfg.segment_bytes
                or now - active.create_time >= self._cfg.segment_max_age_s
            ):
                self._rotate_locked(now)

    def commit_group(self, commits: list[Commit]) -> list[int]:
        """Append one group with a single fsync. Writer thread only.

        Rotation (if due) happens BEFORE the write, always between groups.
        Returns the seq assigned to each commit in order.
        """
        with self._cv:
            now = self._clock.time()
            active = self._segments[-1]
            if active.count > 0 and (
                active.size >= self._cfg.segment_bytes
                or now - active.create_time >= self._cfg.segment_max_age_s
            ):
                self._rotate_locked(now)
                active = self._segments[-1]
            first_seq = self._max_seq_locked() + 1
            seqs = list(range(first_seq, first_seq + len(commits)))
            parts: list[bytes] = []
            for seq, (_, ts, raw_payload) in zip(seqs, commits):
                parts.append(
                    b'{"seq":' + str(seq).encode("ascii")
                    + b',"ts":' + json.dumps(float(ts)).encode("ascii")
                    + b',"payload":' + raw_payload + b"}\n"
                )
            blob = b"".join(parts)
            base = active.size
            fh = active.fh

        fh.write(blob)
        fh.flush()
        os.fsync(fh.fileno())

        with self._cv:
            pos = base
            for part, (reserved, ts, raw_payload), seq in zip(parts, commits, seqs):
                end = pos + len(part)
                self._record_locked(active, seq, ts, pos, end)
                pos = end
                self._reserved -= reserved
            self._max_seq = seqs[-1]
            self._cv.notify_all()
        return seqs

    def _record_locked(self, seg: Segment, seq: int, ts: float,
                       start: int, end: int) -> None:
        if seg.count == 0:
            seg.min_seq = seq
            seg.first_ts = ts
            seg.samples.append(Sample(start, seq, ts))
        elif start - seg.samples[-1].offset >= SAMPLE_INTERVAL:
            seg.samples.append(Sample(start, seq, ts))
        seg.max_seq = seq
        seg.last_ts = ts
        seg.count += 1
        seg.size = end

    # ============================================================== ack/free

    def apply_ack(self, acked_seq: int) -> None:
        """Advance the watermark: durable state first, unlink after."""
        victims: list[Segment] = []
        with self._cv:
            if acked_seq <= self._acked_seq:
                return
            self._acked_seq = acked_seq
            self._write_state_locked()
            victims = self._reclaim_locked()
        for seg in victims:
            try:
                seg.path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                log.exception("failed to unlink %s; reclaimed again on restart",
                              seg.path)
        if victims:
            fsync_dir(self._cfg.data_dir)
            with self._cv:
                self._cv.notify_all()

    # ============================================================== reading

    @staticmethod
    def _start_offset(seg: Segment, target_seq: int) -> int:
        """Byte offset at/before which the frame with target_seq starts."""
        if target_seq <= seg.min_seq:
            return 0
        idx = bisect.bisect_right(_sample_seqs(seg), target_seq) - 1
        return seg.samples[idx].offset

    def _read_closed_plan_locked(self) -> list[tuple[Path, int]]:
        target = self._acked_seq + 1
        plan: list[tuple[Path, int]] = []
        for seg in self._segments:
            if seg.closed and seg.count and seg.max_seq >= target:
                plan.append((seg.path, self._start_offset(seg, target)))
        return plan

    def next_batch(self) -> "Batch | None":
        """Assemble one upload batch from the oldest unacked closed frames.

        The first eligible frame is ALWAYS taken, even when it alone exceeds
        batch_max_records/batch_max_bytes: otherwise one oversized frame would
        block the queue forever (never uploaded, never reclaimed, collectors
        stuck on quota once flash fills). Such a frame forms a singleton
        batch and clears the head of the queue.
        """
        with self._cv:
            target = self._acked_seq + 1
            plan = self._read_closed_plan_locked()
            max_records = self._cfg.batch_max_records
            max_bytes = self._cfg.batch_max_bytes
        frames: list[dict] = []
        wire_bytes = 2  # "[]"
        full = False
        for path, start in plan:
            with open(path, "rb") as fh:
                fh.seek(start)
                for line in fh:
                    if not line.endswith(b"\n"):
                        break  # defensive: closed segments are complete
                    rec = json.loads(line)
                    if rec["seq"] < target:
                        continue
                    if frames and (
                        len(frames) >= max_records
                        or wire_bytes + len(line) + 1 > max_bytes
                    ):
                        full = True
                        break
                    frames.append(rec)
                    wire_bytes += len(line) + 1
                if full:
                    break
        if not frames:
            return None
        first_seq = frames[0]["seq"]
        last_seq = frames[-1]["seq"]
        batch_id = f"b{first_seq:012d}-{last_seq:012d}"
        body = json.dumps(
            {"batch_id": batch_id, "first_seq": first_seq, "frames": frames},
            separators=(",", ":"),
        ).encode("utf-8")
        return Batch(batch_id=batch_id, first_seq=first_seq,
                     last_seq=last_seq, body=body)

    def oldest_unacked_ts(self) -> float | None:
        """Timestamp of the oldest frame with seq > acked_seq."""
        with self._cv:
            target = self._acked_seq + 1
            if target > self._max_seq_locked():
                return None
            for seg in self._segments:
                if seg.count and seg.max_seq >= target and seg.min_seq <= target:
                    idx = bisect.bisect_right(_sample_seqs(seg), target) - 1
                    sample = seg.samples[max(idx, 0)]
                    path, start = seg.path, sample.offset
                    break
            else:  # pragma: no cover - max_seq guarantees a segment
                return None
        with open(path, "rb") as fh:
            fh.seek(start)
            for line in fh:
                if not line.endswith(b"\n"):
                    break
                rec = json.loads(line)
                if rec["seq"] >= target:
                    return float(rec["ts"])
        return None  # pragma: no cover

    def snapshot(self) -> Stats:
        with self._cv:
            max_seq = self._max_seq_locked()
            bytes_used = self._bytes_on_disk_locked()
            reclaimable = sum(
                s.size for s in self._segments
                if s.closed and s.count and s.max_seq <= self._acked_seq
            )
        oldest = self.oldest_unacked_ts() if max_seq > self._acked_seq else None
        return Stats(
            backlog=max_seq - self._acked_seq,
            bytes_used=bytes_used,
            quota=self._cfg.quota_bytes,
            reclaimable_bytes=reclaimable,
            oldest_ts=oldest,
            max_seq=max_seq,
            acked_seq=self._acked_seq,
        )

    @property
    def acked_seq(self) -> int:
        return self._acked_seq

    # ============================================================== shutdown

    def begin_shutdown(self) -> None:
        with self._cv:
            self._closing = True
            self._cv.notify_all()

    def close(self) -> None:
        self.begin_shutdown()
        with self._cv:
            active = self._segments[-1] if self._segments else None
            fh = active.fh if active is not None else None
            if active is not None:
                active.fh = None
        if fh is not None:
            fh.close()


@dataclass(frozen=True)
class Batch:
    batch_id: str
    first_seq: int
    last_seq: int
    body: bytes
