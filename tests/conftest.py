"""Shared pytest helpers (importable as ``conftest`` by the test modules)."""

from __future__ import annotations

import json
import time
from pathlib import Path

from gateway.config import Config


def make_config(tmp_path: Path, **overrides) -> Config:
    # Keep rotation age generous so tests using real time are not racy;
    # rotation-specific tests pass their own smaller value.
    base = dict(
        data_dir=Path(str(tmp_path / "data")),
        segment_bytes=4096,
        segment_max_age_s=30.0,
        group_max_wait_s=0.005,
        group_max_records=32,
        batch_max_records=50,
        batch_max_bytes=8000,
    )
    base.update(overrides)
    return Config(**base)


def write_record_line(seq: int, ts: float, payload: dict) -> bytes:
    """Exact on-disk record encoding used by the store."""
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
    return (
        b'{"seq":' + str(seq).encode("ascii")
        + b',"ts":' + json.dumps(float(ts)).encode("ascii")
        + b',"payload":' + raw + b"}\n"
    )


def append_raw(data_dir: Path, name: str, data: bytes) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    with open(data_dir / name, "ab") as fh:
        fh.write(data)
        fh.flush()


def seed_segments(data_dir: Path, seqs_per_segment: list[list[int]],
                  torn: bool = False) -> None:
    """Write complete segments of valid records; optionally a torn final line."""
    data_dir.mkdir(parents=True, exist_ok=True)
    for idx, seqs in enumerate(seqs_per_segment, start=1):
        lines = b"".join(
            write_record_line(seq, 1000.0 + seq, {"n": seq}) for seq in seqs)
        if torn and idx == len(seqs_per_segment):
            lines += b'{"seq": 999, "ts": 1, "paylo'  # no newline / invalid
        append_raw(data_dir, f"segment-{idx:06d}.log", lines)


CLOCK = time


class FakeClock:
    """Manually advanced clock for rotation/batch timing tests."""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self.t = start

    def time(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt
