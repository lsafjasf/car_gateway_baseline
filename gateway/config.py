"""Gateway configuration.

All knobs in one frozen dataclass. Build via ``Config.default(data_dir)`` or
load a JSON file with ``Config.load``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path

MiB = 1024 * 1024


@dataclass(frozen=True)
class Config:
    data_dir: Path
    # Flash quota; collectors block in submit() once live+reserved hits it.
    quota_bytes: int = 512 * MiB
    # Segment rotation happens only between committed groups.
    segment_bytes: int = 8 * MiB
    segment_max_age_s: float = 2.0
    # Group commit: one write()+fsync() per group.
    group_max_wait_s: float = 0.01
    group_max_records: int = 256
    # In-memory backpressure between collector threads and the writer.
    queue_max_records: int = 10_000
    # Upload batching.
    batch_max_records: int = 500
    batch_max_bytes: int = 256 * 1024
    batch_max_age_s: float = 1.0
    # Upload retry backoff (seconds).
    backoff_initial_s: float = 0.5
    backoff_max_s: float = 30.0
    http_timeout_s: float = 10.0
    cloud_url: str = "http://127.0.0.1:8080"
    cloud_path: str = "/batches"
    api_host: str = "127.0.0.1"
    api_port: int = 8081

    @classmethod
    def default(cls, data_dir: str | Path) -> "Config":
        return cls(data_dir=Path(data_dir))

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if "data_dir" not in raw:
            raise ValueError("config must set data_dir")
        raw["data_dir"] = Path(raw["data_dir"])
        known = {f.name for f in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        unknown = set(raw) - known
        if unknown:
            raise ValueError(f"unknown config keys: {sorted(unknown)}")
        return cls(**raw)

    def with_overrides(self, **kwargs) -> "Config":
        return replace(self, **kwargs)
