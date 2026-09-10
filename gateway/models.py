"""Data types and exceptions shared across gateway modules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class GatewayError(Exception):
    """Base class for gateway errors."""


class DataCorruptionError(GatewayError):
    """A *closed* segment or the segment chain is invalid.

    Raised during recovery when anything other than a torn tail of the
    highest-numbered segment is found. The data directory must be inspected
    manually; the gateway refuses to start rather than risk deleting
    unacknowledged data.
    """


class ShutdownError(GatewayError):
    """Raised by submit() after the gateway started shutting down."""


class FrameTooLargeError(GatewayError):
    """A single frame can never fit in the configured flash quota."""


class Clock(Protocol):
    """Injectable clock (tests use a fake one to drive rotation/batching)."""

    def time(self) -> float: ...


@dataclass(frozen=True)
class Stats:
    backlog: int           # frames with seq > acked_seq
    bytes_used: int        # bytes of all segment files currently on flash
    quota: int
    reclaimable_bytes: int  # bytes of fully-acked closed segments (pending delete)
    oldest_ts: float | None  # timestamp of the oldest unacknowledged frame
    max_seq: int
    acked_seq: int


@dataclass(frozen=True)
class Sample:
    """Sparse index entry inside a segment: a full line starts at offset."""

    offset: int
    seq: int
    ts: float
