"""Durability primitives.

The only place that performs fsync and atomic file replacement. Files are
always written in binary mode so byte offsets are identical on every platform
(text mode on Windows translates \\n to CRLF and would corrupt recovery).
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def fsync_file(fd: int) -> None:
    os.fsync(fd)


def fsync_dir(dir_path: Path) -> None:
    """Fsync a directory so that file creation/removal/replace is durable.

    No-op on Windows, where NTFS metadata is journaled and directories cannot
    be opened with os.open().
    """
    if os.name == "nt":
        return
    fd = os.open(str(dir_path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_replace_write(path: Path, payload: bytes, *, tmp_prefix: str = "") -> None:
    """Write ``payload`` to ``path`` atomically.

    write tmp -> fsync(tmp) -> os.replace -> fsync(dir).
    The tmp file is created in the same directory so os.replace never crosses
    volumes. A reader always sees either the old or the new complete file.
    """
    path = Path(path)
    prefix = tmp_prefix or (path.name + ".")
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=prefix, suffix=".tmp")
    tmp = Path(tmp_name)
    try:
        try:
            os.write(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, path)
        fsync_dir(path.parent)
    except BaseException:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
        raise
