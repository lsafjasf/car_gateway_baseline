"""Minimal cloud endpoint for demos and tests.

Semantics (the contract the real cloud must provide):

* A batch carries ``first_seq`` and frames ``[{seq, ts, payload}, ...]``.
* Frames with ``seq <= watermark`` are duplicates (ACK was lost) and ignored.
* New frames must begin exactly at ``watermark + 1``; a gap answers 409.
* The NEW watermark is fsynced to ``watermark.json`` BEFORE the ACK is sent,
  so a mock crash cannot ACK data it did not durably ingest.
* POSTing the same batch again is idempotent and returns the same watermark.

Test hooks: ``fail_next(n, code=500)`` makes the next n POSTs fail;
``drop_next(n)`` closes the connection without responding (simulates a lost
ACK after server-side processing is unknown to the client).
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .durability import atomic_replace_write


class MockCloud:
    """Owns cloud state; the HTTP handler delegates to a guarded instance."""

    def __init__(self, state_dir: str | Path) -> None:
        self._dir = Path(state_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._state_path = self._dir / "watermark.json"
        self._lock = threading.Lock()
        if self._state_path.exists():
            obj = json.loads(self._state_path.read_text(encoding="utf-8"))
            self._watermark = int(obj["watermark"])
            self.ingested = {int(k): v for k, v in json.loads(
                (self._dir / "ingested.json").read_text("utf-8")).items()}
        else:
            self._watermark = 0
            self.ingested = {}
        self._fail_next = 0
        self._fail_code = 500
        self._drop_next = 0
        self.post_count = 0

    # ------------------------------------------------------------- test API

    def fail_next(self, n: int, code: int = 500) ->  None:
        with self._lock:
            self._fail_next = n
            self._fail_code = code

    def drop_next(self, n: int) -> None:
        with self._lock:
            self._drop_next = n

    @property
    def watermark(self) -> int:
        with self._lock:
            return self._watermark

    # ------------------------------------------------------------- ingest

    def handle_post(self, body: bytes) -> tuple[int, dict]:
        with self._lock:
            self.post_count += 1
            if self._drop_next > 0:
                self._drop_next -= 1
                raise _DropConnection()
            if self._fail_next > 0:
                self._fail_next -= 1
                return self._fail_code, {"error": "injected failure"}
            try:
                req = json.loads(body)
                frames = req["frames"]
                first_seq = int(req["first_seq"])
                if frames and int(frames[0]["seq"]) != first_seq:
                    return 400, {"error": "first_seq mismatch"}
                new_frames = [f for f in frames if int(f["seq"]) > self._watermark]
            except (ValueError, TypeError, KeyError, json.JSONDecodeError):
                return 400, {"error": "bad request"}
            if new_frames:
                # Validate the whole new run before ingesting anything so a
                # rejected batch leaves cloud state untouched.
                try:
                    expected = self._watermark
                    for f in new_frames:
                        seq = int(f["seq"])
                        if seq != expected + 1:
                            return 409, {"error": "seq gap",
                                         "watermark": self._watermark}
                        expected = seq
                except (ValueError, TypeError, KeyError):
                    return 400, {"error": "bad frame"}
                for f in new_frames:
                    self.ingested[int(f["seq"])] = f
                self._watermark = expected
                self._persist_locked()
            return 200, {"acked_seq": self._watermark}

    def _persist_locked(self) -> None:
        atomic_replace_write(
            self._state_path,
            json.dumps({"watermark": self._watermark}).encode("utf-8"),
        )
        # Ingested payloads are a test aid; rewritten in bulk (test-scale).
        atomic_replace_write(
            self._dir / "ingested.json",
            json.dumps(dict(sorted(self.ingested.items()))).encode("utf-8"),
        )


class _DropConnection(Exception):
    """Raised inside the handler to simulate an ACK lost on the wire."""


class _Handler(BaseHTTPRequestHandler):
    server_version = "gateway-mock-cloud/1.0"

    @property
    def cloud(self) -> MockCloud:
        return self.server.cloud  # type: ignore[attr-defined]

    def log_message(self, *args) -> None:  # silence stderr noise
        pass

    def _send_json(self, code: int, obj: dict) -> None:
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        if self.path != "/batches":
            self._send_json(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        try:
            code, obj = self.cloud.handle_post(body)
        except _DropConnection:
            self.close_connection = True
            try:
                self.wfile.flush()
            except OSError:
                pass
            return
        self._send_json(code, obj)

    def do_GET(self) -> None:
        if self.path == "/watermark":
            self._send_json(200, {"watermark": self.cloud.watermark})
        elif self.path == "/ingested":
            with self.cloud._lock:
                seqs = sorted(self.cloud.ingested)
            self._send_json(200, {"seqs": seqs, "count": len(seqs)})
        else:
            self._send_json(404, {"error": "not found"})


class MockCloudServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr, state_dir: str | Path) -> None:
        super().__init__(addr, _Handler)
        self.cloud = MockCloud(state_dir)

    @property
    def url(self) -> str:
        host, port = self.server_address[:2]
        return f"http://{host}:{port}"

    def __enter__(self) -> "MockCloudServer":
        return self

    def __exit__(self, *exc) -> None:
        self.shutdown()
        self.server_close()


def serve_in_thread(state_dir: str | Path, host: str = "127.0.0.1",
                    port: int = 0) -> MockCloudServer:
    server = MockCloudServer((host, port), state_dir)
    thread = threading.Thread(target=server.serve_forever, name="mock-cloud",
                              daemon=True)
    thread.start()
    server._thread = thread  # type: ignore[attr-defined]
    return server


def main(argv: list[str] | None = None) -> int:
    import argparse
    import logging

    parser = argparse.ArgumentParser(description="mock cloud for gateway")
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s mock-cloud: %(message)s")
    server = MockCloudServer((args.host, args.port), args.state_dir)
    logging.info("listening on http://%s:%d, state in %s",
                 args.host, args.port, args.state_dir)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
