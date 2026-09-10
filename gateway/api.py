"""Local HTTP management API (stdlib http.server).

GET /stats ->
    {
      "backlog": <frames not yet cloud-acked>,
      "bytes_used": <segment bytes on flash>,
      "quota_bytes": <configured quota>,
      "reclaimable_bytes": <bytes pending deletion>,
      "oldest_ts": <unix ts of oldest unacked frame or null>,
      "max_seq": ...,
      "acked_seq": ...
    }
GET /healthz -> {"ok": true}
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .segments import Store


class _Handler(BaseHTTPRequestHandler):
    server_version = "gateway-api/1.0"

    @property
    def store(self) -> Store:
        return self.server.store  # type: ignore[attr-defined]

    def log_message(self, *args) -> None:
        pass

    def _send_json(self, code: int, obj: dict) -> None:
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._send_json(200, {"ok": True})
            return
        if self.path != "/stats":
            self._send_json(404, {"error": "not found"})
            return
        s = self.store.snapshot()
        self._send_json(200, {
            "backlog": s.backlog,
            "bytes_used": s.bytes_used,
            "quota_bytes": s.quota,
            "reclaimable_bytes": s.reclaimable_bytes,
            "oldest_ts": s.oldest_ts,
            "max_seq": s.max_seq,
            "acked_seq": s.acked_seq,
        })


class StatsApiServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr, store: Store) -> None:
        super().__init__(addr, _Handler)
        self.store = store

    @property
    def url(self) -> str:
        host, port = self.server_address[:2]
        return f"http://{host}:{port}"


def serve_in_thread(store: Store, host: str = "127.0.0.1",
                    port: int = 0) -> StatsApiServer:
    server = StatsApiServer((host, port), store)
    thread = threading.Thread(target=server.serve_forever,
                              name="gateway-api", daemon=True)
    thread.start()
    server._thread = thread  # type: ignore[attr-defined]
    return server
