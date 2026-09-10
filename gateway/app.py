"""Gateway assembly and lifecycle.

    gw = Gateway(Config.default("./data"))
    gw.start()
    seq = gw.submit({"signal": "speed", "value": 60})
    gw.stop()

Also runnable as ``python -m gateway.app --config cfg.json``; the process
starts writer + uploader + stats API and exits on SIGINT/SIGTERM.
"""

from __future__ import annotations

import argparse
import logging
import signal
import time

from . import api
from .config import Config
from .segments import Store
from .uploader import UploadWorker
from .writer import WriteWorker


class Gateway:
    def __init__(self, cfg: Config, clock=time) -> None:
        self.cfg = cfg
        self._clock = clock
        self.store = Store.open(cfg, clock)
        self.writer = WriteWorker(cfg, self.store, clock)
        self.uploader = UploadWorker(cfg, self.store, clock)
        self._api: api.StatsApiServer | None = None

    def start(self, serve_api: bool = True) -> "Gateway":
        self.writer.start()
        self.uploader.start()
        if serve_api:
            self._api = api.serve_in_thread(
                self.store, self.cfg.api_host, self.cfg.api_port)
        return self

    def submit(self, payload: dict, ts: float | None = None) -> int:
        """Block until the frame is fsynced; returns its seq."""
        seq = self.writer.submit(payload, ts)
        self.uploader.wake()
        return seq

    @property
    def api_url(self) -> str | None:
        return self._api.url if self._api is not None else None

    def stop(self, drain: bool = True, upload_timeout: float = 10.0) -> None:
        self.writer.shutdown(drain=drain)
        if drain:
            # Make the active segment's tail uploadable, then flush.
            self.store.rotate_active()
            self.uploader.shutdown()
            self.uploader.drain(timeout=upload_timeout)
        else:
            self.uploader.shutdown()
        if self._api is not None:
            self._api.shutdown()
            self._api.server_close()
            self._api = None
        self.store.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="vehicle acquisition gateway")
    parser.add_argument("--config", help="JSON config path")
    parser.add_argument("--data-dir", help="data directory (overrides config)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.config:
        cfg = Config.load(args.config)
        if args.data_dir:
            from pathlib import Path
            cfg = cfg.with_overrides(data_dir=Path(args.data_dir))
    elif args.data_dir:
        cfg = Config.default(args.data_dir)
    else:
        parser.error("either --config or --data-dir is required")

    gw = Gateway(cfg).start()
    logging.info("gateway started; data_dir=%s stats=%s cloud=%s",
                 cfg.data_dir, gw.api_url, cfg.cloud_url)

    stop_requested = False

    def _handle(signum, frame):
        nonlocal stop_requested
        stop_requested = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle)
        except (ValueError, OSError):
            pass  # not in main thread / unsupported platform

    try:
        while not stop_requested:
            time.sleep(0.2)
    finally:
        logging.info("shutting down (draining)")
        gw.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
