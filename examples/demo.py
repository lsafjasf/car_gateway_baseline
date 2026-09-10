"""Demo: mock cloud + gateway + simulated collector threads.

    python -m examples.demo
"""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from gateway.app import Gateway  # noqa: E402
from gateway.cloud_mock import serve_in_thread  # noqa: E402
from gateway.config import Config  # noqa: E402


def main(base: Path) -> None:
    cloud = serve_in_thread(base / "cloud", port=8080)
    print(f"mock cloud: {cloud.url}")
    cfg = Config.default(base / "data").with_overrides(cloud_url=cloud.url)
    gw = Gateway(cfg).start()
    print(f"stats API:  {gw.api_url}/stats")

    stop = threading.Event()

    def collector(tid: int) -> None:
        i = 0
        while not stop.is_set():
            gw.submit({"tid": tid, "seq_in_thread": i,
                       "speed_kmh": 60 + (i % 20), "ts_src": time.time()})
            i += 1
            time.sleep(0.01)

    threads = [threading.Thread(target=collector, args=(t,), daemon=True)
               for t in range(3)]
    for t in threads:
        t.start()
    try:
        for _ in range(5):
            time.sleep(1)
            s = gw.store.snapshot()
            print(f"max_seq={s.max_seq} acked={s.acked_seq} "
                  f"backlog={s.backlog} bytes={s.bytes_used} "
                  f"oldest_ts={s.oldest_ts}")
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        gw.stop()
        cloud.shutdown()
        cloud.server_close()
        print("cloud ingested:", len(cloud.cloud.ingested), "frames")


if __name__ == "__main__":
    main(Path(__file__).resolve().parent / "_demo_data")
