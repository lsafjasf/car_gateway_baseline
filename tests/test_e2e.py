"""End-to-end: multiple producers -> writer -> uploader -> mock cloud,
including ACK loss, cloud restart, power-cut recovery and API checks."""

from __future__ import annotations

import json
import threading
import time
import urllib.request

from conftest import make_config
from gateway.app import Gateway
from gateway.cloud_mock import serve_in_thread


def start_stack(tmp_path, cloud_dir=None, **cfg_overrides):
    cloud = serve_in_thread(cloud_dir or (tmp_path / "cloud"))
    cfg = make_config(
        tmp_path,
        cloud_url=cloud.url,
        api_port=0,
        segment_max_age_s=0.1,
        group_max_wait_s=0.005,
        backoff_initial_s=0.05,
        backoff_max_s=0.5,
        http_timeout_s=5.0,
        **cfg_overrides,
    )
    gw = Gateway(cfg).start()
    return cloud, cfg, gw


def wait_drained(gw, n, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if gw.store.snapshot().max_seq == n and gw.store.snapshot().backlog == 0:
            return
        time.sleep(0.02)
    raise AssertionError(f"not drained: {gw.store.snapshot()}")


def test_dense_delivery_multi_producer(tmp_path):
    cloud, cfg, gw = start_stack(tmp_path)
    n_threads, per_thread = 4, 100

    def producer(tid):
        for i in range(per_thread):
            gw.submit({"tid": tid, "i": i, "line": "a\nb", "u": "车速"})

    threads = [threading.Thread(target=producer, args=(t,)) for t in range(n_threads)]
    try:
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        n = n_threads * per_thread
        wait_drained(gw, n)
    finally:
        gw.stop()
        cloud.shutdown()
        cloud.server_close()

    seqs = sorted(cloud.cloud.ingested)
    assert seqs == list(range(1, n + 1))
    payloads = list(cloud.cloud.ingested.values())
    assert all("payload" in f and "seq" in f and "ts" in f for f in payloads)


def test_lost_acks_cause_no_duplicate_ingest(tmp_path):
    cloud, cfg, gw = start_stack(tmp_path)
    cloud.cloud.fail_next(2, code=500)
    cloud.cloud.drop_next(2)
    try:
        for i in range(1, 61):
            gw.submit({"n": i})
        wait_drained(gw, 60)
    finally:
        gw.stop()
        cloud.shutdown()
        cloud.server_close()
    seqs = sorted(cloud.cloud.ingested)
    assert seqs == list(range(1, 61))
    assert cloud.cloud.post_count > 3  # retries actually happened


def test_cloud_restart_mid_stream_no_duplicates(tmp_path):
    cloud_dir = tmp_path / "cloud"
    cloud, cfg, gw = start_stack(tmp_path, cloud_dir=cloud_dir)
    try:
        for i in range(1, 31):
            gw.submit({"n": i})
        wait_drained(gw, 30)
        # Power-cycle the cloud (new server, same durable state dir).
        cloud.shutdown()
        cloud.server_close()
        cloud = serve_in_thread(cloud_dir, port=cloud.server_address[1])
        # Gateway still points at the same host:port.
        for i in range(31, 61):
            gw.submit({"n": i})
        wait_drained(gw, 60)
    finally:
        gw.stop()
        cloud.shutdown()
        cloud.server_close()
    assert sorted(cloud.cloud.ingested) == list(range(1, 61))


def test_retry_batch_bytes_are_identical(tmp_path):
    """Same batch_id + first_seq across retries (assembled deterministically)."""
    cloud, cfg, gw = start_stack(tmp_path, batch_max_records=10)
    seen_bodies = []
    orig = cloud.cloud.handle_post

    def spy(body):
        seen_bodies.append(body)
        return orig(body)

    cloud.cloud.handle_post = spy
    cloud.cloud.fail_next(2, code=500)
    try:
        for i in range(1, 11):
            gw.submit({"n": i})
        wait_drained(gw, 10)
    finally:
        gw.stop()
        cloud.shutdown()
        cloud.server_close()
    assert len(seen_bodies) >= 3
    assert seen_bodies[0] == seen_bodies[1] == seen_bodies[2]


def test_torn_write_recovery_then_full_upload(tmp_path):
    """Power cut mid-write, then restart: torn tail dropped, all committed
    frames eventually uploaded exactly once, and new frames continue dense."""
    import os

    cfg = make_config(
        tmp_path, api_port=0, cloud_url="http://127.0.0.1:1",
        segment_max_age_s=0.1, group_max_wait_s=0.005, http_timeout_s=2.0)
    gw = Gateway(cfg).start()
    try:
        for i in range(1, 61):  # network down: everything stays local
            gw.submit({"n": i})
        deadline = time.time() + 5
        while time.time() < deadline and gw.store.snapshot().max_seq != 60:
            time.sleep(0.01)
        gw.stop()
        # Power loss while a half-written record sits in the active segment.
        logs = sorted(cfg.data_dir.glob("segment-*.log"))
        with open(logs[-1], "ab") as fh:
            fh.write(b'{"seq": 999, "ts": 1, "paylo')
            fh.flush()
            os.fsync(fh.fileno())
    finally:
        if gw.writer.is_alive():
            gw.stop()

    cloud = serve_in_thread(tmp_path / "cloud")
    cfg2 = make_config(
        tmp_path, cloud_url=cloud.url, api_port=0,
        segment_max_age_s=0.1, group_max_wait_s=0.005,
        backoff_initial_s=0.05, backoff_max_s=0.5)
    gw = Gateway(cfg2).start()
    try:
        for i in range(61, 81):
            gw.submit({"n": i})
        wait_drained(gw, 80)
    finally:
        gw.stop()
        cloud.shutdown()
        cloud.server_close()
    assert sorted(cloud.cloud.ingested) == list(range(1, 81))


def test_graceful_stop_drains_active_segment(tmp_path):
    cloud, cfg, gw = start_stack(tmp_path)
    try:
        for i in range(1, 41):
            gw.submit({"n": i})
        # Stop while frames still sit in the active segment.
        gw.stop(upload_timeout=10.0)
        assert sorted(cloud.cloud.ingested) == list(range(1, 41))
    finally:
        cloud.shutdown()
        cloud.server_close()


def test_stats_api_json_and_healthz(tmp_path):
    cloud, cfg, gw = start_stack(tmp_path)
    try:
        with urllib.request.urlopen(gw.api_url + "/healthz", timeout=5) as r:
            assert json.loads(r.read()) == {"ok": True}
        for i in range(1, 21):
            gw.submit({"n": i})
        deadline = time.time() + 10
        while time.time() < deadline:
            with urllib.request.urlopen(gw.api_url + "/stats", timeout=5) as r:
                s = json.loads(r.read())
            if s["backlog"] == 0:
                break
            time.sleep(0.02)
        assert s["acked_seq"] == 20
        assert s["max_seq"] == 20
        assert s["oldest_ts"] is None
        assert s["quota_bytes"] == cfg.quota_bytes
        assert isinstance(s["bytes_used"], int)
    finally:
        gw.stop()
        cloud.shutdown()
        cloud.server_close()


def test_stats_oldest_ts_while_backlogged(tmp_path):
    # Cloud OFFLINE: frames accumulate; oldest_ts is the first frame's ts.
    cfg = make_config(tmp_path, api_port=0, segment_max_age_s=0.05,
                      group_max_wait_s=0.005, cloud_url="http://127.0.0.1:1")
    gw = Gateway(cfg).start()
    try:
        t0 = time.time()
        gw.submit({"n": 1}, ts=t0)
        gw.submit({"n": 2}, ts=t0 + 10)
        deadline = time.time() + 5
        while time.time() < deadline:
            s = gw.store.snapshot()
            if s.max_seq == 2:
                break
            time.sleep(0.01)
        s = gw.store.snapshot()
        assert s.backlog == 2
        assert abs(s.oldest_ts - t0) < 1e-6
    finally:
        gw.stop()
