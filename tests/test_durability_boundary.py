"""Durability boundary: submit-returns-means-fsynced, shutdown semantics,
crash window between state.json fsync and segment unlink."""

from __future__ import annotations

import json
import time
import zlib

from conftest import make_config, seed_segments
from gateway.segments import Store
from gateway.writer import WriteWorker


def test_frame_visible_after_submit_without_graceful_stop(tmp_path):
    # submit() must return only after fsync: reopen the directory "hard"
    # (new Store, no drain) and the frame must be recovered.
    cfg = make_config(tmp_path)
    store = Store.open(cfg, time)
    w = WriteWorker(cfg, store, time)
    w.start()
    seq = w.submit({"hello": "world"})
    assert seq == 1
    # submit() returned only after fsync, so even a stop here cannot lose it.
    w.shutdown(drain=False)
    store.close()
    store2 = Store.open(cfg, time)
    try:
        assert store2.snapshot().max_seq == 1
    finally:
        store2.close()


def test_shutdown_drain_persists_queued_frames(tmp_path):
    cfg = make_config(tmp_path)
    store = Store.open(cfg, time)
    w = WriteWorker(cfg, store, time)
    w.start()
    for i in range(1, 21):
        w.submit({"n": i})
    w.shutdown(drain=True)
    store.close()
    store2 = Store.open(cfg, time)
    try:
        assert store2.snapshot().max_seq == 20
    finally:
        store2.close()


def test_crash_between_state_fsync_and_segment_unlink(tmp_path):
    # state.json says acked=4 but the fully-acked closed segments still exist
    # (power lost in the unlink window). Restart reclaims them safely and the
    # watermark is unchanged; the active segment's data is preserved.
    cfg = make_config(tmp_path)
    seed_segments(cfg.data_dir, [[1, 2], [3, 4], [5]])
    state = {"acked_seq": 4}
    canonical = json.dumps({"acked_seq": 4}, sort_keys=True).encode()
    state["crc32"] = zlib.crc32(canonical) & 0xFFFFFFFF
    (cfg.data_dir / "state.json").write_text(json.dumps(state))

    store = Store.open(cfg, time)
    try:
        names = sorted(p.name for p in cfg.data_dir.glob("segment-*.log"))
        assert names == ["segment-000003.log"]
        s = store.snapshot()
        assert s.acked_seq == 4 and s.backlog == 1 and s.max_seq == 5
    finally:
        store.close()
    # state.json still durable at 4
    assert json.loads((cfg.data_dir / "state.json").read_text())["acked_seq"] == 4


def test_repeated_restarts_keep_dense_seq(tmp_path):
    cfg = make_config(tmp_path)
    for round_no in range(3):
        store = Store.open(cfg, time)
        w = WriteWorker(cfg, store, time)
        w.start()
        base = round_no * 5
        for i in range(1, 6):
            seq = w.submit({"n": base + i})
            assert seq == base + i
        w.shutdown(drain=True)
        store.close()
    store = Store.open(cfg, time)
    try:
        assert store.snapshot().max_seq == 15
    finally:
        store.close()


def test_throughput_hundreds_per_second(tmp_path):
    # Aggregate throughput across many collector threads (the deployment
    # shape): concurrent submits fill group-commit batches.
    import threading
    cfg = make_config(tmp_path, group_max_wait_s=0.01, group_max_records=256)
    store = Store.open(cfg, time)
    w = WriteWorker(cfg, store, time)
    w.start()
    n_threads, per_thread = 8, 500
    n = n_threads * per_thread

    def producer(tid):
        for i in range(per_thread):
            w.submit({"tid": tid, "n": i, "v": i * 0.1})

    threads = [threading.Thread(target=producer, args=(t,))
               for t in range(n_threads)]
    try:
        t0 = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.perf_counter() - t0
    finally:
        w.shutdown()
        store.close()
    store2 = Store.open(cfg, time)
    try:
        assert store2.snapshot().max_seq == n
    finally:
        store2.close()
    rate = n / elapsed
    assert rate > 200, f"only {rate:.0f} frames/s"


def test_quota_default_is_512mb(tmp_path):
    from gateway.config import Config
    cfg = Config.default(tmp_path)
    assert cfg.quota_bytes == 512 * 1024 * 1024
