"""Store stats, batching, oldest_ts, partial ACK, rotation, quota reclaim."""

from __future__ import annotations

import json
import threading
import time

from conftest import CLOCK, FakeClock, make_config, seed_segments
from gateway.segments import Store
from gateway.writer import WriteWorker


def open_store(tmp_path, clock=CLOCK, **cfg_kw):
    return Store.open(make_config(tmp_path, **cfg_kw), clock)


def test_empty_store_stats(tmp_path):
    store = open_store(tmp_path)
    try:
        s = store.snapshot()
        assert (s.backlog, s.max_seq, s.acked_seq, s.oldest_ts) == (0, 0, 0, None)
        assert s.bytes_used == 0
        assert s.quota == 512 * 1024 * 1024
    finally:
        store.close()


def test_partial_ack_keeps_segment_and_exact_oldest_ts(tmp_path):
    cfg = make_config(tmp_path)
    seed_segments(cfg.data_dir, [list(range(1, 51)), list(range(51, 101))])
    store = Store.open(cfg, CLOCK)
    try:
        store.apply_ack(57)
        s = store.snapshot()
        assert s.backlog == 43               # 100 - 57
        assert s.oldest_ts == 1000.0 + 58    # not the segment's first frame
        assert s.acked_seq == 57
        names = sorted(p.name for p in cfg.data_dir.glob("segment-*.log"))
        assert names == ["segment-000002.log"]  # seg1 fully acked -> removed
        assert s.bytes_used > 0              # partially acked seg kept
        assert s.reclaimable_bytes == 0
    finally:
        store.close()


def test_backlog_zero_oldest_null_when_fully_acked(tmp_path):
    cfg = make_config(tmp_path)
    seed_segments(cfg.data_dir, [[1, 2, 3], [4, 5, 6]])
    store = Store.open(cfg, CLOCK)
    try:
        store.apply_ack(6)
        s = store.snapshot()
        assert s.backlog == 0
        assert s.oldest_ts is None
    finally:
        store.close()


def test_next_batch_starts_after_watermark_and_spans_segments(tmp_path):
    cfg = make_config(tmp_path, batch_max_records=100)
    seed_segments(cfg.data_dir, [list(range(1, 31)), list(range(31, 61)),
                                 list(range(61, 66))])
    store = Store.open(cfg, CLOCK)
    try:
        store.apply_ack(20)  # ack into seg1; seg1 gets reaped
        batch = store.next_batch()
        frames = json.loads(batch.body)["frames"]
        # seg2 (closed) read in full; seg3 is the active segment -> not read
        assert [f["seq"] for f in frames] == list(range(21, 61))
        assert batch.first_seq == 21
        assert batch.batch_id == f"b{21:012d}-{60:012d}"
    finally:
        store.close()


def test_next_batch_respects_record_and_byte_caps(tmp_path):
    cfg = make_config(tmp_path, batch_max_records=10, batch_max_bytes=10_000)
    # trailing active segment so the 50-frame segment is closed
    seed_segments(cfg.data_dir, [list(range(1, 51)), [51]])
    store = Store.open(cfg, CLOCK)
    try:
        batch = store.next_batch()
        assert len(json.loads(batch.body)["frames"]) == 10
    finally:
        store.close()


def test_next_batch_none_for_active_segment_only(tmp_path):
    # The single (active) segment is not uploaded until rotated.
    cfg = make_config(tmp_path)
    seed_segments(cfg.data_dir, [list(range(1, 11))])
    store = Store.open(cfg, CLOCK)
    try:
        assert store.next_batch() is None
    finally:
        store.close()


def test_apply_ack_is_monotonic_and_durable_state(tmp_path):
    cfg = make_config(tmp_path)
    seed_segments(cfg.data_dir, [[1, 2]])
    store = Store.open(cfg, CLOCK)
    try:
        store.apply_ack(1)
        store.apply_ack(1)  # stale/duplicate ACK ignored silently
        assert store.snapshot().acked_seq == 1
        state = json.loads((cfg.data_dir / "state.json").read_text())
        assert state["acked_seq"] == 1 and "crc32" in state
    finally:
        store.close()
    # Reopen: watermark survives.
    store2 = Store.open(cfg, CLOCK)
    try:
        assert store2.snapshot().acked_seq == 1
    finally:
        store2.close()


def test_size_rotation_between_groups(tmp_path):
    clock = FakeClock()
    cfg = make_config(tmp_path, segment_bytes=600, segment_max_age_s=30.0,
                      group_max_records=5)
    store = Store.open(cfg, clock)
    w = WriteWorker(cfg, store, clock)
    w.start()
    try:
        for i in range(1, 81):
            w.submit({"n": i, "pad": "x" * 40})
    finally:
        w.shutdown()
        store.close()
    files = sorted(cfg.data_dir.glob("segment-*.log"))
    assert len(files) >= 2
    # Recovery sees all frames with dense seq.
    store2 = Store.open(cfg, clock)
    try:
        assert store2.snapshot().max_seq == 80
    finally:
        store2.close()


def test_age_rotation_on_idle(tmp_path):
    cfg = make_config(tmp_path, segment_max_age_s=0.02)
    store = Store.open(cfg, CLOCK)
    w = WriteWorker(cfg, store, CLOCK)
    w.start()
    try:
        w.submit({"n": 1})
        deadline = time.time() + 3
        while time.time() < deadline:
            files = list(cfg.data_dir.glob("segment-*.log"))
            if len(files) >= 2:
                break
            time.sleep(0.01)
        assert len(list(cfg.data_dir.glob("segment-*.log"))) >= 2
    finally:
        w.shutdown()
        store.close()


def test_quota_blocks_producer_until_ack_reclaims(tmp_path):
    cfg = make_config(tmp_path, quota_bytes=900, segment_max_age_s=0.0,
                      group_max_wait_s=0.002)
    store = Store.open(cfg, CLOCK)
    w = WriteWorker(cfg, store, CLOCK)
    w.start()
    unblocked = threading.Event()
    result = {}

    def producer():
        i = 1
        try:
            while not unblocked.is_set():
                w.submit({"n": i, "pad": "y" * 30})
                result["last"] = i
                i += 1
        except Exception as exc:  # noqa: BLE001
            result["exc"] = exc

    t = threading.Thread(target=producer, daemon=True)
    t.start()
    try:
        time.sleep(0.3)
        assert t.is_alive()  # blocked on quota, cloud offline
        max_before = store.snapshot().max_seq
        # Cloud catches up: ACK everything -> closed segments reclaimed.
        deadline = time.time() + 1.0
        while time.time() < deadline:
            s = store.snapshot()
            if s.max_seq > 0:
                store.apply_ack(s.max_seq)
            if not t.is_alive():
                break
            time.sleep(0.02)
        unblocked.set()
        t.join(timeout=5)
        assert "exc" not in result
        assert result["last"] >= max_before
    finally:
        unblocked.set()
        t.join(timeout=5)
        w.shutdown()
        store.close()
