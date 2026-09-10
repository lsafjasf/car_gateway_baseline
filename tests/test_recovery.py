"""Recovery: torn tails, corruption detection, state.json, restart."""

from __future__ import annotations

import json
import zlib

import pytest

from conftest import CLOCK, append_raw, make_config, seed_segments, write_record_line
from gateway.models import DataCorruptionError
from gateway.segments import Store


def test_torn_final_line_is_truncated(tmp_path):
    cfg = make_config(tmp_path)
    seed_segments(cfg.data_dir, [[1, 2, 3], [4, 5]], torn=True)
    store = Store.open(cfg, CLOCK)
    try:
        assert store.snapshot().max_seq == 5
        # torn bytes removed on disk, ending exactly at last newline
        data = (cfg.data_dir / "segment-000002.log").read_bytes()
        assert data.endswith(b"}\n")
        assert b'"seq": 999' not in data  # torn line removed (spaces are intentional)
    finally:
        store.close()


def test_garbage_after_good_records_in_highest_segment_truncated(tmp_path):
    cfg = make_config(tmp_path)
    good = b"".join(write_record_line(i, 1.0 + i, {"n": i}) for i in range(1, 4))
    append_raw(cfg.data_dir, "segment-000001.log", good + b'{"seq":4}\x00garbage')
    store = Store.open(cfg, CLOCK)
    try:
        assert store.snapshot().max_seq == 3
        text = (cfg.data_dir / "segment-000001.log").read_bytes()
        assert text == good
    finally:
        store.close()


def test_corrupt_record_in_closed_segment_refuses_start(tmp_path):
    cfg = make_config(tmp_path)
    seed_segments(cfg.data_dir, [[1, 2, 3], [4, 5]])
    p = cfg.data_dir / "segment-000001.log"
    data = p.read_bytes()
    p.write_bytes(data[:20] + b"XXXX" + data[24:])  # smash first record
    with pytest.raises(DataCorruptionError):
        Store.open(cfg, CLOCK)


def test_seq_gap_between_segments_refuses_start(tmp_path):
    cfg = make_config(tmp_path)
    seed_segments(cfg.data_dir, [[1, 2], [5, 6]])
    with pytest.raises(DataCorruptionError):
        Store.open(cfg, CLOCK)


def test_seq_gap_inside_highest_segment_truncates_at_gap(tmp_path):
    # Dense 1,2,3 then 5 (missing 4): the 5 line is torn off, max_seq stays 3.
    cfg = make_config(tmp_path)
    lines = b"".join(write_record_line(i, 1.0 + i, {"n": i})
                     for i in (1, 2, 3, 5))
    append_raw(cfg.data_dir, "segment-000001.log", lines)
    store = Store.open(cfg, CLOCK)
    try:
        assert store.snapshot().max_seq == 3
    finally:
        store.close()


def test_recover_reclaims_fully_acked_closed_segments(tmp_path):
    cfg = make_config(tmp_path)
    seed_segments(cfg.data_dir, [[1, 2], [3, 4], [5]])
    state = {"acked_seq": 4}
    canonical = json.dumps({"acked_seq": 4}, sort_keys=True).encode()
    state["crc32"] = zlib.crc32(canonical) & 0xFFFFFFFF
    (cfg.data_dir / "state.json").write_text(json.dumps(state))
    store = Store.open(cfg, CLOCK)
    try:
        names = sorted(p.name for p in cfg.data_dir.glob("segment-*.log"))
        assert names == ["segment-000003.log"]  # active segment never deleted
        assert store.snapshot().backlog == 1
    finally:
        store.close()


def test_active_segment_fully_acked_is_not_deleted(tmp_path):
    cfg = make_config(tmp_path)
    seed_segments(cfg.data_dir, [[1, 2]])  # only (highest) segment
    state = {"acked_seq": 2}
    canonical = json.dumps({"acked_seq": 2}, sort_keys=True).encode()
    state["crc32"] = zlib.crc32(canonical) & 0xFFFFFFFF
    (cfg.data_dir / "state.json").write_text(json.dumps(state))
    store = Store.open(cfg, CLOCK)
    try:
        assert (cfg.data_dir / "segment-000001.log").exists()
        assert store.snapshot().backlog == 0
    finally:
        store.close()


def test_bad_state_json_resets_watermark_and_keeps_data(tmp_path):
    cfg = make_config(tmp_path)
    seed_segments(cfg.data_dir, [[1, 2, 3]])
    (cfg.data_dir / "state.json").write_text("{not json")
    store = Store.open(cfg, CLOCK)
    try:
        s = store.snapshot()
        assert s.acked_seq == 0
        assert s.max_seq == 3
        assert s.backlog == 3
        assert store.warnings  # surfaced loudly
    finally:
        store.close()


def test_state_acked_above_max_seq_resets_to_zero(tmp_path):
    cfg = make_config(tmp_path)
    seed_segments(cfg.data_dir, [[1, 2]])
    for bad in (10**12,):
        state = {"acked_seq": bad}
        canonical = json.dumps({"acked_seq": bad}, sort_keys=True).encode()
        state["crc32"] = zlib.crc32(canonical) & 0xFFFFFFFF
        (cfg.data_dir / "state.json").write_text(json.dumps(state))
        store = Store.open(cfg, CLOCK)
        try:
            assert store.snapshot().acked_seq == 0
            assert (cfg.data_dir / "segment-000001.log").exists()
        finally:
            store.close()


def test_state_crc_mismatch_ignores_state(tmp_path):
    cfg = make_config(tmp_path)
    seed_segments(cfg.data_dir, [[1, 2]])
    (cfg.data_dir / "state.json").write_text(
        json.dumps({"acked_seq": 2, "crc32": 12345}))
    store = Store.open(cfg, CLOCK)
    try:
        assert store.snapshot().acked_seq == 0
    finally:
        store.close()


def test_stale_tmp_files_removed_on_open(tmp_path):
    cfg = make_config(tmp_path)
    cfg.data_dir.mkdir(parents=True)
    (cfg.data_dir / "state.json.abc.tmp").write_bytes(b"x")
    seed_segments(cfg.data_dir, [[1]])
    store = Store.open(cfg, CLOCK)
    try:
        assert not list(cfg.data_dir.glob("*.tmp"))
    finally:
        store.close()


def test_restart_continues_seq_dense_and_keeps_unacked(tmp_path):
    from gateway.writer import WriteWorker
    cfg = make_config(tmp_path, segment_max_age_s=0.0)
    seed_segments(cfg.data_dir, [[1, 2, 3]])
    store = Store.open(cfg, CLOCK)
    try:
        w = WriteWorker(cfg, store, CLOCK)
        w.start()
        seq = w.submit({"n": "after-restart"})
        assert seq == 4
        w.shutdown()
        # Active segments are uploaded only after rotation.
        store.rotate_if_due()
    finally:
        store.close()
    store2 = Store.open(cfg, CLOCK)
    try:
        assert store2.snapshot().max_seq == 4
        batch = store2.next_batch()
        assert [f["seq"] for f in json.loads(batch.body)["frames"]] == [1, 2, 3, 4]
    finally:
        store2.close()
