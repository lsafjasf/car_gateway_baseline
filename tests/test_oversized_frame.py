"""Oversized frames must not permanently block the upload/reclaim pipeline."""

from __future__ import annotations

import json
import time

from conftest import make_config
from gateway.app import Gateway
from gateway.cloud_mock import serve_in_thread
from gateway.models import FrameTooLargeError
from gateway.segments import Store
from gateway.writer import WriteWorker


def test_oversized_frame_forms_singleton_batch(tmp_path):
    # batch_max_bytes is tiny: normal frames already exceed it.
    cfg = make_config(tmp_path, batch_max_records=50, batch_max_bytes=50)
    from conftest import seed_segments
    seed_segments(cfg.data_dir, [[1, 2, 3], [4]])  # seg1 closed, seg2 active
    store = Store.open(cfg, time)
    try:
        b1 = store.next_batch()
        assert len(json.loads(b1.body)["frames"]) == 1  # at least one per trip
        assert b1.first_seq == 1
        store.apply_ack(1)
        b2 = store.next_batch()
        assert [f["seq"] for f in json.loads(b2.body)["frames"]] == [2]
    finally:
        store.close()


def test_oversized_frame_uploaded_and_reclaimed_e2e(tmp_path):
    cloud = serve_in_thread(tmp_path / "cloud")
    cfg = make_config(
        tmp_path, cloud_url=cloud.url, api_port=0,
        batch_max_bytes=200, batch_max_records=10,
        segment_max_age_s=0.1, group_max_wait_s=0.005,
        backoff_initial_s=0.05, backoff_max_s=0.5, http_timeout_s=5.0,
    )
    gw = Gateway(cfg).start()
    try:
        gw.submit({"n": 1, "blob": "x" * 5000})  # one frame >> batch_max_bytes
        gw.submit({"n": 2, "blob": "y" * 5000})
        gw.submit({"n": 3})
        deadline = time.time() + 20
        while time.time() < deadline:
            s = gw.store.snapshot()
            if s.max_seq == 3 and s.backlog == 0:
                break
            time.sleep(0.02)
        s = gw.store.snapshot()
        assert (s.max_seq, s.backlog) == (3, 0)
    finally:
        gw.stop()
        cloud.shutdown()
        cloud.server_close()
    assert sorted(cloud.cloud.ingested) == [1, 2, 3]
    assert len(cloud.cloud.ingested[1]["payload"]["blob"]) == 5000


def test_frame_larger_than_quota_rejected_at_entry(tmp_path):
    cfg = make_config(tmp_path, quota_bytes=2048)
    store = Store.open(cfg, time)
    w = WriteWorker(cfg, store, time)
    w.start()
    try:
        try:
            w.submit({"blob": "z" * 5000})  # raw+overhead > 2048 quota
            raised = False
        except FrameTooLargeError:
            raised = True
        assert raised
        # Pipeline still alive after rejecting the giant frame.
        assert w.submit({"ok": True}) == 1
        # Quota accounting untouched by the rejected frame.
        assert store.snapshot().backlog == 1
    finally:
        w.shutdown()
        store.close()
