"""Mock cloud contract: dedupe watermark, idempotent retries, 409 on gaps,
durable watermark across cloud restart, injected failures."""

from __future__ import annotations

import json

from gateway.cloud_mock import MockCloud, serve_in_thread


def post(cloud: MockCloud, first_seq, frames_raw):
    body = json.dumps({"batch_id": "x", "first_seq": first_seq,
                       "frames": frames_raw}).encode()
    return cloud.handle_post(body)


def frame(seq):
    return {"seq": seq, "ts": 1000.0 + seq, "payload": {"n": seq}}


def test_happy_path_advances_watermark(tmp_path):
    cloud = MockCloud(tmp_path)
    code, resp = post(cloud, 1, [frame(1), frame(2)])
    assert (code, resp) == (200, {"acked_seq": 2})
    assert cloud.watermark == 2


def test_duplicate_batch_after_lost_ack_is_idempotent(tmp_path):
    cloud = MockCloud(tmp_path)
    body = json.dumps({"batch_id": "b1", "first_seq": 1,
                       "frames": [frame(1), frame(2)]}).encode()
    assert cloud.handle_post(body) == (200, {"acked_seq": 2})
    # ACK lost: client resends the SAME bytes.
    assert cloud.handle_post(body) == (200, {"acked_seq": 2})
    assert cloud.watermark == 2
    assert sorted(cloud.ingested) == [1, 2]  # ingested exactly once


def test_retry_overlapping_partial_watermark(tmp_path):
    # Cloud is ahead of what the client thinks; old frames stripped.
    cloud = MockCloud(tmp_path)
    post(cloud, 1, [frame(1), frame(2), frame(3)])
    code, resp = post(cloud, 3, [frame(3), frame(4)])
    assert code == 200 and resp == {"acked_seq": 4}
    assert sorted(cloud.ingested) == [1, 2, 3, 4]


def test_seq_gap_rejected_409(tmp_path):
    cloud = MockCloud(tmp_path)
    code, resp = post(cloud, 1, [frame(1), frame(5)])
    # Whole batch rejected atomically: watermark never advanced.
    assert code == 409 and resp["watermark"] == 0
    assert cloud.watermark == 0
    assert cloud.ingested == {}


def test_injected_failures_then_success(tmp_path):
    cloud = MockCloud(tmp_path)
    cloud.fail_next(2, code=500)
    body = json.dumps({"batch_id": "b1", "first_seq": 1,
                       "frames": [frame(1)]}).encode()
    assert cloud.handle_post(body)[0] == 500
    assert cloud.handle_post(body)[0] == 500
    assert cloud.handle_post(body) == (200, {"acked_seq": 1})


def test_watermark_persists_across_cloud_restart(tmp_path):
    cloud = MockCloud(tmp_path)
    post(cloud, 1, [frame(1), frame(2)])
    cloud2 = MockCloud(tmp_path)  # power-cycle: reload from disk
    assert cloud2.watermark == 2
    # Duplicates after restart are ignored -> no double ingest.
    post(cloud2, 1, [frame(1), frame(2), frame(3)])
    assert sorted(cloud2.ingested) == [1, 2, 3]


def test_http_roundtrip_with_drop(tmp_path):
    server = serve_in_thread(tmp_path / "cloud")
    import http.client
    try:
        host, port = server.server_address[:2]
        body = json.dumps({"batch_id": "b1", "first_seq": 1,
                           "frames": [frame(1)]}).encode()
        server.cloud.drop_next(1)
        conn = http.client.HTTPConnection(host, port, timeout=5)
        conn.request("POST", "/batches", body=body)
        try:
            conn.getresponse()
            raised = False
        except OSError:
            raised = True
        conn.close()
        assert raised  # connection dropped to simulate lost ACK
        # Resend: deduped.
        conn = http.client.HTTPConnection(host, port, timeout=5)
        conn.request("POST", "/batches", body=body)
        resp = conn.getresponse()
        assert resp.status == 200
        assert json.loads(resp.read()) == {"acked_seq": 1}
        conn.close()
    finally:
        server.shutdown()
        server.server_close()
