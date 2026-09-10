"""Vehicle data acquisition gateway (stdlib-only).

Modules:
    config     -- configuration
    models     -- data types and exceptions
    durability -- fsync / atomic file replace helpers
    segments   -- on-flash segment store with crash recovery
    writer     -- multi-producer group-commit write thread
    uploader   -- batched cloud upload thread with at-least-once ACK
    cloud_mock -- test/demo cloud endpoint with persistent watermark
    api        -- local HTTP stats endpoint
    app        -- assembly / lifecycle
"""
