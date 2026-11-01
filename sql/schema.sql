CREATE TABLE IF NOT EXISTS web_requests (
    record_id       VARCHAR(200) PRIMARY KEY,
    source_id       VARCHAR(100) NOT NULL,
    source_line     BIGINT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    host            TEXT NOT NULL,
    http_method     VARCHAR(32) NOT NULL,
    url_path        TEXT NOT NULL,
    protocol        VARCHAR(32),
    status_code     INTEGER NOT NULL CHECK (status_code BETWEEN 100 AND 599),
    bytes_sent      BIGINT CHECK (bytes_sent IS NULL OR bytes_sent >= 0),
    UNIQUE (source_id, source_line)
);

CREATE INDEX IF NOT EXISTS ix_web_requests_timestamp ON web_requests (event_timestamp);
CREATE INDEX IF NOT EXISTS ix_web_requests_path ON web_requests (url_path);
CREATE INDEX IF NOT EXISTS ix_web_requests_status ON web_requests (status_code);
