CREATE TABLE IF NOT EXISTS bot_error_logs (
    id BIGSERIAL PRIMARY KEY,
    log_path TEXT NOT NULL,
    byte_offset BIGINT NOT NULL,
    parsed_at TIMESTAMPTZ,
    level TEXT,
    message TEXT NOT NULL,
    raw_line TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bot_log_offsets (
    log_path TEXT PRIMARY KEY,
    byte_offset BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bot_error_logs_created_at ON bot_error_logs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_bot_error_logs_parsed_at ON bot_error_logs (parsed_at DESC);
CREATE INDEX IF NOT EXISTS idx_bot_error_logs_level ON bot_error_logs (level);
CREATE INDEX IF NOT EXISTS idx_bot_error_logs_path_offset ON bot_error_logs (log_path, byte_offset);
