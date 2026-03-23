CREATE TABLE IF NOT EXISTS activity_sessions (
    username TEXT,
    nickname TEXT,
    city TEXT,
    day TEXT, -- ISO 8601 date string
    start_time TEXT, -- ISO 8601 datetime string
    end_time TEXT,
    steps INTEGER,
    heartbeat_rate INTEGER
);
