-- Create materialized view table (valid in SQLite)
CREATE TABLE daily_steps_user AS
    SELECT nickname, city, day, sum(steps) AS total_steps
    FROM activity_sessions
    GROUP BY nickname, city, day;

-- Create empty delta table with schema from activity_sessions (fixed for SQLite)
CREATE TABLE IF NOT EXISTS delta_activity_sessions AS
SELECT *,
       1               AS _duckdb_ivm_multiplicity, -- Changed from true (SQLite uses 1/0 for booleans)
       datetime('now') AS _duckdb_ivm_timestamp     -- Changed from now
FROM activity_sessions LIMIT 0;

-- Create empty delta table for the materialized view (fixed for SQLite)
CREATE TABLE IF NOT EXISTS delta_daily_steps_user AS
SELECT *,
       1 AS _duckdb_ivm_multiplicity -- Changed from true
FROM daily_steps_user LIMIT 0;

CREATE TRIGGER after_activity_sessions_insert
    AFTER INSERT
    ON activity_sessions
    FOR EACH ROW
BEGIN
    INSERT INTO delta_activity_sessions (username,
                                         nickname,
                                         city,
                                         day,
                                         start_time,
                                         end_time,
                                         steps,
                                         heartbeat_rate,
                                         _duckdb_ivm_multiplicity,
                                         _duckdb_ivm_timestamp)
    VALUES (NEW.username,
            NEW.nickname,
            NEW.city,
            NEW.day,
            NEW.start_time,
            NEW.end_time,
            NEW.steps,
            NEW.heartbeat_rate,
            1, -- Default multiplicity (1 for true)
            datetime('now') -- Current timestamp
           );
END;