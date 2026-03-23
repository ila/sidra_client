SET memory_limit = '16GB';
SET threads TO 16;

ATTACH IF NOT EXISTS 'dbname=sidra user=sidra password=test host=localhost' AS sidra_client (TYPE postgres);
ATTACH 'sidra_parser.db' AS sidra_parser;

-- Create timestamp tracking tables if they don't exist
CREATE TABLE IF NOT EXISTS sidra_parser._duckdb_ivm_views (
                                                              view_name VARCHAR PRIMARY KEY,
                                                              sql_string VARCHAR,
                                                              type TINYINT,
                                                              last_update TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sidra_parser._duckdb_ivm_delta_tables (
                                                                     view_name VARCHAR,
                                                                     table_name VARCHAR,
                                                                     last_update TIMESTAMP,
                                                                     PRIMARY KEY(view_name, table_name)
    );

-- Initialize timestamp tracking entries
INSERT OR REPLACE INTO sidra_parser._duckdb_ivm_views VALUES
    ('daily_steps', 'select city, day, sum(total_steps) as total_steps from daily_steps_user group by city, day', 0, NOW()),
    ('leaderboard', 'select nickname, sum(total_steps) as total_steps from daily_steps_user where nickname is not null group by nickname', 0, NOW());

INSERT OR REPLACE INTO sidra_parser._duckdb_ivm_delta_tables VALUES
    ('daily_steps', '_delta_daily_steps', NOW()),
    ('leaderboard', '_delta_leaderboard', NOW());

-- Step 1: Extract data within TTL window
WITH stats AS (
    SELECT sidra_window, sidra_ttl FROM sidra_parser.sidra_view_constraints
    WHERE view_name = 'daily_steps_user'),
     current_window AS (
         SELECT sidra_window FROM sidra_parser.sidra_current_window
         WHERE view_name = 'sidra_staging_view_daily_steps_user'),
     threshold_window AS (
         SELECT cw.sidra_window - (s.sidra_ttl / s.sidra_window) AS expired_window
         FROM current_window cw, stats s)
INSERT INTO sidra_client._delta_sidra_staging_view_daily_steps_user BY NAME
SELECT nickname, city, day, total_steps, sidra_window, client_id,
    1 AS _duckdb_ivm_multiplicity,
    NOW() AS _duckdb_ivm_timestamp
FROM sidra_client.sidra_staging_view_daily_steps_user
WHERE sidra_window > (SELECT expired_window FROM threshold_window);

-- Step 2: Compute delta for daily_steps (city-level) with timestamp check and min aggregation
INSERT INTO _delta_daily_steps BY NAME
SELECT city, day,
    SUM(total_steps) AS total_steps,
    _duckdb_ivm_multiplicity
FROM sidra_client._delta_sidra_staging_view_daily_steps_user
WHERE (_duckdb_ivm_timestamp IS NULL OR _duckdb_ivm_timestamp >= (
    SELECT COALESCE(last_update, '1970-01-01')
    FROM sidra_parser._duckdb_ivm_delta_tables
    WHERE view_name = 'daily_steps' AND table_name = '_delta_daily_steps'
    ))
  AND (city, day, sidra_window) IN (
    SELECT city, day, sidra_window
    FROM (
    SELECT city, day, sidra_window, COUNT(DISTINCT client_id) as client_count
    FROM sidra_client._delta_sidra_staging_view_daily_steps_user
    GROUP BY city, day, sidra_window
    ) min_agg_check
    WHERE client_count >= 50  -- Minimum client threshold per city/day/window
    )
GROUP BY city, day, _duckdb_ivm_multiplicity;

-- Step 3: Update daily_steps materialized view
INSERT OR REPLACE INTO daily_steps BY NAME
WITH ivm_cte AS (
    SELECT city, day,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = 0 THEN -total_steps ELSE total_steps END) AS total_steps
    FROM _delta_daily_steps
    GROUP BY city, day
)
SELECT d.city, d.day,
       COALESCE(existing.total_steps, 0) + d.total_steps AS total_steps,
       COALESCE(existing.responsiveness, 0) AS responsiveness,
       COALESCE(existing.completeness, 0) AS completeness,
       COALESCE(existing.buffer_size, 0) AS buffer_size
FROM ivm_cte AS d
         LEFT JOIN daily_steps AS existing
                   ON existing.city = d.city AND existing.day = d.day;

-- Update timestamp tracking for daily_steps (refreshed every 4h)
UPDATE sidra_parser._duckdb_ivm_delta_tables
SET last_update = NOW()
WHERE view_name = 'daily_steps' AND table_name = '_delta_daily_steps';

-- Step 4: Compute delta for leaderboard
INSERT INTO _delta_leaderboard BY NAME
SELECT nickname, total_steps, _duckdb_ivm_multiplicity
FROM sidra_client._delta_sidra_staging_view_daily_steps_user
WHERE nickname IS NOT NULL;

-- Step 5: Update leaderboard materialized view
INSERT OR REPLACE INTO leaderboard BY NAME
WITH ivm_cte AS (
    SELECT nickname,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = 0 THEN -total_steps ELSE total_steps END) AS total_steps
    FROM _delta_leaderboard
    GROUP BY nickname
)
SELECT d.nickname,
       COALESCE(existing.total_steps, 0) + d.total_steps AS total_steps,
       COALESCE(existing.responsiveness, 0) AS responsiveness,
       COALESCE(existing.completeness, 0) AS completeness,
       COALESCE(existing.buffer_size, 0) AS buffer_size
FROM ivm_cte AS d
         LEFT JOIN leaderboard AS existing ON existing.nickname = d.nickname
WHERE COALESCE(existing.total_steps, 0) + d.total_steps > 0;

-- Update timestamp tracking for leaderboard (refreshed daily)
UPDATE sidra_parser._duckdb_ivm_delta_tables
SET last_update = NOW()
WHERE view_name = 'leaderboard' AND table_name = '_delta_leaderboard';

-- Step 6: Update responsiveness for leaderboard (per nickname)
WITH distinct_clients AS (
    SELECT nickname, COUNT(DISTINCT client_id) AS client_cnt
    FROM sidra_client._delta_sidra_staging_view_daily_steps_user
    WHERE nickname IS NOT NULL
    GROUP BY nickname
),
     total_clients AS (
         SELECT COUNT(DISTINCT id) AS total_client_cnt FROM sidra_parser.sidra_clients
     ),
     percentages AS (
         SELECT d.nickname, LEAST((d.client_cnt::DOUBLE / t.total_client_cnt) * 100, 100) AS perc
         FROM distinct_clients d, total_clients t
     )
UPDATE leaderboard
SET responsiveness = responsiveness + p.perc
    FROM percentages p
WHERE leaderboard.nickname = p.nickname;

-- Step 7: Update responsiveness for daily_steps (per city, day)
WITH distinct_clients_per_window AS (
    SELECT sidra_window, COUNT(DISTINCT client_id) AS window_client_count
    FROM sidra_client._delta_sidra_staging_view_daily_steps_user
    GROUP BY sidra_window),
     total_clients AS (
         SELECT COUNT(DISTINCT id) AS total_client_count
         FROM sidra_parser.sidra_clients),
     percentages AS (
         SELECT d.sidra_window,
                LEAST((d.window_client_count::DOUBLE / t.total_client_count) * 100, 100) AS percentage
         FROM distinct_clients_per_window d, total_clients t)
UPDATE daily_steps
SET responsiveness = responsiveness + p.percentage
    FROM percentages p
WHERE daily_steps.city IN (
    SELECT DISTINCT city
    FROM sidra_client._delta_sidra_staging_view_daily_steps_user
    WHERE sidra_window = p.sidra_window
    );

-- Step 8: Update completeness for leaderboard (per nickname)
WITH stats AS (
    SELECT sidra_window, sidra_ttl FROM sidra_parser.sidra_view_constraints
    WHERE view_name = 'daily_steps_user'),
     current_window AS (
         SELECT sidra_window FROM sidra_parser.sidra_current_window
         WHERE view_name = 'sidra_staging_view_daily_steps_user'),
     threshold_window AS (
         SELECT cw.sidra_window - (s.sidra_ttl / s.sidra_window) AS expired_window
         FROM current_window cw, stats s),
     discarded_clients AS (
         SELECT nickname, COUNT(DISTINCT client_id) AS discarded
         FROM sidra_client.sidra_staging_view_daily_steps_user
         WHERE nickname IS NOT NULL
           AND sidra_window <= (SELECT expired_window FROM threshold_window)
         GROUP BY nickname
     ),
     kept_clients AS (
         SELECT nickname, COUNT(DISTINCT client_id) AS kept
         FROM sidra_client._delta_sidra_staging_view_daily_steps_user
         WHERE nickname IS NOT NULL
         GROUP BY nickname
     ),
     combined AS (
         SELECT COALESCE(d.nickname, k.nickname) AS nickname,
                COALESCE(d.discarded, 0) AS discarded,
                COALESCE(k.kept, 0) AS kept
         FROM discarded_clients d
                  FULL OUTER JOIN kept_clients k ON d.nickname = k.nickname
     ),
     completeness_metric AS (
         SELECT nickname,
                CASE WHEN (discarded + kept) > 0
                         THEN (kept::DECIMAL / (discarded + kept)) * 100
                     ELSE 100 END AS completeness
         FROM combined
     )
UPDATE leaderboard
SET completeness = c.completeness
    FROM completeness_metric c
WHERE leaderboard.nickname = c.nickname;

-- Step 9: Update completeness for daily_steps (per city, day)
WITH stats AS (
    SELECT sidra_window, sidra_ttl FROM sidra_parser.sidra_view_constraints
    WHERE view_name = 'daily_steps_user'),
     current_window AS (
         SELECT sidra_window FROM sidra_parser.sidra_current_window
         WHERE view_name = 'sidra_staging_view_daily_steps_user'),
     threshold_window AS (
         SELECT cw.sidra_window - (s.sidra_ttl / s.sidra_window) AS expired_window
         FROM current_window cw, stats s),
     to_discard AS (
         SELECT sidra_window, COUNT(*) AS discarded_count
         FROM sidra_client.sidra_staging_view_daily_steps_user
         WHERE sidra_window <= (SELECT expired_window FROM threshold_window)
         GROUP BY sidra_window),
     to_keep AS (
         SELECT sidra_window, COUNT(*) AS kept_count
         FROM sidra_client._delta_sidra_staging_view_daily_steps_user
         GROUP BY sidra_window),
     combined AS (
         SELECT
             COALESCE(d.sidra_window, k.sidra_window) AS sidra_window,
             COALESCE(d.discarded_count, 0) AS discarded,
             COALESCE(k.kept_count, 0) AS kept
         FROM to_discard d
                  FULL OUTER JOIN to_keep k ON d.sidra_window = k.sidra_window),
     discard_stats AS (
         SELECT sidra_window, discarded, kept,
                CASE WHEN (discarded + kept) > 0
                         THEN (kept::DECIMAL / (discarded + kept)) * 100
                     ELSE 100 END AS discard_percentage
         FROM combined)
UPDATE daily_steps
SET completeness = ds.discard_percentage
    FROM discard_stats ds
WHERE daily_steps.city IN (
    SELECT DISTINCT city
    FROM sidra_client._delta_sidra_staging_view_daily_steps_user
    WHERE sidra_window = ds.sidra_window
    );

-- Step 10: Update buffer size for leaderboard (per nickname)
WITH buffer AS (
    SELECT nickname, COUNT(*) AS buffer_count
    FROM sidra_client.sidra_staging_view_daily_steps_user
    WHERE nickname IS NOT NULL
    GROUP BY nickname
),
     processed AS (
         SELECT nickname, COUNT(*) AS processed_count
         FROM sidra_client._delta_sidra_staging_view_daily_steps_user
         WHERE nickname IS NOT NULL
         GROUP BY nickname
     ),
     combined AS (
         SELECT COALESCE(b.nickname, p.nickname) AS nickname,
                COALESCE(b.buffer_count, 0) AS buffer,
                COALESCE(p.processed_count, 0) AS processed
         FROM buffer b
                  FULL OUTER JOIN processed p ON b.nickname = p.nickname
     ),
     stats AS (
         SELECT nickname,
                CASE WHEN (buffer + processed) > 0
                         THEN (buffer::DECIMAL / (buffer + processed)) * 100
                     ELSE 0 END AS buffer_size
         FROM combined
     )
UPDATE leaderboard
SET buffer_size = stats.buffer_size
    FROM stats
WHERE leaderboard.nickname = stats.nickname;

-- Step 11: Update buffer size for daily_steps (per city, day)
WITH buffer_counts AS (
    SELECT sidra_window, COUNT(*) AS buffer_count
    FROM sidra_client.sidra_staging_view_daily_steps_user
    GROUP BY sidra_window),
     delta_counts AS (
         SELECT sidra_window, COUNT(*) AS delta_count
         FROM sidra_client._delta_sidra_staging_view_daily_steps_user
         GROUP BY sidra_window),
     combined AS (
         SELECT
             COALESCE(b.sidra_window, d.sidra_window) AS sidra_window,
             COALESCE(b.buffer_count, 0) AS buffer,
             COALESCE(d.delta_count, 0) AS processed
         FROM buffer_counts b
                  FULL OUTER JOIN delta_counts d ON b.sidra_window = d.sidra_window),
     buffer_stats AS (
         SELECT sidra_window, buffer, processed,
                CASE WHEN (buffer + processed) > 0
                         THEN (buffer::DECIMAL / (buffer + processed)) * 100
                     ELSE 0 END AS buffer_percentage
         FROM combined)
UPDATE daily_steps
SET buffer_size = bs.buffer_percentage
    FROM buffer_stats bs
WHERE daily_steps.city IN (
    SELECT DISTINCT city
    FROM sidra_client._delta_sidra_staging_view_daily_steps_user
    WHERE sidra_window = bs.sidra_window
    );

-- Step 12: TTL cleanup in staging view (expired records)
WITH stats AS (
    SELECT sidra_window, sidra_ttl FROM sidra_parser.sidra_view_constraints
    WHERE view_name = 'daily_steps_user'),
     current_window AS (
         SELECT sidra_window FROM sidra_parser.sidra_current_window
         WHERE view_name = 'sidra_staging_view_daily_steps_user'),
     threshold_window AS (
         SELECT cw.sidra_window - (s.sidra_ttl / s.sidra_window) AS expired_window
         FROM current_window cw, stats s)
DELETE FROM sidra_client.sidra_staging_view_daily_steps_user
WHERE sidra_window <= (SELECT expired_window FROM threshold_window);

-- Step 13: TTL cleanup in delta
WITH stats AS (
    SELECT sidra_window, sidra_ttl FROM sidra_parser.sidra_view_constraints
    WHERE view_name = 'daily_steps_user'),
     current_window AS (
         SELECT sidra_window FROM sidra_parser.sidra_current_window
         WHERE view_name = 'sidra_staging_view_daily_steps_user'),
     threshold_window AS (
         SELECT cw.sidra_window - (s.sidra_ttl / s.sidra_window) AS expired_window
         FROM current_window cw, stats s)
DELETE FROM sidra_client._delta_sidra_staging_view_daily_steps_user
WHERE sidra_window <= (SELECT expired_window FROM threshold_window);

-- Step 14: Remove inactive clients
DELETE FROM sidra_parser.sidra_clients
WHERE last_update < today() - INTERVAL 7 DAY;

-- Step 15: Cleanup zero aggregates
DELETE FROM daily_steps WHERE total_steps = 0;
DELETE FROM leaderboard WHERE total_steps = 0;

-- Step 16: Cleanup delta tables
DELETE FROM sidra_client._delta_sidra_staging_view_daily_steps_user;
DELETE FROM _delta_daily_steps;
DELETE FROM _delta_leaderboard;