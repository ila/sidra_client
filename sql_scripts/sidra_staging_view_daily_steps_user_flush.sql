SET memory_limit = '16GB';
SET threads TO 16;

ATTACH IF NOT EXISTS 'dbname=sidra user=sidra password=test host=localhost' AS sidra_client (TYPE postgres);
ATTACH 'sidra_parser.db' AS sidra_parser;

-- extract data within TTL window
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
SELECT nickname, city, day, total_steps, client_id
FROM sidra_client.sidra_staging_view_daily_steps_user
WHERE sidra_window > (SELECT expired_window FROM threshold_window);

-- compute delta for daily_steps (city-level)
INSERT INTO _delta_daily_steps BY NAME
SELECT city, day,
    SUM(total_steps) AS total_steps,
    _duckdb_ivm_multiplicity
FROM sidra_client._delta_sidra_staging_view_daily_steps_user
GROUP BY city, day, _duckdb_ivm_multiplicity;

-- update daily_steps materialized view
INSERT OR REPLACE INTO daily_steps BY NAME
WITH ivm_cte AS (
    SELECT city, day,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = 0 THEN -total_steps ELSE total_steps END) AS total_steps
    FROM _delta_daily_steps
    GROUP BY city, day
)
SELECT d.city, d.day,
       COALESCE(existing.total_steps, 0) + d.total_steps AS total_steps
FROM ivm_cte AS d
         LEFT JOIN daily_steps AS existing
                   ON existing.city = d.city AND existing.day = d.day;

-- compute delta for leaderboard
INSERT INTO _delta_leaderboard BY NAME
SELECT nickname, total_steps, _duckdb_ivm_multiplicity
FROM sidra_client._delta_sidra_staging_view_daily_steps_user
WHERE nickname IS NOT NULL;

-- update leaderboard materialized view
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

-- update responsiveness for leaderboard (per nickname)
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
SET responsiveness = p.perc
    FROM percentages p
WHERE leaderboard.nickname = p.nickname;

-- update responsiveness for daily_steps (per city, day)
WITH distinct_clients AS (
    SELECT city, day, COUNT(DISTINCT client_id) AS client_cnt
FROM sidra_client._delta_sidra_staging_view_daily_steps_user
GROUP BY city, day
    ),
    total_clients AS (
SELECT COUNT(DISTINCT id) AS total_client_cnt FROM sidra_parser.sidra_clients
    ),
    percentages AS (
SELECT d.city, d.day, LEAST((d.client_cnt::DOUBLE / t.total_client_cnt) * 100, 100) AS perc
FROM distinct_clients d, total_clients t
    )
UPDATE daily_steps
SET responsiveness = p.perc
    FROM percentages p
WHERE daily_steps.city = p.city AND daily_steps.day = p.day;

-- update completeness for leaderboard (per nickname)
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

-- update completeness for daily_steps (per city, day)
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
         SELECT city, day, COUNT(DISTINCT client_id) AS discarded
FROM sidra_client.sidra_staging_view_daily_steps_user
WHERE sidra_window <= (SELECT expired_window FROM threshold_window)
GROUP BY city, day
    ),
    kept_clients AS (
SELECT city, day, COUNT(DISTINCT client_id) AS kept
FROM sidra_client._delta_sidra_staging_view_daily_steps_user
GROUP BY city, day
    ),
    combined AS (
SELECT COALESCE(d.city, k.city) AS city,
    COALESCE(d.day, k.day) AS day,
    COALESCE(d.discarded, 0) AS discarded,
    COALESCE(k.kept, 0) AS kept
FROM discarded_clients d
    FULL OUTER JOIN kept_clients k ON d.city = k.city AND d.day = k.day
    ),
    completeness_metric AS (
SELECT city, day,
    CASE WHEN (discarded + kept) > 0
    THEN (kept::DECIMAL / (discarded + kept)) * 100
    ELSE 100 END AS completeness
FROM combined
    )
UPDATE daily_steps
SET completeness = c.completeness
    FROM completeness_metric c
WHERE daily_steps.city = c.city AND daily_steps.day = c.day;

-- clear staging view
DELETE FROM sidra_client.sidra_staging_view_daily_steps_user;

-- ttl cleanup in delta
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

-- update buffer size for leaderboard (per nickname)
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

-- update buffer size for daily_steps (per city, day)
WITH buffer AS (
    SELECT city, day, COUNT(*) AS buffer_count
FROM sidra_client.sidra_staging_view_daily_steps_user
GROUP BY city, day
    ),
    processed AS (
SELECT city, day, COUNT(*) AS processed_count
FROM sidra_client._delta_sidra_staging_view_daily_steps_user
GROUP BY city, day
    ),
    combined AS (
SELECT COALESCE(b.city, p.city) AS city,
    COALESCE(b.day, p.day) AS day,
    COALESCE(b.buffer_count, 0) AS buffer,
    COALESCE(p.processed_count, 0) AS processed
FROM buffer b
    FULL OUTER JOIN processed p ON b.city = p.city AND b.day = p.day
    ),
    stats AS (
SELECT city, day,
    CASE WHEN (buffer + processed) > 0
    THEN (buffer::DECIMAL / (buffer + processed)) * 100
    ELSE 0 END AS buffer_size
FROM combined
    )
UPDATE daily_steps
SET buffer_size = stats.buffer_size
    FROM stats
WHERE daily_steps.city = stats.city AND daily_steps.day = stats.day;

-- remove inactive clients
DELETE FROM sidra_parser.sidra_clients
WHERE last_update < today() - INTERVAL 7 DAY;

-- cleanup zero aggregates
DELETE FROM daily_steps WHERE total_steps = 0;
DELETE FROM leaderboard WHERE total_steps = 0;

-- cleanup delta tables
DELETE FROM sidra_client._delta_sidra_staging_view_daily_steps_user;
DELETE FROM _delta_daily_steps;
DELETE FROM _delta_leaderboard;
