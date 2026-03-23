SET memory_limit = '16GB';
SET threads TO 16;

-- Step 1: Attach the databases
attach if not exists 'dbname=sidra user=sidra password=test host=localhost' as sidra_client (type postgres);

-- Step 2: Calculate the partial sums for daily_steps_user delta
INSERT INTO sidra_client._delta_sidra_staging_view_daily_steps_user BY NAME
SELECT nickname, city, day, SUM(steps) AS total_steps, sidra_window, client_id,
    action as _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp
FROM sidra_client.sidra_staging_view_activity_sessions
GROUP BY nickname, city, day, sidra_window, client_id, _duckdb_ivm_multiplicity;

-- Step 3: Propagate changes to daily_steps_user delta table
INSERT INTO _delta_centralized_daily_steps_user BY NAME
SELECT nickname, city, day,
    SUM(CASE WHEN _duckdb_ivm_multiplicity = 0 THEN -total_steps ELSE total_steps END) AS total_steps,
    true as _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp
FROM sidra_client._delta_sidra_staging_view_daily_steps_user
GROUP BY nickname, city, day;

-- Step 4: Insert or replace into centralized_daily_steps_user using IVM logic
INSERT OR REPLACE INTO centralized_daily_steps_user
WITH ivm_cte AS (
    SELECT nickname, city, day,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = 0 THEN -total_steps ELSE total_steps END) AS total_steps
    FROM _delta_centralized_daily_steps_user
    GROUP BY nickname, city, day
)
SELECT d.nickname, d.city, d.day,
       COALESCE(existing.total_steps, 0) + d.total_steps AS total_steps
FROM ivm_cte AS d
         LEFT JOIN centralized_daily_steps_user AS existing
                   ON existing.nickname = d.nickname
                       AND existing.city = d.city
                       AND existing.day = d.day;

-- Step 5: Propagate changes to daily_steps delta table
INSERT INTO _delta_centralized_daily_steps BY NAME
SELECT city, day,
    SUM(CASE WHEN _duckdb_ivm_multiplicity = 0 THEN -total_steps ELSE total_steps END) AS total_steps,
    true as _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp
FROM _delta_centralized_daily_steps_user
GROUP BY city, day;

-- Step 6: Insert or replace into centralized_daily_steps using IVM logic
INSERT OR REPLACE INTO centralized_daily_steps
WITH ivm_cte AS (
    SELECT city, day,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = 0 THEN -total_steps ELSE total_steps END) AS total_steps
    FROM _delta_centralized_daily_steps
    GROUP BY city, day
)
SELECT d.city, d.day,
       COALESCE(existing.total_steps, 0) + d.total_steps AS total_steps
FROM ivm_cte AS d
         LEFT JOIN centralized_daily_steps AS existing
                   ON existing.city = d.city
                       AND existing.day = d.day;

-- Step 7: Propagate changes to leaderboard delta table
INSERT INTO _delta_centralized_leaderboard BY NAME
SELECT nickname,
       SUM(CASE WHEN _duckdb_ivm_multiplicity = 0 THEN -total_steps ELSE total_steps END) AS total_steps,
       true as _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp
FROM _delta_centralized_daily_steps_user
WHERE nickname IS NOT NULL
GROUP BY nickname;

-- Step 8: Insert or replace into centralized_leaderboard using IVM logic
INSERT OR REPLACE INTO centralized_leaderboard
WITH ivm_cte AS (
    SELECT nickname,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = 0 THEN -total_steps ELSE total_steps END) AS total_steps
    FROM _delta_centralized_leaderboard
    GROUP BY nickname
)
SELECT d.nickname,
       COALESCE(existing.total_steps, 0) + d.total_steps AS total_steps
FROM ivm_cte AS d
         LEFT JOIN centralized_leaderboard AS existing
                   ON existing.nickname = d.nickname;

-- Step 9: Delete zeroed rows
DELETE FROM centralized_daily_steps_user
WHERE total_steps = 0;

DELETE FROM centralized_daily_steps
WHERE total_steps = 0;

DELETE FROM centralized_leaderboard
WHERE total_steps = 0;

-- Step 10: Clean up deltas
DELETE FROM sidra_client.sidra_staging_view_activity_sessions;
DELETE FROM sidra_client._delta_sidra_staging_view_daily_steps_user;
DELETE FROM _delta_centralized_daily_steps_user;
DELETE FROM _delta_centralized_daily_steps;
DELETE FROM _delta_centralized_leaderboard;