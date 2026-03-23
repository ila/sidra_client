attach if not exists 'dbname=sidra user=sidra password=test host=localhost' as sidra_client (type postgres);
ATTACH 'sidra_parser.db' AS sidra_parser;

CREATE DECENTRALIZED TABLE activity_sessions (
    username TEXT SENSITIVE,
    nickname TEXT,
    city TEXT DIMENSION,
    day TEXT,
    start_time TEXT,
    end_time TEXT,
    steps INTEGER,
    heartbeat_rate INTEGER
);

CREATE DECENTRALIZED MATERIALIZED VIEW daily_steps_user
AS (SELECT nickname, city, day, SUM(steps) AS total_steps
    FROM activity_sessions
    GROUP BY nickname, city, day)
REFRESH 4 --- refresh every 4h (client and server-side)
WINDOW 24 --- emit one result per day
TTL 48;

-- now we create the tables for the fully decentralized example
create table if not exists sidra_client.sidra_staging_view_daily_steps_user (
    nickname text,
    city text,
    day text,
    total_steps integer,
    generation timestamptz,
    arrival timestamptz,
    sidra_window int,
    client_id bigint,
    action smallint);

create table if not exists daily_steps as
select city, day,
    sum(total_steps) as total_steps,
    cast(0.0 as decimal(5,2)) as responsiveness,
    cast(100.0 as decimal(5,2)) as completeness,
    cast(0.0 as decimal(5,2)) as buffer_size
from sidra_client.sidra_staging_view_daily_steps_user
group by city, day;

-- leaderboard table
create table if not exists leaderboard as
select nickname,
       sum(total_steps) as total_steps,
       cast(0.0 as decimal(5,2)) as responsiveness,
       cast(100.0 as decimal(5,2)) as completeness,
       cast(0.0 as decimal(5,2)) as buffer_size
from sidra_client.sidra_staging_view_daily_steps_user
where nickname is not null
group by nickname;

-- manually creating delta tables now (to simplify reproducibility)
create table if not exists sidra_client._delta_sidra_staging_view_daily_steps_user as
select nickname, city, day, total_steps, sidra_window, client_id,
    true as _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp
from sidra_client.sidra_staging_view_daily_steps_user limit 0;

create table if not exists _delta_daily_steps as
select *, true as _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp
from daily_steps limit 0;

create table if not exists _delta_leaderboard as
select nickname, total_steps, true as _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp
from leaderboard limit 0;

create unique index if not exists daily_steps_ivm_index on daily_steps(city, day);
create unique index if not exists leaderboard_ivm_index on leaderboard(nickname);

-- dropping the delta tables
drop table if exists sidra_client.delta_sidra_staging_view_daily_steps_user;
drop table if exists delta_daily_steps;
drop table if exists delta_leaderboard;

.q
