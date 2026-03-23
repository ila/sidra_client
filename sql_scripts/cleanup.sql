-- sidra_parser.db
attach if not exists 'sidra_parser.db' as sidra_parser;
update sidra_parser.sidra_view_constraints set sidra_min_agg = 100 where view_name = 'daily_steps_user';
-- LEAST((d.window_client_count::DOUBLE / t.total_client_count) * 100, 100) AS percentage
update sidra_parser.sidra_current_window set sidra_window = 0;
delete from sidra_parser.sidra_clients;

-- activity_sessions.db
attach if not exists 'activity_sessions.db' as activity_sessions;
delete from activity_sessions.daily_steps;
delete from activity_sessions.centralized_daily_steps;
delete from activity_sessions.leaderboard;
delete from activity_sessions.centralized_leaderboard;

attach if not exists 'dbname=sidra user=sidra password=test host=localhost' as sidra_client (type postgres);
delete from sidra_client.sidra_staging_view_daily_steps_user;
delete from sidra_client.sidra_staging_view_activity_sessions;

-- delete from delta tables (should not be necessary, but in case something fails mid-refresh)
delete from activity_sessions._delta_centralized_daily_steps_user;
delete from activity_sessions._delta_daily_steps_club;
delete from activity_sessions._delta_centralized_daily_steps_club;
