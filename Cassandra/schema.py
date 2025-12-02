"""
Queries de cql
"""

CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS analisis_deportivo
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 };
        """

CREATE_POINTS_BY_TEAM_MATCH_TABLE = """
        CREATE TABLE IF NOT EXISTS points_by_team_match (
            match_id uuid,
            team_id uuid,
            total_points int,
            PRIMARY KEY ((match_id, team_id))
        );
        """

CREATE_POINTS_BY_PLAYER_MATCH_TABLE = """
        CREATE TABLE IF NOT EXISTS points_by_player_match (
            match_id uuid,
            player_id uuid,
            total_points int,
            PRIMARY KEY ((match_id, player_id))
        );
        """

CREATE_SANCTIONS_BY_PLAYER_MATCH_TABLE = """
        CREATE TABLE IF NOT EXISTS sanctions_by_player_match (
            match_id uuid,
            player_id uuid,
            sanction_time timestamp,
            sanction_type text,
            description text,
            PRIMARY KEY ((match_id, player_id), sanction_time)
        );
        """

CREATE_SANCTIONS_BY_TEAM_SEASON_TABLE = """
        CREATE TABLE IF NOT EXISTS sanctions_by_team_season (
            team_id uuid,
            season_id text,
            sanction_type text,
            description text,
            total_sanctions int,
            PRIMARY KEY ((team_id, season_id))
        );
        """

CREATE_MVP_BY_TEAM_SEASON_TABLE = """
        CREATE TABLE IF NOT EXISTS mvp_by_team_season (
            team_id uuid,
            season_id text,
            player_id uuid,
            PRIMARY KEY ((team_id, season_id))
        );
        """

CREATE_EVENTS_BY_TEAM_MATCH_TABLE = """
        CREATE TABLE IF NOT EXISTS events_by_team_match (
            match_id uuid,
            team_id uuid,
            event_time timestamp,
            event_type text,
            player_id uuid,
            description text,
            PRIMARY KEY ((match_id, team_id), event_time)
        );
        """

CREATE_PERFORMANCE_BY_PLAYER_MATCH_TABLE = """
        CREATE TABLE IF NOT EXISTS  performance_by_player_match (
            match_id uuid,
            player_id uuid,
            distance_moved float,
            possesion float,
            points_scored int,
            assists int,
            PRIMARY KEY ((match_id, player_id))
        );
        """

CREATE_HISTORICAL_PERFORMANCE_BY_PLAYER_TABLE = """
        CREATE TABLE IF NOT EXISTS historical_performance_by_player (
            player_id uuid,
            matches_played int,
            total_points int,
            total_assists int,
            minutes_played int,
            PRIMARY KEY (player_id)
        );
        """

CREATE_LINEUP_BY_TEAM_MATCH_TABLE = """
        CREATE TABLE IF NOT EXISTS lineup_by_team_match (
            match_id uuid,
            team_id uuid,
            player_id uuid,
            position text,
            last_update timestamp,
            PRIMARY KEY ((match_id, team_id), player_id)
        );
        """

CREATE_PLAYER_CURRENT_POSITION_TABLE = """
        CREATE TABLE IF NOT EXISTS player_current_position (
            player_id uuid,
            match_id uuid,
            updated timestamp,
            position text,
            ball_possession boolean,
            PRIMARY KEY (player_id)
        );
        """

CREATE_MATCHES_BY_TEAM_SEASON_TABLE = """
        CREATE TABLE IF NOT EXISTS matches_by_team_season (
            team_id uuid,
            season_id text,
            match_datetime timestamp,
            match_id uuid,
            opponent_team_id uuid,
            location text,
            PRIMARY KEY ((team_id, season_id), match_datetime)
        ) WITH CLUSTERING ORDER BY (match_datetime ASC);
        """

CREATE_MATCHES_BY_PLAYER_TABLE = """
        CREATE TABLE IF NOT EXISTS matches_by_player (
            player_id uuid,
            match_datetime timestamp,
            match_id uuid,
            PRIMARY KEY ((player_id), match_datetime)
        ) WITH CLUSTERING ORDER BY (match_datetime DESC);
        """

CREATE_HEAD_TO_HEAD_TEAMS_TABLE = """
        CREATE TABLE IF NOT EXISTS head_to_head_teams (
            team_a_id uuid,
            team_b_id uuid,
            wins_a int,
            wins_b int,
            draws int,
            PRIMARY KEY ((team_a_id, team_b_id))
        );
        """

QUERY_POINTS_BY_TEAM_MATCH_TABLE = """
        SELECT total_points
        FROM points_by_team_match
        WHERE match_id = ? AND team_id = ?;
        """

QUERY_POINTS_BY_PLAYER_MATCH_TABLE = """
        SELECT *
        FROM points_by_player_match
        WHERE match_id = ? AND player_id = ?;
        """

QUERY_SANCTIONS_BY_PLAYER_MATCH_TABLE = """
        SELECT *
        FROM sanctions_by_player_match
        WHERE match_id = ? AND player_id = ?;
        """

QUERY_SANCTIONS_BY_TEAM_SEASON_TABLE = """
        SELECT *
        FROM sanctions_by_team_season
        WHERE team_id = ? AND season_id = ?;
        """

QUERY_MVP_BY_TEAM_SEASON_TABLE = """
        SELECT *
        FROM mvp_by_team_season
        CWHERE team_id = ? AND season_id = ?;
        """

QUERY_EVENTS_BY_TEAM_MATCH_TABLE = """
        SELECT *
        FROM events_by_team_match
        WHERE match_id = ? AND team_id = ?;
        """

QUERY_PERFORMANCE_BY_PLAYER_MATCH_TABLE = """
        SELECT *
        FROM performance_by_player_match
        WHERE match_id = ? AND player_id = ?;
        """

QUERY_HISTORICAL_PERFORMANCE_BY_PLAYER_TABLE = """
        SELECT *
        FROM historical_performance_by_player
        WHERE player_id = ?;
        """

QUERY_LINEUP_BY_TEAM_MATCH_TABLE = """
        SELECT *
        FROM lineup_by_team_match
        WHERE match_id = ? AND team_id = ?;
        """

QUERY_PLAYER_CURRENT_POSITION_TABLE = """
        SELECT *
        FROM player_current_position
        WHERE player_id = ?;
        """

QUERY_MATCHES_BY_TEAM_SEASON_TABLE = """
        SELECT *
        FROM matches_by_team_season
        WHERE team_id = ? AND season_id = ?;
        """

QUERY_MATCHES_BY_PLAYER_TABLE = """
        SELECT *
        FROM matches_by_player
        WHERE player_id = ?;
        """

QUERY_HEAD_TO_HEAD_TEAMS_TABLE = """
        SELECT *
        FROM head_to_head_teams
        WHERE team_a_id = ? AND team_b_id = ?;
        """

INSERT_POINTS_BY_TEAM_MATCH_TABLE = """
        INSERT INTO points_by_team_match (match_id, team_id, total_points)
        VALUES (?, ?, ?);
        """

INSERT_POINTS_BY_PLAYER_MATCH_TABLE = """
        INSERT INTO points_by_player_match (match_id, player_id, total_points)
        VALUES (?, ?, ?);
        """

INSERT_SANCTIONS_BY_PLAYER_MATCH_TABLE = """
        INSERT INTO sanctions_by_player_match (match_id, player_id, sanction_time, sanction_type, description)
        VALUES (?, ?, ?, ?, ?);
        """

INSERT_SANCTIONS_BY_TEAM_SEASON_TABLE = """
        INSERT INTO sanctions_by_team_season (team_id, season_id, sanction_type, description, total_sanctions)
        VALUES (?, ?, ?, ?, ?);
        """

INSERT_MVP_BY_TEAM_SEASON_TABLE = """
        INSERT INTO mvp_by_team_season (team_id, season_id, player_id)
        VALUES (?, ?, ?);
        """

INSERT_EVENTS_BY_TEAM_MATCH_TABLE = """
        INSERT INTO events_by_team_match (match_id, team_id, event_time, event_type, player_id, description)
        VALUES (?, ?, ?, ?, ?, ?);
        """

INSERT_PERFORMANCE_BY_PLAYER_MATCH_TABLE = """
        INSERT INTO performance_by_player_match (match_id, player_id, distance_moved, possesion, points_scored, assists)
        VALUES (?, ?, ?, ?, ?, ?);
        """

INSERT_HISTORICAL_PERFORMANCE_BY_PLAYER_TABLE = """
        INSERT INTO historical_performance_by_player (player_id, matches_played, total_points, total_assists, minutes_played)
        VALUES (?, ?, ?, ?, ?);
        """

INSERT_LINEUP_BY_TEAM_MATCH_TABLE = """
        INSERT INTO lineup_by_team_match (match_id, team_id, player_id, position, last_update)
        VALUES (?, ?, ?, ?, ?);
        """

INSERT_PLAYER_CURRENT_POSITION_TABLE = """
        INSERT INTO player_current_position (player_id, match_id, updated, position, ball_possession)
        VALUES (?, ?, ?, ?, ?);
        """

INSERT_MATCHES_BY_TEAM_SEASON_TABLE = """
        INSERT INTO matches_by_team_season (team_id, season_id, match_datetime, match_id, opponent_team_id, location)
        VALUES (?, ?, ?, ?, ?, ?);
        """

INSERT_MATCHES_BY_PLAYER_TABLE = """
        INSERT INTO matches_by_player (player_id, match_datetime, match_id)
        VALUES (?, ?, ?);
        """

INSERT_HEAD_TO_HEAD_TEAMS_TABLE = """
        INSERT INTO head_to_head_teams (team_a_id, team_b_id, wins_a, wins_b, draws)
        VALUES (?, ?, ?, ?, ?);
        """
