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
            season_id uuid,
            sanction_type text,
            description text,
            total_sanctions int,
            PRIMARY KEY ((team_id, season_id))
        );
        """

CREATE_MVP_BY_TEAM_SEASON_TABLE = """
        CREATE TABLE IF NOT EXISTS mvp_by_team_season (
            team_id uuid,
            season_id uuid,
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
