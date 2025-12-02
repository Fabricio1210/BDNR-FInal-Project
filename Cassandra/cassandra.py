"""
    Modelos y controladores de cassandra
"""
import csv
import os
import logging
from uuid import UUID
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from Cassandra import schema
from Mongo.Mongo import MongoService

log = logging.getLogger()
mongo_service = MongoService()

class CassandraSingleton:
    """
    No docstring :)
    """

    _instance = None

    def _init_instance(self):
        raw_ips = os.getenv("CASSANDRA_CLUSTER_IPS", "127.0.0.1")
        ips = raw_ips.split(",")
        self._cluster = Cluster(ips)
        self.session = self._cluster.connect()

    def __new__(cls):
        if cls._instance:
            return cls._instance
        cls._instance = super().__new__(cls)
        cls._instance._init_instance()
        return cls._instance

    def shutdown(self):
        """
        No docstring :)
        """
        self.session.shutdown()
        self._cluster.shutdown()

class CassandraService:
    """
    No docstring :)
    """

    def __init__(self):
        self.cassandra_session = CassandraSingleton()

    def execute_batch(self, session, stmt, data):
        """
        No docstring :)
        """
        batch_size = 10
        for i in range(0, len(data), batch_size):
            batch = BatchStatement()
            for item in data[i : i+batch_size]:
                batch.add(stmt, item)
            session.execute(batch)

    def bulk_insert(self, session):
        """
        No docstring :)
        """

    def populate_data(self):
        """
        No docstring :)
        """
        log.info("Creating keyspace:  with replication factor 1")
        self.cassandra_session.session.execute(schema.CREATE_KEYSPACE)
        self.cassandra_session.session.execute("USE analisis_deportivo;")
        log.info("Creating logistics schema")
        self.cassandra_session.session.execute(schema.CREATE_POINTS_BY_TEAM_MATCH_TABLE)
        self.cassandra_session.session.execute(schema.CREATE_POINTS_BY_PLAYER_MATCH_TABLE)
        self.cassandra_session.session.execute(schema.CREATE_SANCTIONS_BY_PLAYER_MATCH_TABLE)
        self.cassandra_session.session.execute(schema.CREATE_SANCTIONS_BY_TEAM_SEASON_TABLE)
        self.cassandra_session.session.execute(schema.CREATE_MVP_BY_TEAM_SEASON_TABLE)
        self.cassandra_session.session.execute(schema.CREATE_EVENTS_BY_TEAM_MATCH_TABLE)
        self.cassandra_session.session.execute(schema.CREATE_PERFORMANCE_BY_PLAYER_MATCH_TABLE)
        self.cassandra_session.session.execute(schema.CREATE_HISTORICAL_PERFORMANCE_BY_PLAYER_TABLE)
        self.cassandra_session.session.execute(schema.CREATE_LINEUP_BY_TEAM_MATCH_TABLE)
        self.cassandra_session.session.execute(schema.CREATE_PLAYER_CURRENT_POSITION_TABLE)
        self.cassandra_session.session.execute(schema.CREATE_MATCHES_BY_TEAM_SEASON_TABLE )
        self.cassandra_session.session.execute(schema.CREATE_MATCHES_BY_PLAYER_TABLE)
        self.cassandra_session.session.execute(schema.CREATE_HEAD_TO_HEAD_TEAMS_TABLE)
        self._insert_points_by_team_match()
        self._insert_points_by_player_match()
        self._insert_sanctions_by_player_match()
        self._insert_sanctions_by_team_season()
        self._insert_mvp_by_team_season()
        self._insert_events_by_team_match()
        self._insert_performance_by_player_match()
        self._insert_historical_performance_by_player()
        self._insert_lineup_by_team_match()
        self._insert_player_current_position()
        self._insert_matches_by_team_season()
        self._insert_matches_by_player()
        self._insert_head_to_head_teams()

    def _insert_points_by_team_match(self):
        """
        No docstring :)
        """
        base_path = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_path, "..", "data", "partidos.csv")
        # prepared = self.cassandra_session.session.prepare(schema.INSERT_POINTS_BY_TEAM_MATCH_TABLE)
        # with open(csv_path, newline="", encoding="utf-8") as f:
        #     reader = csv.DictReader(f)
        #     for row in reader:
        #         match_id = UUID(row["match_id"])
        #         team_id = UUID(row["team_id"])
        #         total_points = int(row["total_points"])
        #         bound = prepared.bind((match_id, team_id, total_points))
        #         self.cassandra_session.session.execute(bound)

    def _insert_points_by_player_match(self):
        """
        No docstring :)
        """
        base_path = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_path, "..", "data", "puntos_por_jugador_partido.csv")
        # prepared = self.cassandra_session.session.prepare(schema.INSERT_POINTS_BY_PLAYER_MATCH_TABLE)
        # with open(csv_path, newline="", encoding="utf-8") as f:
        #     reader = csv.DictReader(f)
        #     for row in reader:
        #         nombre_jugador = row["nombre"]
        #         apellido_jugador = row["apellido"]
        #         jugador = mongo_service.obtener_jugadores(nombre_jugador, apellido_jugador)
        #         if not jugador:
        #             raise ValueError("El jugador no existe aun en la base de datos de mongo")
        #         jugador = jugador[0]
        #         jugador_id = jugador.get("_id")
        #         total_points = int(row["puntos_anotados"])
        #         match = mongo_service.obtener_partidos(??, row["fecha"])
        #         if not jugador:
        #             raise ValueError("La partida no existe aun en la base de datos de mongo")
        #         match = match[0]
        #         match_id = match.get("_id")
        #         bound = prepared.bind((match_id, jugador_id, total_points))
        #         self.cassandra_session.session.execute(bound)

    def _insert_sanctions_by_player_match(self):
        """
        No docstring :)
        """
        base_path = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_path, "..", "data", "sanctions_by_player_match.csv")
        # prepared = self.cassandra_session.session.prepare(
        #     schema.INSERT_SANCTIONS_BY_PLAYER_MATCH_TABLE
        # )
        # with open(csv_path, newline=", encoding="utf-8") as f:
        #     reader = csv.DictReader(f)
        #     for row in reader:
        #         match_id = UUID(row["match_id"])
        #         player_id = UUID(row["player_id"])
        #         sanction_time = datetime.fromisoformat(row["sanction_time"])
        #         sanction_type = row["sanction_type"]
        #         description = row["description"]
        #         bound = prepared.bind(
        #             (match_id, player_id, sanction_time, sanction_type, description)
        #         )
        #         self.cassandra_session.session.execute(bound)

    def _insert_sanctions_by_team_season(self):
        """
        No docstring :)
        """
        base_path = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_path, "..", "data", "sanctions_by_team_season.csv")
        prepared = self.cassandra_session.session.prepare(
            schema.INSERT_SANCTIONS_BY_TEAM_SEASON_TABLE
        )
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                team_name = row["nombre_equipo"]
                team = mongo_service.obtener_equipo(team_name)
                if not team:
                    raise ValueError(
                        "El equipo no esta registrado en la base de datos de mongo aun"
                    )
                team_id = team[0].get("_id")
                season_id = row["temporada"]
                sanction_type = row["tipo_sancion"]
                description = row["descripcion"]
                total_sanctions = int(row["total_sanciones"])
                bound = prepared.bind(
                    (team_id, season_id, sanction_type, description, total_sanctions)
                )
                self.cassandra_session.session.execute(bound)

    def _insert_mvp_by_team_season(self):
        """
        No docstring :)
        """
        base_path = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_path, "..", "data", "mvp_por_equipo_temporada.csv")
        prepared = self.cassandra_session.session.prepare(schema.INSERT_MVP_BY_TEAM_SEASON_TABLE)
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                team_name = row["nombre_equipo"]
                team = mongo_service.obtener_equipo(team_name)
                if not team:
                    raise ValueError(
                        "El equipo no esta registrado en la base de datos de mongo aun"
                    )
                team_id = team[0].get("_id")
                season_id = row["temporada"]
                player_name = row["nombre_jugador"]
                player_last_name = row["apellido_jugador"]
                player = mongo_service.obtener_jugadores(player_name, player_last_name)
                if not player:
                    raise ValueError(
                        "El jugador no esta registrado en la base de datos de mongo aun"
                    )
                player_id = player[0].get("_id")
                bound = prepared.bind((team_id, season_id, player_id))
                self.cassandra_session.session.execute(bound)

    def _insert_events_by_team_match(self):
        """
        No docstring :)
        """
        base_path = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_path, "..", "data", "events_by_team_match.csv")
        # prepared = self.cassandra_session.session.prepare(schema.INSERT_EVENTS_BY_TEAM_MATCH_TABLE)
        # with open(csv_path, newline="", encoding="utf-8") as f:
        #     reader = csv.DictReader(f)
        #     for row in reader:
        #         match_id = UUID(row["match_id"])
        #         team_id = UUID(row["team_id"])
        #         event_time = datetime.fromisoformat(row["event_time"])
        #         event_type = row["event_type"]
        #         player_id = UUID(row["player_id"])
        #         description = row["description"]
        #         bound = prepared.bind(
        #             (match_id, team_id, event_time, event_type, player_id, description)
        #         )
        #         self.cassandra_session.session.execute(bound)

    def _insert_performance_by_player_match(self):
        """
        No docstring :)
        """
        base_path = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_path, "..", "data", "performance_by_player_match.csv")
        # prepared = self.cassandra_session.session.prepare(
        #     schema.INSERT_PERFORMANCE_BY_PLAYER_MATCH_TABLE
        # )
        # with open(csv_path, newline="", encoding="utf-8") as f:
        #     reader = csv.DictReader(f)
        #     for row in reader:
        #         match_id = UUID(row["match_id"])
        #         player_id = UUID(row["player_id"])
        #         distance_moved = float(row["distance_moved"])
        #         possesion = float(row["possesion"])
        #         points_scored = int(row["points_scored"])
        #         assists = int(row["assists"])
        #         bound = prepared.bind(
        #             (match_id, player_id, distance_moved, possesion, points_scored, assists)
        #         )
        #         self.cassandra_session.session.execute(bound)

    def _insert_historical_performance_by_player(self):
        """
        No docstring :)
        """
        base_path = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_path, "..", "data", "historical_performance_by_player.csv")
        prepared = self.cassandra_session.session.prepare(
            schema.INSERT_HISTORICAL_PERFORMANCE_BY_PLAYER_TABLE
        )
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                player_id = UUID(row["player_id"])
                matches_played = int(row["matches_played"])
                total_points = int(row["total_points"])
                total_assists = int(row["total_assists"])
                minutes_played = int(row["minutes_played"])
                bound = prepared.bind(
                    (player_id, matches_played, total_points, total_assists, minutes_played)
                )
                self.cassandra_session.session.execute(bound)

    def _insert_lineup_by_team_match(self):
        """
        No docstring :)
        """
        base_path = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_path, "..", "data", "lineup_by_team_match.csv")
        prepared = self.cassandra_session.session.prepare(schema.INSERT_LINEUP_BY_TEAM_MATCH_TABLE)
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                match_id = UUID(row["match_id"])
                team_id = UUID(row["team_id"])
                player_id = UUID(row["player_id"])
                position = row["position"]
                last_update = datetime.fromisoformat(row["last_update"])
                bound = prepared.bind((match_id, team_id, player_id, position, last_update))
                self.cassandra_session.session.execute(bound)

    def _insert_player_current_position(self):
        """
        No docstring :)
        """
        base_path = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_path, "..", "data", "player_current_position.csv")
        prepared = self.cassandra_session.session.prepare(
            schema.INSERT_PLAYER_CURRENT_POSITION_TABLE
        )
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                player_id = UUID(row["player_id"])
                match_id = UUID(row["match_id"])
                updated = datetime.fromisoformat(row["updated"])
                position = row["position"]
                ball_possession = row["ball_possession"].lower() == "true"
                bound = prepared.bind((player_id, match_id, updated, position, ball_possession))
                self.cassandra_session.session.execute(bound)

    def _insert_matches_by_team_season(self):
        """
        No docstring :)
        """
        base_path = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_path, "..", "data", "matches_by_team_season.csv")
        prepared = self.cassandra_session.session.prepare(
            schema.INSERT_MATCHES_BY_TEAM_SEASON_TABLE
        )
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                team_id = UUID(row["team_id"])
                season_id = UUID(row["season_id"])
                match_datetime = datetime.fromisoformat(row["match_datetime"])
                match_id = UUID(row["match_id"])
                opponent_team_id = UUID(row["opponent_team_id"])
                location = row["location"]
                bound = prepared.bind(
                    (team_id, season_id, match_datetime, match_id, opponent_team_id, location)
                )
                self.cassandra_session.session.execute(bound)

    def _insert_matches_by_player(self):
        """
        No docstring :)
        """
        base_path = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_path, "..", "data", "matches_by_player.csv")
        prepared = self.cassandra_session.session.prepare(schema.INSERT_MATCHES_BY_PLAYER_TABLE)
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                player_id = UUID(row["player_id"])
                match_datetime = datetime.fromisoformat(row["match_datetime"])
                match_id = UUID(row["match_id"])
                bound = prepared.bind((player_id, match_datetime, match_id))
                self.cassandra_session.session.execute(bound)

    def _insert_head_to_head_teams(self):
        """
        No docstring :)
        """
        base_path = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_path, "..", "data", "head_to_head_teams.csv")
        prepared = self.cassandra_session.session.prepare(schema.INSERT_HEAD_TO_HEAD_TEAMS_TABLE)
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                team_a_id = UUID(row["team_a_id"])
                team_b_id = UUID(row["team_b_id"])
                wins_a = int(row["wins_a"])
                wins_b = int(row["wins_b"])
                draws = int(row["draws"])
                bound = prepared.bind((team_a_id, team_b_id, wins_a, wins_b, draws))
                self.cassandra_session.session.execute(bound)

    def obtener_puntos_por_partido_equipo(self, match_id: str, team_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.session.prepare(schema.QUERY_POINTS_BY_TEAM_MATCH_TABLE)
        match_uuid = UUID(match_id)
        team_uuid = UUID(team_id)
        return self.cassandra_session.session.execute(prepared, (match_uuid, team_uuid))

    def obtener_puntos_por_jugador_partido(self, match_id: str, player_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.session.prepare(schema.QUERY_POINTS_BY_PLAYER_MATCH_TABLE)
        return self.cassandra_session.session.execute(prepared, (UUID(match_id), UUID(player_id)))


    def obtener_sanciones_por_jugador_partido(self, match_id: str, player_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.session.prepare(
            schema.QUERY_SANCTIONS_BY_PLAYER_MATCH_TABLE
        )
        return self.cassandra_session.session.execute(prepared, (UUID(match_id), UUID(player_id)))

    def obtener_sanciones_por_equipo_temporada(self, team_id: str, season_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.session.prepare(
            schema.QUERY_SANCTIONS_BY_TEAM_SEASON_TABLE
        )
        return self.cassandra_session.session.execute(prepared, (UUID(team_id), UUID(season_id)))

    def obtener_mvp_por_equipo_temporada(self, team_id: str, season_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.session.prepare(schema.QUERY_MVP_BY_TEAM_SEASON_TABLE)
        return self.cassandra_session.session.execute(prepared, (UUID(team_id), UUID(season_id)))

    def obtener_eventos_por_equipo_partido(self, match_id: str, team_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.session.prepare(schema.QUERY_EVENTS_BY_TEAM_MATCH_TABLE)
        return self.cassandra_session.session.execute(prepared, (UUID(match_id), UUID(team_id)))

    def obtener_rendimiento_por_jugador_partido(self, match_id: str, player_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.session.prepare(
            schema.QUERY_PERFORMANCE_BY_PLAYER_MATCH_TABLE
        )
        return self.cassandra_session.session.execute(prepared, (UUID(match_id), UUID(player_id)))

    def obtener_rendimiento_historico_jugador(self, player_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.session.prepare(
            schema.QUERY_HISTORICAL_PERFORMANCE_BY_PLAYER_TABLE
        )
        return self.cassandra_session.session.execute(prepared, (UUID(player_id),))

    def obtener_alineacion_por_equipo_partido(self, match_id: str, team_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.session.prepare(schema.QUERY_LINEUP_BY_TEAM_MATCH_TABLE)
        return self.cassandra_session.session.execute(prepared, (UUID(match_id), UUID(team_id)))

    def obtener_posicion_actual_jugador(self, player_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.session.prepare(
            schema.QUERY_PLAYER_CURRENT_POSITION_TABLE
        )
        return self.cassandra_session.session.execute(prepared, (UUID(player_id),))

    def obtener_partidos_por_equipo_temporada(self, team_id: str, season_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.session.prepare(schema.QUERY_MATCHES_BY_TEAM_SEASON_TABLE)
        return self.cassandra_session.session.execute(prepared, (UUID(team_id), UUID(season_id)))

    def obtener_partidos_por_jugador(self, player_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.session.prepare(schema.QUERY_MATCHES_BY_PLAYER_TABLE)
        return self.cassandra_session.session.execute(prepared, (UUID(player_id),))

    def obtener_head_to_head(self, team_a_id: str, team_b_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.session.prepare(schema.QUERY_HEAD_TO_HEAD_TEAMS_TABLE)
        return self.cassandra_session.session.execute(prepared, (UUID(team_a_id), UUID(team_b_id)))
