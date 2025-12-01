"""
    Modelos y controladores de cassandra
"""
import os
import logging
from uuid import UUID

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from Cassandra import schema

log = logging.getLogger()

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
        prepared = self.cassandra_session.prepare(schema.QUERY_POINTS_BY_PLAYER_MATCH_TABLE)
        return self.cassandra_session.execute(prepared, (UUID(match_id), UUID(player_id)))


    def obtener_sanciones_por_jugador_partido(self, match_id: str, player_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.prepare(schema.QUERY_SANCTIONS_BY_PLAYER_MATCH_TABLE)
        return self.cassandra_session.execute(prepared, (UUID(match_id), UUID(player_id)))

    def obtener_sanciones_por_equipo_temporada(self, team_id: str, season_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.prepare(schema.QUERY_SANCTIONS_BY_TEAM_SEASON_TABLE)
        return self.cassandra_session.execute(prepared, (UUID(team_id), UUID(season_id)))

    def obtener_mvp_por_equipo_temporada(self, team_id: str, season_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.prepare(schema.QUERY_MVP_BY_TEAM_SEASON_TABLE)
        return self.cassandra_session.execute(prepared, (UUID(team_id), UUID(season_id)))

    def obtener_eventos_por_equipo_partido(self, match_id: str, team_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.prepare(schema.QUERY_EVENTS_BY_TEAM_MATCH_TABLE)
        return self.cassandra_session.execute(prepared, (UUID(match_id), UUID(team_id)))

    def obtener_rendimiento_por_jugador_partido(self, match_id: str, player_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.prepare(schema.QUERY_PERFORMANCE_BY_PLAYER_MATCH_TABLE)
        return self.cassandra_session.execute(prepared, (UUID(match_id), UUID(player_id)))

    def obtener_rendimiento_historico_jugador(self, player_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.prepare(
            schema.QUERY_HISTORICAL_PERFORMANCE_BY_PLAYER_TABLE
        )
        return self.cassandra_session.execute(prepared, (UUID(player_id),))

    def obtener_alineacion_por_equipo_partido(self, match_id: str, team_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.prepare(schema.QUERY_LINEUP_BY_TEAM_MATCH_TABLE)
        return self.cassandra_session.execute(prepared, (UUID(match_id), UUID(team_id)))

    def obtener_posicion_actual_jugador(self, player_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.prepare(schema.QUERY_PLAYER_CURRENT_POSITION_TABLE)
        return self.cassandra_session.execute(prepared, (UUID(player_id),))

    def obtener_partidos_por_equipo_temporada(self, team_id: str, season_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.prepare(schema.QUERY_MATCHES_BY_TEAM_SEASON_TABLE)
        return self.cassandra_session.execute(prepared, (UUID(team_id), UUID(season_id)))

    def obtener_partidos_por_jugador(self, player_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.prepare(schema.QUERY_MATCHES_BY_PLAYER_TABLE)
        return self.cassandra_session.execute(prepared, (UUID(player_id),))

    def obtener_head_to_head(self, team_a_id: str, team_b_id: str):
        """
        No docstring :)
        """
        prepared = self.cassandra_session.prepare(schema.QUERY_HEAD_TO_HEAD_TEAMS_TABLE)
        return self.cassandra_session.execute(prepared, (UUID(team_a_id), UUID(team_b_id)))
