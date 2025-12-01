"""
    Modelos y controladores de cassandra
"""
import os
import logging
from uuid import UUID

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import schema

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

    def populateData(self):
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
