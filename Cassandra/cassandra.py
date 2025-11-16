"""
    Modelos y controladores de cassandra
"""
import os
import logging

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

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