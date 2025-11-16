from cassandra.cluster import Cluster
import os

class cassandraSingleton:
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
        self.session.shutdown()
        self._cluster.shutdown()

class CassandraService:
    def __init__(self):
        self.cassandra_session = cassandraSingleton()
    