"""
Connections to the databases
"""
from Cassandra.cassandra import CassandraService
from Dgraph.dgraph import DgraphService
from Mongo.Mongo import MongoSingleton

class DatabaseFacade():
    """
    No docstring >:(
    """
    def __init__(self):
        self._cassandra = CassandraService()
        self._dgraph = DgraphService()
        self._mongo = MongoSingleton()

    def populate_databases(self):
        """
        No docstring >:(
        """
        self._mongo.populate()
        self._cassandra.populate_data()
        