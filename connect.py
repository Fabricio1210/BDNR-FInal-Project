"""
Connections to the databases
"""
from Cassandra.cassandra import CassandraService
from Dgraph.dgraph import DgraphService
from Mongo.Mongo import MongoService

class DatabaseFacade():
    """
    No docstring >:(
    """
    def __init__(self):
        self._cassandra = CassandraService()
        self._dgraph = DgraphService()
        self._mongo = MongoService()

    def populate_databases(self):
        """
        No docstring >:(
        """
        self._mongo.poblar()
        self._cassandra.populate_data()

    def delete_databases(self):
        """
        No docstring >:(
        """

    def get_player_info(self, name, last_name):
        """
        No docstring >:(
        """

    def get_points_scored_by_player_match(self, name, last_name, match_id):
        """
        No docstring >:(
        """

    def get_player_teammates(self, name, last_name):
        """
        No docstring >:(
        """
