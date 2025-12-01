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
        try:
            jugadores = self._mongo.obtener_jugadores(name, last_name)
            jugadores += self._dgraph.consultar_jugador_completo(name, last_name)
            if not jugadores:
                return "No se encontro ningun jugador con ese nombre y aplleido."
            else:
                return jugadores
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_points_scored_by_player_match(self, name, last_name, match_id):
        """
        No docstring >:(
        """
        try:
            player = self._mongo.obtener_jugadores(name, last_name)
            if not player:
                return "No se encontraron jugadores con ese nombre y aplleido."
            player_id = None
            data = self._cassandra.obtener_puntos_por_jugador_partido(match_id, player_id)
            if not data:
                return "No se encontraron datos de ese partido"
            else:
                return data
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_player_teammates(self, name, last_name):
        """
        No docstring >:(
        """

    def get_matches_by_date_sport(self, sport, date):
        """
        No docstring >:(
        """

    def get_events_by_team_match(self, team, match_id):
        """
        No docstring >:(
        """

    def get_matches_by_stadium(self, stadium):
        """
        No docstring >:(
        """

    def get_team_info(self, team):
        """
        No docstring >:(
        """

    def get_matches_by_team_season(self, team, season):
        """
        No docstring >:(
        """

    def get_teams_by_stadium(self, stadium):
        """
        No docstring >:(
        """

    def get_team_reanking_by_sport(self, sport):
        """
        No docstring >:(
        """

    def get_first_places_from_all_sports(self):
        """
        No docstring >:(
        """

    def get_league_stats_by_season(self, league, season):
        """
        No docstring >:(
        """  

    def get_all_leagues_by_sport(self, sport):
        """
        No docstring >:(
        """  
