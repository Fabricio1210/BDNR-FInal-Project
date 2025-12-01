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
        self._dgraph.populate_data()

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

    def consultar_jugador_completo(self, nombre, apellido):
        """
        Consulta información completa de un jugador desde Dgraph
        """
        return self._dgraph.consultar_jugador_completo(nombre, apellido)

    def consultar_enfrentamientos_equipo_temporada(self, nombre_equipo, nombre_temporada):
        """
        Consulta enfrentamientos de un equipo en una temporada desde Dgraph
        """
        return self._dgraph.consultar_enfrentamientos_equipo_temporada(nombre_equipo, nombre_temporada)

    def consultar_equipos_locales_estadio(self, nombre_campo):
        """
        Consulta equipos locales de un estadio desde Dgraph
        """
        return self._dgraph.consultar_equipos_locales_estadio(nombre_campo)

    def consultar_campos_equipo(self, nombre_equipo):
        """
        Consulta campos donde juega un equipo desde Dgraph
        """
        return self._dgraph.consultar_campos_equipo(nombre_equipo)

    def consultar_enfrentamientos_estadio(self, nombre_campo):
        """
        Consulta todos los enfrentamientos en un estadio desde Dgraph
        """
        return self._dgraph.consultar_enfrentamientos_estadio(nombre_campo)
