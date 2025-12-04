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
        self._cassandra.borrar_base_de_datos()

    def get_player_info(self, name, last_name):
        """
        No docstring >:(
        """
        try:
            jugadores = self._mongo.obtener_jugadores(name, last_name)
            #jugadores += self._dgraph.consultar_jugador_completo(name, last_name)
            return jugadores
        except ValueError as e:
            return "No se encontro el perfil"
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
        except ValueError as e:
            return "No se encontro el partido"
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
        try:
            matches = self._mongo.obtener_partidos(sport, date)
            if not matches:
                return "No se encontraron partidos en esa fecha para ese deporte."
            return matches
        except ValueError as e:
            return "No se encontro el partido"
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)
    
    def get_matches_by_date_and_teams(self, date, home_team, away_team):
        """
        No docstring >:(
        """
        try:
            match = self._mongo.obtener_partido_por_fecha_y_equipos(date, home_team, away_team)
            if not match:
                return "No se encontró un partido con esos datos"
            return match
        except ValueError as e:
            return "No se encontro el partido"
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

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
        try:
            equipo = self._mongo.obtener_equipo(team)
            if not equipo:
                return "No se encontro el equipo deseado"
            return equipo
        except ValueError as e:
            return "No se encontro el equipo"
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

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
        try: 
            ligas = self._mongo.obtener_ligas(sport)
            if not ligas:
                return "No se enocntraron las ligas"
            return ligas
        except ValueError as e:
            return "No se encontroran las ligas"
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)