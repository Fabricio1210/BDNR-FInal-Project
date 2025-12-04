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
        # self._cassandra.populate_data()  # Comentado temporalmente - error con UUID
        self._dgraph.populate_data()

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
        try:
            return self._dgraph.consultar_companeros_jugador(name, last_name)
        except Exception as e:
            return {"error": f"Error consultando compañeros: {str(e)}"}

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

    def consultar_jugador_completo(self, nombre, apellido):
        """
        Consulta completa de jugador desde Dgraph
        """
        try:
            return self._dgraph.consultar_jugador_completo(nombre, apellido)
        except Exception as e:
            return {"error": f"Error consultando jugador: {str(e)}"}

    def consultar_enfrentamientos_estadio(self, nombre_campo):
        """
        Consulta enfrentamientos en un estadio desde Dgraph
        """
        try:
            return self._dgraph.consultar_enfrentamientos_estadio(nombre_campo)
        except Exception as e:
            return {"error": f"Error consultando enfrentamientos: {str(e)}"}

    def consultar_enfrentamientos_equipo_temporada(self, nombre_equipo, nombre_temporada):
        """
        Consulta enfrentamientos de equipo en temporada desde Dgraph
        """
        try:
            return self._dgraph.consultar_enfrentamientos_equipo_temporada(nombre_equipo, nombre_temporada)
        except Exception as e:
            return {"error": f"Error consultando enfrentamientos: {str(e)}"}

    def consultar_equipos_locales_estadio(self, nombre_campo):
        """
        Consulta equipos locales en un estadio desde Dgraph
        """
        try:
            return self._dgraph.consultar_equipos_locales_estadio(nombre_campo)
        except Exception as e:
            return {"error": f"Error consultando equipos: {str(e)}"}

    def consultar_campos_equipo(self, nombre_equipo):
        """
        Consulta campos donde juega un equipo desde Dgraph
        """
        try:
            return self._dgraph.consultar_campos_equipo(nombre_equipo)
        except Exception as e:
            return {"error": f"Error consultando campos: {str(e)}"}