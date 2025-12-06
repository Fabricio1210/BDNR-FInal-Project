"""
Connections to the databases
"""
import json
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
        self._mongo.eliminar_base_de_datos()
        self._cassandra.borrar_base_de_datos()
        self._dgraph.drop_all()

    def get_player_info(self, name, last_name):
        """
        No docstring >:(
        """
        try:
            jugadores = self._mongo.obtener_jugadores(name, last_name)
            jugador_dgraph = self._dgraph.consultar_jugador_completo(name, last_name)
            print(f"Numero: {jugadores[0].get('numero')}")
            print(f"Fecha de nacimiento: {jugadores[0].get('fecha_nacimiento')}")
            print(f"Deporte: {jugadores[0].get('deporte')}")
            print(f"Pais de origen: {jugadores[0].get('pais_origen')}")
            print(f"Atributos adicionales: ")
            for atributo, valor in jugadores[0].get("atributos_adicionales").items():
                print(f"{atributo}: {valor}") 
            if not jugadores[0].get("atributos_adicionales"):
                print("Ninguno")
            if jugador_dgraph.get('jugador'):
                for jugador in jugador_dgraph['jugador']:
                    if jugador.get('juega_para'):
                        print("\nHISTORIAL DE EQUIPOS:")
                        for i, equipo in enumerate(jugador['juega_para'], 1):
                            print(f"\n  [{i}] {equipo.get('nombre', 'N/A')}")
                            print(f"      Liga: {equipo.get('liga', 'N/A')}")
                            print(f"      País: {equipo.get('pais', 'N/A')}")
                            print(f"      Ciudad: {equipo.get('ciudad', 'N/A')}")
                            if 'juega_para|fechaInicio' in equipo:
                                print(f"      Fecha Inicio: {equipo.get('juega_para|fechaInicio', 'N/A')}")
                            if 'juega_para|fechaFin' in equipo:
                                print(f"      Fecha Fin: {equipo.get('juega_para|fechaFin', 'N/A')}")

                            if equipo.get('campo_local'):
                                campo = equipo['campo_local']
                                print(f"      Estadio Local: {campo.get('nombre', 'N/A')} ({campo.get('pais', 'N/A')})")
            return ""
        except ValueError:
            return "No se encontro el perfil"
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_points_scored_by_player_match(self, name, last_name, date_match, local_team, visitor_team):
        """
        No docstring >:(
        """
        try:
            player = self._mongo.obtener_jugadores(name, last_name)
            player_id = player[0].get("_id")
            match = self._mongo.obtener_partido_por_fecha_y_equipos(date_match, local_team, visitor_team)
            data = self._cassandra.obtener_puntos_por_jugador_partido(match.get("_id"), player_id)
            for row in data:
                row_dict = row._asdict()
                for k, v in row_dict.items():
                    print(f"{k}: {v}")
            return ""
        except ValueError as e:
            return "No se encontro el partido"
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_player_teammates(self, name, last_name):
        """
        No docstring >:(
        """
        try:
            result = self._dgraph.consultar_companeros_jugador(name, last_name)
            print(json.dumps(result, indent=4))
            return ""
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_matches_by_date_sport(self, sport, date):
        """
        No docstring >:(
        """
        try:
            matches = self._mongo.obtener_partidos(sport, date)
            print(matches)
            return "matches"
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
            print(json.dumps(match, indent=4))
            return ""
        except ValueError as e:
            return "No se encontro el partido"
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_events_by_team_match(self, team, date_match, local_team, visitor_team):
        """
        No docstring >:(
        """
        try:
            match = self._mongo.obtener_partido_por_fecha_y_equipos(date_match, local_team, visitor_team)
            team_obj = self._mongo.obtener_equipo(team)
            team_id = team_obj.get("_id")
            result = self._cassandra.obtener_eventos_por_equipo_partido(match.get("_id"), team_id)
            for row in result:
                row_dict = row._asdict()
                print(f"Tiempo del evento: {row_dict['event_time']}")
                print(f"Tipo de evento: {row_dict['event_type']}")
                print(f"Descripcion: {row_dict['description']}")
                print(f"Id del jugador: {row_dict['player_id']}")
            return ""
        except ValueError as e:
            return "No se encontro el partido con ese equipo: " + str(e)
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)
        
    def get_matches_by_stadium(self, stadium):
        """
        No docstring >:(
        """
        try:
            result = self._dgraph.consultar_enfrentamientos_estadio(stadium)
            print(json.dumps(result, indent=4))
            return ""
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)


    def get_team_info(self, team):
        """
        No docstring >:(
        """
        try:
            equipo = self._mongo.obtener_equipo(team)
            print(json.dumps(equipo, indent=4))
            return ""
        except ValueError as e:
            return "No se encontro el equipo"
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_matches_by_team_season(self, team, season):
        """
        No docstring >:(
        """
        try:
            resultado = self._dgraph.consultar_enfrentamientos_equipo_temporada(team, season)
            if resultado.get('enfrentamientos'):
                print("\n" + "="*60)
                print(f"ENFRENTAMIENTOS DE {team.upper()}")
                print(f"TEMPORADA: {team}")
                print("="*60)
                for i, partido in enumerate(resultado['enfrentamientos'], 1):
                    print(f"\n[{i}] {partido.get('fecha', 'N/A')}")
                    local = partido.get('equipo_local', {})
                    visitante = partido.get('equipo_visitante', {})

                    # Determinar si el equipo consultado es local o visitante
                    es_local = local.get('nombre', '') == team
                    rival = visitante if es_local else local

                    print(f"    {local.get('nombre', 'N/A')} vs {visitante.get('nombre', 'N/A')}")
                    print(f"    Marcador: {partido.get('marcadorLocal', 0)} - {partido.get('marcadorVisitante', 0)}")
                    print(f"    Resultado: {partido.get('resultado', 'N/A')}")
                    print(f"    Rival: {rival.get('nombre', 'N/A')} ({rival.get('pais', 'N/A')})")

                    if partido.get('campo'):
                        campo = partido['campo']
                        print(f"    Estadio: {campo.get('nombre', 'N/A')} - {campo.get('pais', 'N/A')}")
                        print(f"    Tipo: {campo.get('tipo', 'N/A')} | Capacidad: {campo.get('capacidad', 'N/A'):,}")
            return ""
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_teams_by_stadium(self, stadium):
        """
        No docstring >:(
        """
        try:
            result = self._dgraph.consultar_equipos_locales_estadio(stadium)
            print(json.dumps(result, indent=4))
            return ""
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_team_ranking_by_sport(self, sport):
        """
        No docstring >:(
        """
        try:
            ranking = self._mongo.obtener_puntajes_por_deporte(sport)
            print(json.dumps(ranking, indent=4))
            return ""
        except ValueError as e:
            return "No se encontraron los equipos"
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_first_places_from_all_sports(self):
        """
        No docstring >:(
        """
        try:
            ranking = self._mongo.obtener_primer_lugar_por_deporte()
            print(json.dumps(ranking, indent=4))
            return ""
        except ValueError as e:
            return "No se encontraron los equipos"
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_league_stats_by_season(self, league, season):
        """
        No docstring >:(
        """ 
        try:
            estadisticas = self._mongo.estadisticas_liga(league, season)
            print(json.dumps(estadisticas, indent=4))
            return ""
        except ValueError as e:
            return "No se encontraron las ligas"
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_sanctions_by_player_match(self, name, last_name, date_match, local_team, visitor_team):
        """
        No docstring >:(
        """
        try:
            player = self._mongo.obtener_jugadores(name, last_name)
            player_id = player[0].get("_id")
            match = self._mongo.obtener_partido_por_fecha_y_equipos(date_match, local_team, visitor_team)
            data = self._cassandra.obtener_sanciones_por_jugador_partido(match.get("_id"), player_id)
            for row in data:
                row_dict = row._asdict()
                for k, v in row_dict.items():
                    print(f"{k}: {v}")
            return ""
        except ValueError as e:
            return "No se encontroran las ligas"
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_all_leagues_by_sport(self, sport):
        """
        No docstring >:(
        """  
        try: 
            ligas = self._mongo.obtener_ligas(sport)
            print(json.dumps(ligas, indent=4))
            return ""
        except ValueError as e:
            return "No se encontroran las ligas"
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)
        
    def get_sanctions_by_team_season(self, team, season):
        """
        No docstring >:(
        """  
        try:
            team_obj = self._mongo.obtener_equipo(team)
            team_id = team_obj.get("_id")
            data = self._cassandra.obtener_sanciones_por_equipo_temporada(team_id, season)
            for row in data:
                row_dict = row._asdict()
                for k, v in row_dict.items():
                    print(f"{k}: {v}")
            return ""
        except ValueError as e:
            return "No se encontro el partido"
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)
        
    def get_mvps_by_team_season(self, team, season):
        """
        No docstring >:(
        """
        try:
            team_obj = self._mongo.obtener_equipo(team)
            team_id = team_obj.get("_id")
            data = self._cassandra.obtener_mvp_por_equipo_temporada(team_id, season)
            for row in data:
                row_dict = row._asdict()
                for k, v in row_dict.items():
                    print(f"{k}: {v}")
            return ""
        except ValueError as e:
            return "No se encontroran las ligas"
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def add_player(self, nombre, apellido, numero, fecha_nacimiento, deporte, pais_origen, posicion, altura_cm, equipo_nombre):
        """
        No docstring >:(
        """  
        try:
            resultado = self._mongo.agregar_jugador(
                nombre,
                apellido,
                numero,
                fecha_nacimiento,
                deporte,
                pais_origen,
                posicion,
                altura_cm,
                equipo_nombre
            )

            if "error" in resultado:
                return "Error: " + resultado["error"]

            return f"Jugador agregado correctamente al equipo {equipo_nombre}. ID: {resultado['jugador_id']}"

        except ValueError:
            return "Error: datos inválidos para agregar jugador."
        except Exception as e:
            return "Error en la base de datos: " + str(e)
