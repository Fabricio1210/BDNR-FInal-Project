"""
Connections to the databases
"""
import json
from Cassandra.cassandra import CassandraService
from Dgraph.dgraph import DgraphService
from Mongo.Mongo import MongoService


def clean_date(date_str):
    """Helper para limpiar formato de fecha ISO 8601 a formato simple YYYY-MM-DD"""
    if not date_str or date_str == 'N/A':
        return date_str
    
    if 'T' in date_str:
        return date_str.split('T')[0]
    return date_str


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

    def get_team_rivalries(self, team):
        """
        No docstring >:(
        """
        try:
            result = self._dgraph.consultar_rivalidades_equipo(team)
            if result.get('equipo') and len(result['equipo']) > 0:
                equipo = result['equipo'][0]
                print(f"\n{'='*60}")
                print(f"RIVALIDADES DE {equipo.get('nombre', 'N/A').upper()}")
                print(f"Liga: {equipo.get('liga', 'N/A')}")
                print(f"{'='*60}")

                if equipo.get('rivalidad'):
                    for i, rival in enumerate(equipo['rivalidad'], 1):
                        print(f"\n[{i}] {rival.get('nombre', 'N/A')}")
                        print(f"    Liga: {rival.get('liga', 'N/A')}")
                        print(f"    País: {rival.get('pais', 'N/A')}")
                else:
                    print("\nNo se encontraron rivalidades para este equipo")
            else:
                print("No se encontró el equipo")
            return ""
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_player_rival_teams(self, name, last_name):
        """
        No docstring >:(
        """
        try:
            result = self._dgraph.consultar_jugadores_equipos_rivales(name, last_name)
            if result.get('jugador') and len(result['jugador']) > 0:
                jugador = result['jugador'][0]
                print(f"\n{'='*60}")
                print(f"HISTORIAL EN EQUIPOS RIVALES: {jugador.get('nombre', '')} {jugador.get('apellido', '')}")
                print(f"{'='*60}")

                if jugador.get('juega_para'):
                    for i, equipo in enumerate(jugador['juega_para'], 1):
                        print(f"\n[{i}] {equipo.get('nombre', 'N/A')}")
                        print(f"    Liga: {equipo.get('liga', 'N/A')}")
                        fecha_inicio = clean_date(equipo.get('juega_para|fechaInicio', 'N/A'))
                        fecha_fin = clean_date(equipo.get('juega_para|fechaFin', 'N/A'))
                        print(f"    Periodo: {fecha_inicio} - {fecha_fin}")

                        if equipo.get('rivalidad'):
                            print(f"    Equipos rivales:")
                            for rival in equipo['rivalidad']:
                                print(f"      → {rival.get('nombre', 'N/A')} ({rival.get('pais', 'N/A')})")
                else:
                    print("\nNo se encontró historial de equipos")
            else:
                print("No se encontró el jugador")
            return ""
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_player_seniority(self, name, last_name):
        """
        No docstring >:(
        """
        try:
            result = self._dgraph.consultar_antiguedad_jugador(name, last_name)
            if result.get('jugador') and len(result['jugador']) > 0:
                jugador = result['jugador'][0]
                print(f"\n{'='*60}")
                print(f"ANTIGÜEDAD: {jugador.get('nombre', '')} {jugador.get('apellido', '')}")
                print(f"{'='*60}")

                if jugador.get('juega_para'):
                    for equipo in jugador['juega_para']:
                        print(f"\nEquipo: {equipo.get('nombre', 'N/A')}")
                        print(f"Liga: {equipo.get('liga', 'N/A')}")
                        fecha_inicio = clean_date(equipo.get('juega_para|fechaInicio', 'N/A'))
                        print(f"Fecha de ingreso: {fecha_inicio}")

                if result.get('temporadas'):
                    temporadas_unicas = {}
                    for enf in result['temporadas']:
                        if enf.get('temporada'):
                            temp = enf['temporada']
                            temp_nombre = temp.get('nombre')
                            if temp_nombre and temp_nombre not in temporadas_unicas:
                                temporadas_unicas[temp_nombre] = temp

                    if temporadas_unicas:
                        print(f"\nTemporadas disputadas: {len(temporadas_unicas)}")
                        print("\nDetalle por temporada:")
                        for i, temp in enumerate(sorted(temporadas_unicas.values(), key=lambda x: x.get('anio', 0)), 1):
                            print(f"  [{i}] {temp.get('nombre', 'N/A')} - {temp.get('liga', 'N/A')} ({temp.get('anio', 'N/A')})")
                            print(f"      {clean_date(temp.get('fechaInicio', 'N/A'))} → {clean_date(temp.get('fechaFin', 'N/A'))}")
                else:
                    print("\nNo se encontraron temporadas disputadas")
            else:
                print("No se encontró el jugador")
            return ""
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_home_advantage(self, team):
        """
        No docstring >:(
        """
        try:
            result = self._dgraph.consultar_impacto_localia(team)
            if result.get('equipo') and len(result['equipo']) > 0:
                equipo = result['equipo'][0]
                print(f"\n{'='*60}")
                print(f"IMPACTO DE LOCALÍA: {equipo.get('nombre', 'N/A').upper()}")
                print(f"{'='*60}")

                if equipo.get('campo_local'):
                    campo = equipo['campo_local']
                    print(f"\nEstadio local: {campo.get('nombre', 'N/A')} ({campo.get('pais', 'N/A')})")

                partidos_local = result.get('local', [])
                partidos_visitante = result.get('visitante', [])

                print(f"\n--- COMO LOCAL (en su estadio) ---")
                print(f"Total partidos: {len(partidos_local)}")
                if partidos_local:
                    victorias = sum(1 for p in partidos_local if 'Victoria Local' in p.get('resultado', ''))
                    print(f"Victorias: {victorias}")
                    for i, p in enumerate(partidos_local, 1):
                        print(f"\n  [{i}] {clean_date(p.get('fecha', 'N/A'))}")
                        print(f"      vs {p.get('equipo_visitante', {}).get('nombre', 'N/A')}")
                        print(f"      {p.get('marcadorLocal', 0)} - {p.get('marcadorVisitante', 0)} ({p.get('resultado', 'N/A')})")

                print(f"\n--- COMO VISITANTE (fuera de casa) ---")
                print(f"Total partidos: {len(partidos_visitante)}")
                if partidos_visitante:
                    victorias = sum(1 for p in partidos_visitante if 'Victoria Local' in p.get('resultado', ''))
                    print(f"Victorias como local: {victorias}")
                    for i, p in enumerate(partidos_visitante, 1):
                        print(f"\n  [{i}] {clean_date(p.get('fecha', 'N/A'))}")
                        if p.get('campo'):
                            print(f"      Estadio: {p['campo'].get('nombre', 'N/A')} ({p['campo'].get('pais', 'N/A')})")
                        print(f"      vs {p.get('equipo_visitante', {}).get('nombre', 'N/A')}")
                        print(f"      {p.get('marcadorLocal', 0)} - {p.get('marcadorVisitante', 0)} ({p.get('resultado', 'N/A')})")
            else:
                print("No se encontró el equipo")
            return ""
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)

    def get_team_seasons(self, team):
        """
        No docstring >:(
        """
        try:
            result = self._dgraph.consultar_temporadas_equipo(team)
            if result.get('enfrentamientos'):
                print(f"\n{'='*60}")
                print(f"TEMPORADAS DE {team.upper()}")
                print(f"{'='*60}")

                # Agrupar por temporada
                temporadas_dict = {}
                for enf in result['enfrentamientos']:
                    if enf.get('temporada'):
                        temp = enf['temporada']
                        temp_nombre = temp.get('nombre')
                        if temp_nombre not in temporadas_dict:
                            temporadas_dict[temp_nombre] = {
                                'info': temp,
                                'enfrentamientos': []
                            }
                        temporadas_dict[temp_nombre]['enfrentamientos'].append(enf)

                print(f"\nTotal temporadas: {len(temporadas_dict)}")

                for i, (nombre_temp, data) in enumerate(sorted(temporadas_dict.items(),
                                                               key=lambda x: x[1]['info'].get('anio', 0)), 1):
                    temp_info = data['info']
                    partidos = data['enfrentamientos']

                    print(f"\n{'─'*60}")
                    print(f"[{i}] TEMPORADA: {temp_info.get('nombre', 'N/A')}")
                    print(f"    Liga: {temp_info.get('liga', 'N/A')} | Año: {temp_info.get('anio', 'N/A')}")
                    print(f"    Periodo: {clean_date(temp_info.get('fechaInicio', 'N/A'))} → {clean_date(temp_info.get('fechaFin', 'N/A'))}")
                    print(f"    Partidos jugados: {len(partidos)}")

                    print(f"\n    Enfrentamientos:")
                    for j, p in enumerate(partidos, 1):
                        local = p.get('equipo_local', {}).get('nombre', 'N/A')
                        visitante = p.get('equipo_visitante', {}).get('nombre', 'N/A')
                        print(f"      [{j}] {clean_date(p.get('fecha', 'N/A'))}: {local} {p.get('marcadorLocal', 0)} - {p.get('marcadorVisitante', 0)} {visitante}")
                        print(f"          {p.get('resultado', 'N/A')}")
            else:
                print("No se encontraron enfrentamientos para este equipo")
            return ""
        except Exception as e:
            return "Hubo un error en la base de datos. Error: " + str(e)
