"""
Main file that interacts with the user
"""
import sys
from connect import DatabaseFacade

MENU = """
------------------------------------------------------------
    MENU:
    ++++++++++++++++++
    1- Poblar base de datos
    2- Consultar jugadores
    3- Consultar partidos
    4- Consultar equipos
    5- Consultar ligas
    6- Borrar base de datos
    7- Salir
------------------------------------------------------------
"""

MENU_PLAYERS = """
------------------------------------------------------------
    CONSULTAR JUGADORES:
    ++++++++++++++++++
    1- Consultar toda la informacion de un jugador por su nombre y apellido
    2- Consultar lo puntos que ha anotado un jugador en un partido por nombre de jugador e id de partido
    3- Consultar todos los compañeros de cierto jugador por su nombre y apellido
    4- Regresar
------------------------------------------------------------
"""

MENU_MATCHES = """
------------------------------------------------------------
    CONSULTAR PARTIDOS:
    ++++++++++++++++++
    1- Consultar partidos en cierta fecha en cierto deporte
    2- Consultar los puntos que ha anotado un jugador en un partido por nombre de jugador e id de partido
    3- Obtener los eventos que ha tenido un equipo en cierto partido por nombre de equipo e id de partido
    4- Obtener todos los enfrentamientos que ha habido en un estadio
    5- Regresar
------------------------------------------------------------
"""

MENU_TEAMS = """
------------------------------------------------------------
    CONSULTAR EQUIPOS:
    ++++++++++++++++++
    1- Consultar toda la informacion (nombre, deporte, pais, region, jugadores) de un equipo por su nombre
    2- Obtener los eventos que ha tenido un equipo en cierto partido por nombre de equipo e id de partido
    3- Obtener los enfrentamientos de un equipo en cierta temporada
    4- Obtener los equipos que juegan como locales en cierto estadio o viceversa
    5- Ranking de equipos por deporte
    6- Obtener los primeros lugares de todos los deportes
    7- Regresar
------------------------------------------------------------
"""

MENU_LEAGUES = """
------------------------------------------------------------
    CONSULTAR LIGAS:
    ++++++++++++++++++
    1- Consultar todas las estadisticas de un torneo por su nombre y su temporada
    2- Obtener todas las ligas de un deporte
    3- Regresar
------------------------------------------------------------
"""
OPCIONES = {
    2: "PLAYERS",
    3: "MATCHES",
    4: "TEAMS",
    5: "LEAGUES"
}

if __name__ == "__main__":
    database_controller = DatabaseFacade()
    estado = "MENU"
    while True:
        match estado:
            case "MENU":
                print(MENU)
                response = input()
                if not response.isdigit():
                    print("Por favor, responde con el numero de la opcion que necesites")
                    continue
                if OPCIONES.get(int(response)):
                    estado = OPCIONES[int(response)]
                    continue
                if int(response) == 1:
                    database_controller.populate_databases()
                    continue
                if int(response) == 6:
                    database_controller.delete_databases()
                    continue
                if int(response) == 7:
                    sys.exit()
                print("Opcion invalida. Seleccione una opcion del 1-7")

            case "PLAYERS":
                print(MENU_PLAYERS)
                response = input()
                if not response.isdigit():
                    print("Por favor, responde con el numero de la opcion que necesites")
                    continue
                response = int(response)
                if response == 1:
                    nombre = input("Nombre del jugador: ")
                    apellido = input("Apellido del jugador: ")
                    print(database_controller.get_player_info(nombre, apellido))
                    continue
                if response == 2:
                    nombre = input("Nombre del jugador: ")
                    apellido = input("Apellido del jugador: ")
                    partido_id = input("Id del partido: ")
                    print(
                        database_controller.get_points_scored_by_player_match(
                            nombre, apellido, partido_id
                        )
                    )
                    continue
                if response == 3:
                    nombre = input("Nombre del jugador: ")
                    apellido = input("Apellido del jugador: ")
                    print(database_controller.get_player_teammates(nombre, apellido))
                    continue
                if response == 4:
                    estado = "MENU"
                    continue
                print("Opcion invalida. Seleccione una opcion del 1-5")

            case "MATCHES":
                print(MENU_MATCHES)
                response = input()
                if not response.isdigit():
                    print("Por favor, responde con el numero de la opcion que necesites")
                    continue
                response = int(response)
                if response == 1:
                    deporte = input("Deporte: ")
                    fecha = input("Fecha: ")
                    print(database_controller.get_matches_by_date_sport(deporte, fecha))
                    continue
                if response == 2:
                    nombre = input("Nombre del jugador: ")
                    apellido = input("Apellido del jugador: ")
                    partido_id = input("Id del partido: ")
                    print(
                        database_controller.get_points_scored_by_player_match(
                            nombre, apellido, partido_id
                        )
                    )
                    continue
                if response == 3:
                    equipo = input("Nombre del equipo: ")
                    partido_id = input("Id del partido: ")
                    print(database_controller.get_events_by_team_match(equipo, partido_id))
                    continue
                if response == 4:
                    estadio = input("Nombre del estadio: ")
                    print(database_controller.get_matches_by_stadium(estadio))
                    continue 
                if response == 5:
                    estado = "MENU"
                    continue
                print("Opcion invalida. Seleccione una opcion del 1-5")

            case "TEAMS":
                print(MENU_TEAMS)
                response = input()
                if not response.isdigit():
                    print("Por favor, responde con el numero de la opcion que necesites")
                    continue
                response = int(response)
                if response == 1:
                    equipo = input("Nombre del equipo: ")
                    print(database_controller.get_team_info(equipo))
                    continue
                if response == 2:
                    equipo = input("Nombre del equipo: ")
                    partido_id = input("Id del partido: ")
                    print(database_controller.get_events_by_team_match(equipo, partido_id))
                    continue
                if response == 3:
                    equipo = input("Nombre del equipo: ")
                    temporada = input("Temporada: ")
                    print(database_controller.get_matches_by_team_season(equipo, temporada))
                    continue
                if response == 4:
                    estadio = input("Estadio: ")
                    print(database_controller.get_teams_by_stadium(estadio))
                    continue
                if response == 5:
                    deporte = input("Deporte: ")
                    print(database_controller.get_team_reanking_by_sport(deporte))
                    continue
                if response == 6:
                    print(database_controller.get_first_places_from_all_sports())
                    continue
                if response == 7:
                    estado = "MENU"
                    continue
                print("Opcion invalida. Seleccione una opcion del 1-8")

            case "LEAGUES":
                print(MENU_LEAGUES)
                response = input()
                if not response.isdigit():
                    print("Por favor, responde con el numero de la opcion que necesites")
                    continue
                response = int(response)
                if response == 1:
                    liga = input("Nombre del torneo: ")
                    temporada = input("Temporada: ")
                    print(database_controller.get_league_stats_by_season(liga, temporada))
                    continue
                if response == 2:
                    deporte = input("Deporte: ")
                    print(database_controller.get_all_leagues_by_sport(deporte))
                    continue
                if response == 3:
                    estado = "MENU"
                    continue
                print("Opcion invalida. Seleccione una opcion del 1-3")
