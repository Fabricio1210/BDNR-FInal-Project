"""
Main file that interacts with the user
"""
import sys
import json
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
    2- Obtener los eventos que ha tenido un equipo en cierto partido por nombre de equipo e id de partido
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
                    # Consultar toda la información de un jugador
                    nombre = input("Ingrese el nombre del jugador: ").strip()
                    apellido = input("Ingrese el apellido del jugador: ").strip()
                    try:
                        resultado = database_controller.consultar_jugador_completo(nombre, apellido)
                        if resultado.get('jugador'):
                            print("\n" + "="*60)
                            print("INFORMACIÓN DEL JUGADOR")
                            print("="*60)
                            for jugador in resultado['jugador']:
                                print(f"\nNombre: {jugador.get('nombre', '')} {jugador.get('apellido', '')}")
                                print(f"Número: {jugador.get('numero', 'N/A')}")
                                print(f"País: {jugador.get('pais', 'N/A')}")
                                print(f"Fecha de Nacimiento: {jugador.get('fechaNacimiento', 'N/A')}")

                                if jugador.get('juega_para'):
                                    print("\nHISTORIAL DE EQUIPOS:")
                                    for i, equipo in enumerate(jugador['juega_para'], 1):
                                        print(f"\n  [{i}] {equipo.get('nombre', 'N/A')}")
                                        print(f"      Liga: {equipo.get('liga', 'N/A')}")
                                        print(f"      País: {equipo.get('pais', 'N/A')}")
                                        print(f"      Ciudad: {equipo.get('ciudad', 'N/A')}")

                                        # Mostrar facets si existen
                                        if 'juega_para|fechaInicio' in equipo:
                                            print(f"      Fecha Inicio: {equipo.get('juega_para|fechaInicio', 'N/A')}")
                                        if 'juega_para|fechaFin' in equipo:
                                            print(f"      Fecha Fin: {equipo.get('juega_para|fechaFin', 'N/A')}")

                                        if equipo.get('campo_local'):
                                            campo = equipo['campo_local']
                                            print(f"      Estadio Local: {campo.get('nombre', 'N/A')} ({campo.get('pais', 'N/A')})")
                            print("="*60 + "\n")
                        else:
                            print(f"\nNo se encontró ningún jugador con nombre '{nombre} {apellido}'\n")
                    except Exception as e:
                        print(f"\nError al consultar jugador: {e}\n")
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
                    # Obtener todos los enfrentamientos en un estadio
                    nombre_campo = input("Ingrese el nombre del estadio: ").strip()
                    try:
                        resultado = database_controller.consultar_enfrentamientos_estadio(nombre_campo)
                        if resultado.get('enfrentamientos'):
                            print("\n" + "="*60)
                            print(f"ENFRENTAMIENTOS EN {nombre_campo.upper()}")
                            print("="*60)
                            for i, partido in enumerate(resultado['enfrentamientos'], 1):
                                print(f"\n[{i}] {partido.get('fecha', 'N/A')}")
                                local = partido.get('equipo_local', {})
                                visitante = partido.get('equipo_visitante', {})
                                print(f"    {local.get('nombre', 'N/A')} vs {visitante.get('nombre', 'N/A')}")
                                print(f"    Marcador: {partido.get('marcadorLocal', 0)} - {partido.get('marcadorVisitante', 0)}")
                                print(f"    Resultado: {partido.get('resultado', 'N/A')}")
                                print(f"    Asistencia: {partido.get('asistencia', 'N/A'):,} personas")

                                if partido.get('temporada'):
                                    temp = partido['temporada']
                                    print(f"    Temporada: {temp.get('nombre', 'N/A')} - {temp.get('liga', 'N/A')} ({temp.get('anio', 'N/A')})")

                                if partido.get('campo'):
                                    campo = partido['campo']
                                    print(f"    Estadio: {campo.get('nombre', 'N/A')} (Capacidad: {campo.get('capacidad', 'N/A'):,})")
                            print("\n" + "="*60 + "\n")
                        else:
                            print(f"\nNo se encontraron enfrentamientos en el estadio '{nombre_campo}'\n")
                    except Exception as e:
                        print(f"\nError al consultar enfrentamientos: {e}\n")
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
                    continue
                if response == 2:
                    continue
                if response == 3:
                    # Obtener enfrentamientos de un equipo en cierta temporada
                    nombre_equipo = input("Ingrese el nombre del equipo: ").strip()
                    nombre_temporada = input("Ingrese el nombre de la temporada: ").strip()
                    try:
                        resultado = database_controller.consultar_enfrentamientos_equipo_temporada(nombre_equipo, nombre_temporada)
                        if resultado.get('enfrentamientos'):
                            print("\n" + "="*60)
                            print(f"ENFRENTAMIENTOS DE {nombre_equipo.upper()}")
                            print(f"TEMPORADA: {nombre_temporada}")
                            print("="*60)
                            for i, partido in enumerate(resultado['enfrentamientos'], 1):
                                print(f"\n[{i}] {partido.get('fecha', 'N/A')}")
                                local = partido.get('equipo_local', {})
                                visitante = partido.get('equipo_visitante', {})

                                # Determinar si el equipo consultado es local o visitante
                                es_local = local.get('nombre', '') == nombre_equipo
                                rival = visitante if es_local else local

                                print(f"    {local.get('nombre', 'N/A')} vs {visitante.get('nombre', 'N/A')}")
                                print(f"    Marcador: {partido.get('marcadorLocal', 0)} - {partido.get('marcadorVisitante', 0)}")
                                print(f"    Resultado: {partido.get('resultado', 'N/A')}")
                                print(f"    Rival: {rival.get('nombre', 'N/A')} ({rival.get('pais', 'N/A')})")

                                if partido.get('campo'):
                                    campo = partido['campo']
                                    print(f"    Estadio: {campo.get('nombre', 'N/A')} - {campo.get('pais', 'N/A')}")
                                    print(f"    Tipo: {campo.get('tipo', 'N/A')} | Capacidad: {campo.get('capacidad', 'N/A'):,}")

                                if partido.get('asistencia'):
                                    print(f"    Asistencia: {partido.get('asistencia'):,} personas")
                            print("\n" + "="*60 + "\n")
                        else:
                            print(f"\nNo se encontraron enfrentamientos para '{nombre_equipo}' en la temporada '{nombre_temporada}'\n")
                    except Exception as e:
                        print(f"\nError al consultar enfrentamientos: {e}\n")
                    continue
                if response == 4:
                    # Obtener equipos locales en un estadio o campos de un equipo
                    print("\nSeleccione una opción:")
                    print("1- Equipos que juegan como locales en un estadio")
                    print("2- Campos donde juega un equipo")
                    opcion = input("Opción: ").strip()

                    if opcion == "1":
                        # Equipos locales en un estadio
                        nombre_campo = input("Ingrese el nombre del estadio: ").strip()
                        try:
                            resultado = database_controller.consultar_equipos_locales_estadio(nombre_campo)
                            if resultado.get('campo'):
                                for campo in resultado['campo']:
                                    print("\n" + "="*60)
                                    print(f"ESTADIO: {campo.get('nombre', 'N/A').upper()}")
                                    print("="*60)
                                    print(f"País: {campo.get('pais', 'N/A')}")
                                    print(f"Capacidad: {campo.get('capacidad', 'N/A'):,}")
                                    print(f"Tipo: {campo.get('tipo', 'N/A')}")

                                    if campo.get('equipos_locales'):
                                        print(f"\nEQUIPOS LOCALES ({len(campo['equipos_locales'])}):")
                                        for i, equipo in enumerate(campo['equipos_locales'], 1):
                                            print(f"\n  [{i}] {equipo.get('nombre', 'N/A')}")
                                            print(f"      Liga: {equipo.get('liga', 'N/A')}")
                                            print(f"      País: {equipo.get('pais', 'N/A')}")
                                            print(f"      Ciudad: {equipo.get('ciudad', 'N/A')}")

                                            if equipo.get('jugadores'):
                                                print(f"      Plantel ({len(equipo['jugadores'])} jugadores):")
                                                for j, jugador in enumerate(equipo['jugadores'], 1):
                                                    print(f"        {j}. {jugador.get('nombre', '')} {jugador.get('apellido', '')} "
                                                          f"(#{jugador.get('numero', 'N/A')}) - {jugador.get('pais', 'N/A')}")
                                    else:
                                        print("\n  No hay equipos locales registrados en este estadio")
                                    print("="*60 + "\n")
                            else:
                                print(f"\nNo se encontró el estadio '{nombre_campo}'\n")
                        except Exception as e:
                            print(f"\nError al consultar equipos locales: {e}\n")

                    elif opcion == "2":
                        # Campos donde juega un equipo
                        nombre_equipo = input("Ingrese el nombre del equipo: ").strip()
                        try:
                            resultado = database_controller.consultar_campos_equipo(nombre_equipo)
                            if resultado.get('equipo'):
                                for equipo in resultado['equipo']:
                                    print("\n" + "="*60)
                                    print(f"EQUIPO: {equipo.get('nombre', 'N/A').upper()}")
                                    print("="*60)
                                    print(f"País: {equipo.get('pais', 'N/A')}")

                                    if equipo.get('campo_local'):
                                        campo = equipo['campo_local']
                                        print(f"\nESTADIO LOCAL:")
                                        print(f"  Nombre: {campo.get('nombre', 'N/A')}")
                                        print(f"  País: {campo.get('pais', 'N/A')}")
                                        print(f"  Capacidad: {campo.get('capacidad', 'N/A'):,}")
                                        print(f"  Tipo: {campo.get('tipo', 'N/A')}")

                                # Recopilar campos de enfrentamientos
                                campos_visitados = set()
                                for enfr_tipo in ['enfrentamientos_local', 'enfrentamientos_visitante']:
                                    if resultado.get(enfr_tipo):
                                        for partido in resultado[enfr_tipo]:
                                            if partido.get('campo'):
                                                campo = partido['campo']
                                                campos_visitados.add((
                                                    campo.get('nombre', 'N/A'),
                                                    campo.get('pais', 'N/A'),
                                                    campo.get('capacidad', 0),
                                                    campo.get('tipo', 'N/A')
                                                ))

                                if campos_visitados:
                                    print(f"\nOTROS CAMPOS DONDE HA JUGADO ({len(campos_visitados)}):")
                                    for i, (nombre, pais, capacidad, tipo) in enumerate(sorted(campos_visitados), 1):
                                        print(f"  [{i}] {nombre} - {pais}")
                                        print(f"      Capacidad: {capacidad:,} | Tipo: {tipo}")
                                print("="*60 + "\n")
                            else:
                                print(f"\nNo se encontró el equipo '{nombre_equipo}'\n")
                        except Exception as e:
                            print(f"\nError al consultar campos del equipo: {e}\n")
                    else:
                        print("\nOpción inválida\n")
                    continue
                if response == 5:
                    continue
                if response == 6:
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
                    continue
                if response == 2:
                    continue
                if response == 3:
                    estado = "MENU"
                    continue
                print("Opcion invalida. Seleccione una opcion del 1-3")
