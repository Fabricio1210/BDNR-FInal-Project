"""
    Modelos y controladores de dgraph
"""
import logging
import os
import csv
import json
from datetime import datetime
import pydgraph
from Dgraph import schema

log = logging.getLogger()

class DgraphSingleton:
    """
    No docstring :)
    """

    _instance = None

    def _init_instance(self):
        addrs = os.getenv("DGRAPH_ALPHA_ADDRS", "127.0.0.1:9080")
        self._stub = pydgraph.DgraphClientStub(addrs)
        self.client = pydgraph.DgraphClient(self._stub)

    def __new__(cls):
        if cls._instance:
            return cls._instance
        cls._instance = super().__new__(cls)
        cls._instance._init_instance()
        return cls._instance

    def shutdown(self):
        """
        No docstring :)
        """
        self._stub.close()

class DgraphService:
    """
    No docstring :)
    """
    def __init__(self):
        self._dgraph_client = DgraphSingleton()
        self._dgraph_client.client.alter(pydgraph.Operation(schema=schema.SCHEMA))

    def insert_nodes(self, statement):
        """
        No docstring :)
        """

    def consultar_jugador_completo(self, nombre, apellido):
        """
        Consulta recursiva que devuelve toda la información de un jugador
        incluyendo su historial de equipos con facets de fechaInicio y fechaFin
        """
        query = """
        query jugador_info($nombre: string, $apellido: string) {
            jugador(func: type(Jugador)) @filter(eq(nombre, $nombre) AND eq(apellido, $apellido)) {
                uid
                nombre
                apellido
                numero
                fechaNacimiento
                pais
                juega_para @facets {
                    uid
                    nombre
                    liga
                    pais
                    ciudad
                    campo_local {
                        nombre
                        pais
                        capacidad
                        tipo
                    }
                }
            }
        }
        """
        variables = {'$nombre': nombre, '$apellido': apellido}
        txn = self._dgraph_client.client.txn(read_only=True)
        try:
            res = txn.query(query, variables=variables)
            return json.loads(res.json)
        finally:
            txn.discard()

    def consultar_enfrentamientos_equipo_temporada(self, nombre_equipo, nombre_temporada):
        """
        Retorna todos los enfrentamientos de un equipo en una temporada específica
        mostrando equipo rival, resultado y campo
        """
        query = """
        query enfrentamientos($nombre_equipo: string, $nombre_temporada: string) {
            var(func: type(Equipo)) @filter(eq(nombre, $nombre_equipo)) {
                E as uid
            }

            var(func: type(Temporada)) @filter(eq(nombre, $nombre_temporada)) {
                T as uid
            }

            enfrentamientos(func: type(Enfrentamiento)) @filter(uid_in(temporada, uid(T)) AND (uid_in(equipo_local, uid(E)) OR uid_in(equipo_visitante, uid(E)))) {
                uid
                fecha
                marcadorLocal
                marcadorVisitante
                resultado
                asistencia
                equipo_local {
                    uid
                    nombre
                    pais
                }
                equipo_visitante {
                    uid
                    nombre
                    pais
                }
                campo {
                    nombre
                    pais
                    capacidad
                    tipo
                }
                temporada {
                    nombre
                    liga
                    anio
                }
            }
        }
        """
        variables = {'$nombre_equipo': nombre_equipo, '$nombre_temporada': nombre_temporada}
        txn = self._dgraph_client.client.txn(read_only=True)
        try:
            res = txn.query(query, variables=variables)
            return json.loads(res.json)
        finally:
            txn.discard()

    def consultar_equipos_locales_estadio(self, nombre_campo):
        """
        Consulta bidireccional que retorna todos los equipos que juegan
        como locales en un estadio, mostrando su plantel
        """
        query = """
        query equipos_estadio($nombre_campo: string) {
            campo(func: type(Campo)) @filter(eq(nombre, $nombre_campo)) {
                uid
                nombre
                pais
                capacidad
                tipo
                equipos_locales {
                    uid
                    nombre
                    liga
                    pais
                    ciudad
                    jugadores {
                        nombre
                        apellido
                        numero
                        pais
                    }
                }
            }
        }
        """
        variables = {'$nombre_campo': nombre_campo}
        txn = self._dgraph_client.client.txn(read_only=True)
        try:
            res = txn.query(query, variables=variables)
            return json.loads(res.json)
        finally:
            txn.discard()

    def consultar_campos_equipo(self, nombre_equipo):
        """
        Retorna el campo local de un equipo y todos los campos donde ha jugado
        """
        query = """
        query campos_equipo($nombre_equipo: string) {
            equipo(func: type(Equipo)) @filter(eq(nombre, $nombre_equipo)) {
                uid
                nombre
                pais
                campo_local {
                    uid
                    nombre
                    pais
                    capacidad
                    tipo
                }
            }

            var(func: type(Equipo)) @filter(eq(nombre, $nombre_equipo)) {
                E as uid
            }

            enfrentamientos_visitante(func: type(Enfrentamiento)) @filter(uid_in(equipo_visitante, uid(E))) {
                campo {
                    uid
                    nombre
                    pais
                    capacidad
                    tipo
                }
            }

            enfrentamientos_local(func: type(Enfrentamiento)) @filter(uid_in(equipo_local, uid(E))) {
                campo {
                    uid
                    nombre
                    pais
                    capacidad
                    tipo
                }
            }
        }
        """
        variables = {'$nombre_equipo': nombre_equipo}
        txn = self._dgraph_client.client.txn(read_only=True)
        try:
            res = txn.query(query, variables=variables)
            return json.loads(res.json)
        finally:
            txn.discard()

    def consultar_enfrentamientos_estadio(self, nombre_campo):
        """
        Retorna todos los enfrentamientos que han ocurrido en un estadio
        mostrando equipos, temporada, fecha, marcador y asistencia
        """
        query = """
        query enfrentamientos_campo($nombre_campo: string) {
            var(func: type(Campo)) @filter(eq(nombre, $nombre_campo)) {
                C as uid
            }

            enfrentamientos(func: type(Enfrentamiento)) @filter(uid_in(campo, uid(C))) {
                uid
                fecha
                marcadorLocal
                marcadorVisitante
                resultado
                asistencia
                equipo_local {
                    uid
                    nombre
                    liga
                    pais
                }
                equipo_visitante {
                    uid
                    nombre
                    liga
                    pais
                }
                temporada {
                    nombre
                    liga
                    anio
                    fechaInicio
                    fechaFin
                }
                campo {
                    nombre
                    pais
                    capacidad
                }
            }
        }
        """
        variables = {'$nombre_campo': nombre_campo}
        txn = self._dgraph_client.client.txn(read_only=True)
        try:
            res = txn.query(query, variables=variables)
            return json.loads(res.json)
        finally:
            txn.discard()

    def drop_all(self):
        """
        Elimina todos los datos de Dgraph (conserva el schema)
        """
        try:
            self._dgraph_client.client.alter(pydgraph.Operation(drop_all=True))
            # Reaplica el schema después del drop
            self._dgraph_client.client.alter(pydgraph.Operation(schema=schema.SCHEMA))
            log.info("Dgraph: Todos los datos eliminados y schema reaplicado")
        except Exception as e:
            log.error(f"Error al eliminar datos de Dgraph: {e}")
            raise

    def populate_data(self):
        """
        Puebla Dgraph con datos desde los archivos CSV
        """
        try:
            log.info("Iniciando población de Dgraph...")

            # Primero eliminamos todos los datos
            self.drop_all()

            # Mapas para almacenar UIDs generados
            equipo_uids = {}  # {csv_id: uid}
            campo_uids = {}   # {nombre_campo: uid}
            jugador_uids = {} # {nombre_apellido: uid}
            temporada_uids = {} # {torneo_nombre_temporada: uid}

            # 1. Poblar Campos
            log.info("Poblando nodo Campo...")
            campos_file = os.path.join("data", "campos.csv")
            if os.path.exists(campos_file):
                with open(campos_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    txn = self._dgraph_client.client.txn()
                    try:
                        for row in reader:
                            campo = {
                                "dgraph.type": "Campo",
                                "nombre": row['nombre'],
                                "pais": row['pais'],
                                "capacidad": int(row['capacidad']),
                                "tipo": row['tipo']
                            }
                            response = txn.mutate(set_obj=campo, commit_now=False)
                            uid = list(response.uids.values())[0]
                            campo_uids[row['nombre']] = uid
                        txn.commit()
                        log.info(f"Campos insertados: {len(campo_uids)}")
                    finally:
                        txn.discard()

            # 2. Poblar Equipos
            log.info("Poblando nodo Equipo...")
            equipos_file = os.path.join("data", "equipos.csv")
            with open(equipos_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                txn = self._dgraph_client.client.txn()
                try:
                    campos_data = {}
                    if os.path.exists(campos_file):
                        with open(campos_file, 'r', encoding='utf-8') as cf:
                            campos_reader = csv.DictReader(cf)
                            for campo_row in campos_reader:
                                campos_data[campo_row['equipo_local_csv_id']] = campo_row['nombre']

                    for row in reader:
                        equipo_csv_id = row['jugadores_ids']
                        equipo = {
                            "dgraph.type": "Equipo",
                            "nombre": row['nombre'],
                            "liga": row.get('deporte', ''),
                            "pais": row['pais'],
                            "ciudad": row.get('region', '')
                        }

                        # Asignar campo_local si existe
                        if equipo_csv_id in campos_data:
                            campo_nombre = campos_data[equipo_csv_id]
                            if campo_nombre in campo_uids:
                                equipo["campo_local"] = {"uid": campo_uids[campo_nombre]}

                        response = txn.mutate(set_obj=equipo, commit_now=False)
                        uid = list(response.uids.values())[0]
                        equipo_uids[equipo_csv_id] = uid
                    txn.commit()
                    log.info(f"Equipos insertados: {len(equipo_uids)}")
                finally:
                    txn.discard()

            # 3. Poblar Jugadores
            log.info("Poblando nodo Jugador...")
            jugadores_file = os.path.join("data", "jugadores.csv")
            with open(jugadores_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                txn = self._dgraph_client.client.txn()
                try:
                    for row in reader:
                        # Parsear fecha de nacimiento
                        fecha_nac = row['fecha_nacimiento']
                        fecha_nac_dt = datetime.strptime(fecha_nac, "%Y-%m-%d")

                        jugador = {
                            "dgraph.type": "Jugador",
                            "nombre": row['nombre'],
                            "apellido": row['apellido'],
                            "numero": int(row['numero']) if row['numero'] else 0,
                            "fechaNacimiento": fecha_nac + "T00:00:00Z",
                            "pais": row['pais_origen']
                        }

                        # Conectar con equipo actual con facets
                        equipo_id = row['equipo_id']
                        if equipo_id in equipo_uids:
                            # Asumimos que el jugador se unió al equipo actual hace 2 años
                            fecha_inicio = f"{fecha_nac_dt.year + 18}-01-01T00:00:00Z"

                            jugador["juega_para"] = [{
                                "uid": equipo_uids[equipo_id],
                                "juega_para|fechaInicio": fecha_inicio,
                                "juega_para|fechaFin": "2024-12-31T23:59:59Z"
                            }]

                        response = txn.mutate(set_obj=jugador, commit_now=False)
                        uid = list(response.uids.values())[0]
                        key = f"{row['nombre']}_{row['apellido']}"
                        jugador_uids[key] = uid
                    txn.commit()
                    log.info(f"Jugadores insertados: {len(jugador_uids)}")
                finally:
                    txn.discard()

            # 4. Poblar Temporadas desde estadisticas_torneos.csv
            log.info("Poblando nodo Temporada...")
            stats_file = os.path.join("data", "estadisticas_torneos.csv")
            temporadas_creadas = set()
            with open(stats_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                txn = self._dgraph_client.client.txn()
                try:
                    for row in reader:
                        temporada_key = f"{row['torneo_nombre']}_{row['temporada']}"
                        if temporada_key not in temporadas_creadas:
                            # Parsear año de la temporada
                            anio = int(row['temporada'].split('-')[0]) if '-' in row['temporada'] else 2024

                            temporada = {
                                "dgraph.type": "Temporada",
                                "anio": anio,
                                "nombre": row['temporada'],
                                "liga": row['torneo_nombre'],
                                "fechaInicio": f"{anio}-01-01T00:00:00Z",
                                "fechaFin": f"{anio}-12-31T23:59:59Z"
                            }
                            response = txn.mutate(set_obj=temporada, commit_now=False)
                            uid = list(response.uids.values())[0]
                            temporada_uids[temporada_key] = uid
                            temporadas_creadas.add(temporada_key)
                    txn.commit()
                    log.info(f"Temporadas insertadas: {len(temporada_uids)}")
                finally:
                    txn.discard()

            # 5. Poblar Enfrentamientos
            log.info("Poblando nodo Enfrentamiento...")
            partidos_file = os.path.join("data", "partidos.csv")
            with open(partidos_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                txn = self._dgraph_client.client.txn()
                try:
                    for row in reader:
                        # Generar asistencia aleatoria basada en el deporte
                        import random
                        if row['deporte'] == 'Futbol':
                            asistencia = random.randint(10000, 60000)
                        else:  # Baloncesto
                            asistencia = random.randint(15000, 21000)

                        enfrentamiento = {
                            "dgraph.type": "Enfrentamiento",
                            "fecha": row['fecha'] + "T00:00:00Z",
                            "marcadorLocal": int(row['goles_local']),
                            "marcadorVisitante": int(row['goles_visitante']),
                            "asistencia": asistencia
                        }

                        # Determinar resultado
                        if int(row['goles_local']) > int(row['goles_visitante']):
                            enfrentamiento["resultado"] = "Victoria Local"
                        elif int(row['goles_local']) < int(row['goles_visitante']):
                            enfrentamiento["resultado"] = "Victoria Visitante"
                        else:
                            enfrentamiento["resultado"] = "Empate"

                        # Conectar equipos
                        if row['equipo_local_csv_id'] in equipo_uids:
                            enfrentamiento["equipo_local"] = {"uid": equipo_uids[row['equipo_local_csv_id']]}
                        if row['equipo_visitante_csv_id'] in equipo_uids:
                            enfrentamiento["equipo_visitante"] = {"uid": equipo_uids[row['equipo_visitante_csv_id']]}

                        # Conectar campo (el del equipo local)
                        if os.path.exists(campos_file):
                            with open(campos_file, 'r', encoding='utf-8') as cf:
                                campos_reader = csv.DictReader(cf)
                                for campo_row in campos_reader:
                                    if campo_row['equipo_local_csv_id'] == row['equipo_local_csv_id']:
                                        if campo_row['nombre'] in campo_uids:
                                            enfrentamiento["campo"] = {"uid": campo_uids[campo_row['nombre']]}
                                        break

                        # Conectar temporada
                        temporada_key = f"{row['torneo_nombre']}_2024-Apertura"
                        if temporada_key in temporada_uids:
                            enfrentamiento["temporada"] = {"uid": temporada_uids[temporada_key]}

                        txn.mutate(set_obj=enfrentamiento, commit_now=False)
                    txn.commit()
                    log.info("Enfrentamientos insertados exitosamente")
                finally:
                    txn.discard()

            # 6. Crear rivalidades entre equipos que se han enfrentado
            log.info("Creando rivalidades...")
            self._crear_rivalidades(equipo_uids)

            log.info("Población de Dgraph completada exitosamente")
            return True

        except Exception as e:
            log.error(f"Error al poblar Dgraph: {e}")
            raise

    def _crear_rivalidades(self, equipo_uids):
        """
        Crea relaciones de rivalidad entre equipos que se han enfrentado
        """
        try:
            partidos_file = os.path.join("data", "partidos.csv")
            rivalidades = {}  # {(equipo1, equipo2): count}

            with open(partidos_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    local = row['equipo_local_csv_id']
                    visitante = row['equipo_visitante_csv_id']
                    # Ordenar para evitar duplicados
                    par = tuple(sorted([local, visitante]))
                    rivalidades[par] = rivalidades.get(par, 0) + 1

            # Crear aristas de rivalidad para equipos que se han enfrentado al menos 2 veces
            txn = self._dgraph_client.client.txn()
            try:
                for (equipo1, equipo2), count in rivalidades.items():
                    if count >= 2 and equipo1 in equipo_uids and equipo2 in equipo_uids:
                        # Crear relación bidireccional
                        mutation = {
                            "uid": equipo_uids[equipo1],
                            "rivalidad": [{"uid": equipo_uids[equipo2]}]
                        }
                        txn.mutate(set_obj=mutation, commit_now=False)
                txn.commit()
                log.info(f"Rivalidades creadas: {len([v for v in rivalidades.values() if v >= 2])}")
            finally:
                txn.discard()

        except Exception as e:
            log.error(f"Error al crear rivalidades: {e}")
            raise
