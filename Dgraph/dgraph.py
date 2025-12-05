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
        """Consulta recursiva de jugador con equipos y facets"""
        txn = self._dgraph_client.client.txn(read_only=True)
        try:
            res = txn.query(schema.QUERY_JUGADOR_COMPLETO, variables={'$nombre': nombre, '$apellido': apellido})
            return json.loads(res.json)
        finally:
            txn.discard()

    def consultar_enfrentamientos_equipo_temporada(self, nombre_equipo, nombre_temporada):
        """Enfrentamientos de un equipo en una temporada"""
        txn = self._dgraph_client.client.txn(read_only=True)
        try:
            res = txn.query(schema.QUERY_ENFRENTAMIENTOS_EQUIPO_TEMPORADA,
                          variables={'$nombre_equipo': nombre_equipo, '$nombre_temporada': nombre_temporada})
            return json.loads(res.json)
        finally:
            txn.discard()

    def consultar_equipos_locales_estadio(self, nombre_campo):
        """Equipos que juegan como locales en un estadio"""
        txn = self._dgraph_client.client.txn(read_only=True)
        try:
            res = txn.query(schema.QUERY_EQUIPOS_LOCALES_ESTADIO, variables={'$nombre_campo': nombre_campo})
            return json.loads(res.json)
        finally:
            txn.discard()

    def consultar_campos_equipo(self, nombre_equipo):
        """Campo local y todos los campos donde ha jugado un equipo"""
        txn = self._dgraph_client.client.txn(read_only=True)
        try:
            res = txn.query(schema.QUERY_CAMPOS_EQUIPO, variables={'$nombre_equipo': nombre_equipo})
            return json.loads(res.json)
        finally:
            txn.discard()

    def consultar_enfrentamientos_estadio(self, nombre_campo):
        """Todos los enfrentamientos en un estadio"""
        txn = self._dgraph_client.client.txn(read_only=True)
        try:
            res = txn.query(schema.QUERY_ENFRENTAMIENTOS_ESTADIO, variables={'$nombre_campo': nombre_campo})
            return json.loads(res.json)
        finally:
            txn.discard()

    def consultar_companeros_jugador(self, nombre, apellido):
        """Compañeros de equipo de un jugador"""
        txn = self._dgraph_client.client.txn(read_only=True)
        try:
            res = txn.query(schema.QUERY_COMPANEROS_JUGADOR, variables={'$nombre': nombre, '$apellido': apellido})
            return json.loads(res.json)
        finally:
            txn.discard()

    # ==================== ADMIN ====================

    def drop_all(self):
        """Elimina todos los datos de Dgraph"""
        try:
            self._dgraph_client.client.alter(pydgraph.Operation(drop_all=True))
            self._dgraph_client.client.alter(pydgraph.Operation(schema=schema.SCHEMA))
            log.info("Dgraph: Datos eliminados y schema reaplicado")
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

            # 2.5. Vincular Campos ↔ Equipos (relación inversa)
            log.info("Vinculando Campos y Equipos...")
            txn = self._dgraph_client.client.txn()
            try:
                for equipo_csv_id, campo_nombre in campos_data.items():
                    if campo_nombre in campo_uids and equipo_csv_id in equipo_uids:
                        # Agregar equipo a la lista equipos_locales del campo
                        txn.mutate(set_obj={
                            "uid": campo_uids[campo_nombre],
                            "equipos_locales": [{"uid": equipo_uids[equipo_csv_id]}]
                        }, commit_now=False)
                txn.commit()
                log.info("Campos vinculados con equipos")
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

            # 4. Poblar Temporadas
            log.info("Poblando nodo Temporada...")
            temporadas_file = os.path.join("data", "temporadas.csv")
            with open(temporadas_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                txn = self._dgraph_client.client.txn()
                try:
                    for row in reader:
                        temporada = {
                            "dgraph.type": "Temporada",
                            "anio": int(row['anio']),
                            "nombre": row['nombre'],
                            "liga": row['liga'],
                            "fechaInicio": row['fechaInicio'] + "T00:00:00Z",
                            "fechaFin": row['fechaFin'] + "T23:59:59Z"
                        }

                        response = txn.mutate(set_obj=temporada, commit_now=False)
                        uid = list(response.uids.values())[0]
                        # Crear clave única para la temporada
                        temp_key = f"{row['liga']}_{row['nombre']}"
                        temporada_uids[temp_key] = uid
                    txn.commit()
                    log.info(f"Temporadas insertadas: {len(temporada_uids)}")
                finally:
                    txn.discard()

            # 5. Pre-cargar mapeo de campos por equipo (optimización)
            log.info("Pre-cargando mapeo campos-equipos...")
            campos_por_equipo = {}
            if os.path.exists(campos_file):
                with open(campos_file, 'r', encoding='utf-8') as cf:
                    campos_reader = csv.DictReader(cf)
                    for campo_row in campos_reader:
                        campos_por_equipo[campo_row['equipo_local_csv_id']] = campo_row['nombre']

            # 6. Poblar Enfrentamientos
            log.info("Poblando nodo Enfrentamiento...")
            partidos_file = os.path.join("data", "partidos.csv")
            with open(partidos_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                txn = self._dgraph_client.client.txn()
                try:
                    for row in reader:
                        # Determinar resultado
                        goles_local = int(row['goles_local'])
                        goles_visitante = int(row['goles_visitante'])

                        if goles_local > goles_visitante:
                            resultado = "Victoria Local"
                        elif goles_local < goles_visitante:
                            resultado = "Victoria Visitante"
                        else:
                            resultado = "Empate"

                        enfrentamiento = {
                            "dgraph.type": "Enfrentamiento",
                            "fecha": row['fecha'] + "T00:00:00Z",
                            "marcadorLocal": goles_local,
                            "marcadorVisitante": goles_visitante,
                            "resultado": resultado
                        }

                        # Conectar equipos
                        if row['equipo_local_csv_id'] in equipo_uids:
                            enfrentamiento["equipo_local"] = {"uid": equipo_uids[row['equipo_local_csv_id']]}
                        if row['equipo_visitante_csv_id'] in equipo_uids:
                            enfrentamiento["equipo_visitante"] = {"uid": equipo_uids[row['equipo_visitante_csv_id']]}

                        # Conectar campo (usando mapeo pre-cargado)
                        campo_nombre = campos_por_equipo.get(row['equipo_local_csv_id'])
                        if campo_nombre and campo_nombre in campo_uids:
                            enfrentamiento["campo"] = {"uid": campo_uids[campo_nombre]}

                        # Conectar temporada
                        temp_key = f"{row['torneo_nombre']}_{row['temporada']}"

                        if temp_key in temporada_uids:
                            enfrentamiento["temporada"] = {"uid": temporada_uids[temp_key]}
                        else:
                            #log.warning(f"Temporada no encontrada: {temp_key}. Partido omitido.")
                            continue

                        txn.mutate(set_obj=enfrentamiento, commit_now=False)
                    txn.commit()
                    log.info("Enfrentamientos insertados exitosamente")
                finally:
                    txn.discard()

            # 7. Crear rivalidades entre equipos que se han enfrentado
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
