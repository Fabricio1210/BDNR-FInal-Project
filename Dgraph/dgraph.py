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

log = logging.getLogger("ProyectoBases")
log.propagate = False

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
        log.info("Conexión exitosa a Dgraph")

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

    def consultar_rivalidades_equipo(self, nombre_equipo):
        """Equipos rivales de un equipo específico con conteo de enfrentamientos"""
        txn = self._dgraph_client.client.txn(read_only=True)
        try:
            res = txn.query(schema.QUERY_RIVALIDADES_EQUIPO, variables={'$nombre_equipo': nombre_equipo})
            result = json.loads(res.json)

            # Procesar conteo de enfrentamientos que ya vienen en la query
            if result.get('equipo') and len(result['equipo']) > 0:
                equipo = result['equipo'][0]
                if equipo.get('rivalidad'):
                    for rival in equipo['rivalidad']:
                        # Sumar los enfrentamientos como local y como visitante
                        total_local = len(rival.get('enfrentamientos_como_local', []))
                        total_visitante = len(rival.get('enfrentamientos_como_visitante', []))
                        rival['total_enfrentamientos'] = total_local + total_visitante
                        # Limpiar los campos auxiliares
                        rival.pop('enfrentamientos_como_local', None)
                        rival.pop('enfrentamientos_como_visitante', None)

            return result
        finally:
            txn.discard()

    def consultar_jugadores_equipos_rivales(self, nombre, apellido):
        """Jugadores que han jugado en equipos rivales"""
        txn = self._dgraph_client.client.txn(read_only=True)
        try:
            res = txn.query(schema.QUERY_JUGADORES_EQUIPOS_RIVALES, variables={'$nombre': nombre, '$apellido': apellido})
            return json.loads(res.json)
        finally:
            txn.discard()

    def consultar_antiguedad_jugador(self, nombre, apellido):
        """Antigüedad de jugador medida en temporadas"""
        txn = self._dgraph_client.client.txn(read_only=True)
        try:
            res = txn.query(schema.QUERY_ANTIGUEDAD_JUGADOR, variables={'$nombre': nombre, '$apellido': apellido})
            return json.loads(res.json)
        finally:
            txn.discard()

    def consultar_impacto_localia(self, nombre_equipo):
        """Impacto de localía en rendimiento del equipo"""
        txn = self._dgraph_client.client.txn(read_only=True)
        try:
            res = txn.query(schema.QUERY_IMPACTO_LOCALIA, variables={'$nombre_equipo': nombre_equipo})
            return json.loads(res.json)
        finally:
            txn.discard()

    def consultar_temporadas_equipo(self, nombre_equipo):
        """Todas las temporadas de un equipo con enfrentamientos"""
        txn = self._dgraph_client.client.txn(read_only=True)
        try:
            res = txn.query(schema.QUERY_TEMPORADAS_EQUIPO, variables={'$nombre_equipo': nombre_equipo})
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
                    finally:
                        txn.discard()

            # 2. Poblar Equipos
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
                finally:
                    txn.discard()

            # 2.5. Vincular Campos ↔ Equipos (relación inversa)
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
            finally:
                txn.discard()

            # 3. Poblar Jugadores
            jugadores_file = os.path.join("data", "jugadores.csv")
            with open(jugadores_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                txn = self._dgraph_client.client.txn()
                try:
                    for row in reader:
                        jugador = {
                            "dgraph.type": "Jugador",
                            "nombre": row['nombre'],
                            "apellido": row['apellido'],
                            "numero": int(row['numero']) if row['numero'] else 0,
                            "fechaNacimiento": row['fecha_nacimiento'],
                            "pais": row['pais_origen']
                        }

                        response = txn.mutate(set_obj=jugador, commit_now=False)
                        uid = list(response.uids.values())[0]
                        key = f"{row['nombre']}_{row['apellido']}"
                        jugador_uids[key] = uid
                    txn.commit()
                finally:
                    txn.discard()

            # 3.5. Vincular Jugadores con Equipos (historial)
            historial_file = os.path.join("data", "jugadores_equipos_historial.csv")
            if os.path.exists(historial_file):
                with open(historial_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    txn = self._dgraph_client.client.txn()
                    try:
                        for row in reader:
                            jugador_key = f"{row['jugador_nombre']}_{row['jugador_apellido']}"
                            if jugador_key in jugador_uids and row['equipo_id'] in equipo_uids:
                                txn.mutate(set_obj={
                                    "uid": jugador_uids[jugador_key],
                                    "juega_para": [{
                                        "uid": equipo_uids[row['equipo_id']],
                                        "juega_para|fechaInicio": row['fecha_inicio'],
                                        "juega_para|fechaFin": row['fecha_fin']
                                    }]
                                }, commit_now=False)
                        txn.commit()
                    finally:
                        txn.discard()

            # 4. Poblar Temporadas
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
                            "fechaInicio": row['fechaInicio'],
                            "fechaFin": row['fechaFin']
                        }

                        response = txn.mutate(set_obj=temporada, commit_now=False)
                        uid = list(response.uids.values())[0]
                        # Crear clave única para la temporada usando solo el nombre
                        # porque partidos.csv usa torneo_nombre y temporada (que es el nombre)
                        temp_key = row['nombre']
                        temporada_uids[temp_key] = uid
                    txn.commit()
                finally:
                    txn.discard()

            # 5. Pre-cargar mapeo de campos por equipo (optimización)
            campos_por_equipo = {}
            if os.path.exists(campos_file):
                with open(campos_file, 'r', encoding='utf-8') as cf:
                    campos_reader = csv.DictReader(cf)
                    for campo_row in campos_reader:
                        campos_por_equipo[campo_row['equipo_local_csv_id']] = campo_row['nombre']

            # 6. Poblar Enfrentamientos
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
                            "fecha": row['fecha'],
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

                        # Conectar temporada usando solo el nombre de la temporada
                        temp_key = row['temporada']

                        if temp_key in temporada_uids:
                            enfrentamiento["temporada"] = {"uid": temporada_uids[temp_key]}
                        else:
                            #log.warning(f"Temporada no encontrada: {temp_key}. Partido omitido.")
                            continue

                        txn.mutate(set_obj=enfrentamiento, commit_now=False)
                    txn.commit()
                finally:
                    txn.discard()

            # 7. Crear rivalidades entre equipos que se han enfrentado
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
                        # Crear relación bidireccional (de equipo1 a equipo2)
                        mutation1 = {
                            "uid": equipo_uids[equipo1],
                            "rivalidad": [{"uid": equipo_uids[equipo2]}]
                        }
                        txn.mutate(set_obj=mutation1, commit_now=False)

                        # Crear relación bidireccional (de equipo2 a equipo1)
                        mutation2 = {
                            "uid": equipo_uids[equipo2],
                            "rivalidad": [{"uid": equipo_uids[equipo1]}]
                        }
                        txn.mutate(set_obj=mutation2, commit_now=False)
                txn.commit()
            finally:
                txn.discard()

        except Exception as e:
            log.error(f"Error al crear rivalidades: {e}")
            raise
