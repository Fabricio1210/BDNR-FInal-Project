import logging
from pymongo import MongoClient
from Mongo.populate import poblar
from datetime import datetime
from bson import ObjectId

log = logging.getLogger()

class MongoSingleton:
    _instance = None
    DEFAULT_HOST = "localhost"
    DEFAULT_PORT = 27017
    DEFAULT_DB = "sports_db"

    def _init_instance(self):
        
        host = self.DEFAULT_HOST
        port = self.DEFAULT_PORT
        db_name = self.DEFAULT_DB
        uri = f"mongodb://{host}:{port}/"
        
        try:
            self._client = MongoClient(uri)
            self.db = self._client[db_name]
            self._client.admin.command('ping')
            print("Conexión a MongoDB exitosa")
            log.info("Conexión Singleton a MongoDB establecida")

        except Exception as e:
            print(f"Error al conectar con MongoDB: {e}")
            log.error(f"Error al conectar con MongoDB usando URI: {uri}. {e}")
            raise

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoSingleton, cls).__new__(cls)
            cls._instance._init_instance()
        return cls._instance

    def shutdown(self):
        if self._client:
            self._client.close()
            log.info("Conexión Singleton a MongoDB cerrada.")

class MongoService:
    def __init__(self):
        log.info("Conexión Singleton a MongoDB establecida")
        self.db = MongoSingleton()

    def poblar(self):
        poblar()

    def to_dict(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)

        if isinstance(obj, datetime):
            return obj.isoformat() 

        if isinstance(obj, dict):
            return {k: self.to_dict(v) for k, v in obj.items()}

        if isinstance(obj, list):
            return [self.to_dict(i) for i in obj]

        return obj

    def obtener_jugadores(self, nombre, apellido):
        jugadores = self.db.db.jugadores.find({
            "nombre": nombre,
            "apellido": apellido
        })
        jugadores = [self.to_dict(j) for j in jugadores]
        if not jugadores or len(jugadores) == 0:
            raise ValueError("No se encontraron jugadores con ese nombre y apellido.")
        else:
            return jugadores

    def obtener_partidos(self, deporte, fecha):
        fecha_iso = datetime.strptime(fecha, "%Y-%m-%d")
        partidos = self.db.db.partidos.find({
            "deporte": deporte,
            "fecha": fecha_iso
        })
        partidos = [self.to_dict(j) for j in partidos]
        if not partidos or len(partidos) == 0:
            raise ValueError("No se encontraron partidos en esa fecha para ese deporte.")
        else:
            return partidos

    def obtener_equipo(self, nombre_equipo):
        equipo_doc = self.db.db.equipos.find_one({
            "nombre": nombre_equipo
        })
        if equipo_doc is None:
            raise ValueError(f"No se encontró el equipo")
        else:
            equipo_dict = self.to_dict(equipo_doc)
            return equipo_dict

    def obtener_estadisticas_torneo(self, torneo_nombre_arg, temporada_arg):
        cursor_estadisticas = self.db.db.estadisticas_torneos.find({
            "torneo_nombre": torneo_nombre_arg,
            "temporada": temporada_arg
        })
        resultados_dict = [self.to_dict(doc) for doc in cursor_estadisticas]
        if not resultados_dict:
            raise ValueError(f"No se encontró el torneo")
        return resultados_dict

    def obtener_ligas(self, nombre_deporte):
        cursor_ligas = self.db.db.ligas.find({
            "nombre_deporte": nombre_deporte
        })
        ligas_dict = [self.to_dict(doc) for doc in cursor_ligas]
        if not ligas_dict:
            raise ValueError(
                f"No se encontraron ligas para el deporte: '{nombre_deporte}'."
            )
        return ligas_dict

    def estadisticas_liga(self, liga, temporada):
        stats_ligas = self.db.db.estadisticas_torneos.find({
            "torneo_nombre": liga,
            "temporada": temporada
        })
        stats_ligas_dict = [self.to_dict(doc) for doc in stats_ligas]
        if not stats_ligas_dict:
            raise ValueError(
                f"No se encontraron ligas para el deporte."
            )
        return stats_ligas_dict

    def obtener_partido_por_fecha_y_equipos(self, fecha_str, nombre_local, nombre_visitante):
        fecha = datetime.strptime(fecha_str, "%Y-%m-%d")

        pipeline = [
            { "$match": { "fecha": fecha } },
            {
                "$lookup": {
                    "from": "equipos",
                    "localField": "equipo_local_id",
                    "foreignField": "_id",
                    "as": "equipo_local"
                }
            },
            { "$unwind": "$equipo_local" },
            {
                "$lookup": {
                    "from": "equipos",
                    "localField": "equipo_visitante_id",
                    "foreignField": "_id",
                    "as": "equipo_visitante"
                }
            },
            { "$unwind": "$equipo_visitante" },
            {
                "$match": {
                    "equipo_local.nombre": nombre_local,
                    "equipo_visitante.nombre": nombre_visitante
                }
            }
        ]

        resultado = list(self.db.db.partidos.aggregate(pipeline))
        if not resultado:
            raise ValueError("No se encontró un partido con esos datos.")
        partido_dict = self.to_dict(resultado[0])

        if 'fecha' in partido_dict and isinstance(partido_dict['fecha'], datetime):
            partido_dict['fecha'] = partido_dict['fecha'].strftime("%Y-%m-%d")

        return partido_dict

    def obtener_puntajes_por_deporte(self, deporte_arg):

        pipeline = [
            {
                "$match": {
                    "deporte": deporte_arg
                }
            },
            {
                "$lookup": {
                    "from": "equipos",
                    "localField": "equipo_id",
                    "foreignField": "_id",
                    "as": "equipo"
                }
            },
            { "$unwind": "$equipo" },
            {
                "$project": {
                    "_id": 0,
                    "equipo": "$equipo.nombre",
                    "deporte": 1,
                    "liga": "$torneo_nombre",
                    "temporada": 1,
                    "puntos": "$metricas.puntos"
                }
            },
            {
                "$sort": {
                    "puntos": -1
                }
            }
        ]

        resultado = list(self.db.db.estadisticas_torneos.aggregate(pipeline))

        if not resultado:
            raise ValueError("No se encontraron puntajes para ese deporte.")

        puntajes = [self.to_dict(doc) for doc in resultado]
        return puntajes

    def obtener_primer_lugar_por_deporte(self):

        pipeline = [
            {
                "$lookup": {
                    "from": "equipos",
                    "localField": "equipo_id",
                    "foreignField": "_id",
                    "as": "equipo"
                }
            },
            { "$unwind": "$equipo" },
            {
                "$project": {
                    "_id": 0,
                    "equipo": "$equipo.nombre",
                    "deporte": 1,
                    "liga": "$torneo_nombre",
                    "temporada": 1,
                    "puntos": "$metricas.puntos"
                }
            },
            { "$sort": { "deporte": 1, "puntos": -1 } },
            {
                "$group": {
                    "_id": "$deporte",
                    "equipo": { "$first": "$equipo" },
                    "liga": { "$first": "$liga" },
                    "temporada": { "$first": "$temporada" },
                    "puntos": { "$first": "$puntos" }
                }
            },
            { "$sort": { "_id": 1 } }
        ]

        resultado = list(self.db.db.estadisticas_torneos.aggregate(pipeline))

        if not resultado:
            raise ValueError("No hay estadísticas registradas.")

        return [self.to_dict(doc) for doc in resultado]


    def agregar_jugador(self, nombre, apellido, numero, fecha_nacimiento, deporte, pais_origen, posicion, altura_cm, equipo_nombre):
        try:
            equipo = self.db.db.equipos.find_one({"nombre": equipo_nombre})

            if not equipo:
                return {"error": f"El equipo '{equipo_nombre}' no existe"}
            try:
                fecha_dt = datetime.strptime(fecha_nacimiento, "%Y-%m-%d")
            except ValueError:
                return {"error": "El formato de fecha de nacimiento es incorrecto. Use AAAA-MM-DD."}
                
            nuevo_jugador = {
                "nombre": nombre,
                "apellido": apellido,
                "numero": numero,
                "fecha_nacimiento": fecha_dt, 
                "deporte": deporte,
                "pais_origen": pais_origen,
                "equipo_id": equipo["_id"],
                "atributos_adicionales": {
                    "posicion": posicion,
                    "altura_cm": altura_cm
                }
            }

            jugador_id = self.db.db.jugadores.insert_one(nuevo_jugador).inserted_id
            self.db.db.equipos.update_one(
                {"_id": equipo["_id"]},
                {"$push": {"jugadores_ids": jugador_id}}
            )

            return {
                "mensaje": "Jugador agregado correctamente",
                "jugador_id": str(jugador_id),
                "equipo_actualizado": equipo_nombre
            }

        except Exception as e:
            return {"error": str(e)}

    def eliminar_base_de_datos(self):
        db_name = self.db.DEFAULT_DB
        log.info("Base de datos de cassandra borrada exitosamente")
        try:
            self.db._client.drop_database(db_name)            
            return {"mensaje": f"La base de datos '{db_name}' ha sido eliminada correctamente."}

        except Exception as e:
            raise Exception(f"Error al intentar eliminar la base de datos '{db_name}': {e}")

    def agregar_equipo(self, nombre, deporte, pais, region, trofeos_totales=0, puntos_historicos=0):
        try:
            existente = self.db.db.equipos.find_one({"nombre": nombre})
            if existente:
                return {"error": f"El equipo '{nombre}' ya existe"}

            try:
                trofeos_totales = int(trofeos_totales)
                puntos_historicos = int(puntos_historicos)
            except ValueError:
                return {"error": "Los valores de trofeos_totales y puntos_historicos deben ser enteros"}

            nuevo_equipo = {
                "nombre": nombre,
                "deporte": deporte,
                "pais": pais,
                "region": region,
                "estadisticas_acumuladas": {
                    "trofeos_totales": trofeos_totales,
                    "puntos_historicos": puntos_historicos
                },
                "isActive": True,
                "jugadores_ids": []
            }

            equipo_id = self.db.db.equipos.insert_one(nuevo_equipo).inserted_id

            return {
                "mensaje": "Equipo agregado correctamente",
                "equipo_id": str(equipo_id),
                "nombre": nombre
            }

        except Exception as e:
            return {"error": str(e)}
