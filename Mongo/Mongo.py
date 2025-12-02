import logging
from pymongo import MongoClient
from Mongo.populate import poblar
from datetime import datetime

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
        self.db = MongoSingleton()

    def poblar(self):
        poblar()

    def obtener_jugadores(self, nombre, apellido):
        jugadores = self.db.db.jugadores.find({
            "nombre": nombre,
            "apellido": apellido
        })

        return list(jugadores)

    def obtener_partidos(self, deporte, fecha):
        partidos = self.db.db.partidos.find({
            "deporte": deporte,
            "fecha": fecha
        })

        return list(partidos)

    def obtener_equipo(self, nombre_equipo):
        equipo = self.db.db.equipos.find_one({
            "nombre": nombre_equipo
        })

        return list(equipo)

    def obtener_estadisticas_torneo(self, torneo_nombre_arg, temporada_arg):
        estadisticas = self.db.db.estadisticas_torneos.find({
            "torneo_nombre": torneo_nombre_arg,
            "temporada": temporada_arg
        })

        return list(estadisticas)

    def obtener_ligas(self, nombre_deporte):
        ligas = self.db.db.ligas.find({
            "nombre_deporte": nombre_deporte
        })

        return list(ligas)

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
        return resultado[0] if resultado else None
