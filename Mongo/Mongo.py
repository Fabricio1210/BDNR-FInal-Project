import logging
from pymongo import MongoClient
from Mongo.populate import algo

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
        pass

    def poblar_equipos_db(self):
        algo()
