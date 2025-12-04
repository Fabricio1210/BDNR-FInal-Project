from Cassandra.cassandra import CassandraService
from Mongo.Mongo import MongoService
from connect import DatabaseFacade
import Cassandra.schema as schema
import json
facade = DatabaseFacade()
print(facade.agregar_jugador("Javier","Hernandez",14,"1996-12-13","Futbol","Mexico","Delantero",175,"Al Nassr FC"))