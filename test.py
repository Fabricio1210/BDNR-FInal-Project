from Cassandra.cassandra import CassandraService
from Mongo.Mongo import MongoService
import Cassandra.schema as schema
import json
from tabulate import tabulate
service = CassandraService()
service.cassandra_session.session.execute(schema.CREATE_KEYSPACE)
service.cassandra_session.session.execute("USE analisis_deportivo;")
service.cassandra_session.session.execute(schema.CREATE_POINTS_BY_TEAM_MATCH_TABLE)
print(json.dumps(service._insert_points_by_team_match(), indent=4))