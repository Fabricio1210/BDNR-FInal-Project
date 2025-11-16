"""
Connections to the databases
"""
#from Cassandra.cassandra import CassandraService
from Mongo.Mongo import MongoSingleton

def main():
    """
    No docstring >:(
    """
    #CassandraService()
    mongo_instance = MongoSingleton()

if __name__ == "__main__":
    main()