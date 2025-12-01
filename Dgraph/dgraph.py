"""
    Modelos y controladores de dgraph
"""
import logging
import os
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
