from airflow.hooks.base import BaseHook
from neo4j import GraphDatabase
import logging

class Neo4jHook(BaseHook):
    """
    Hook for interacting with Neo4j knowledge graph database.
    
    :param neo4j_conn_id: Connection ID for Neo4j
    :type neo4j_conn_id: str
    """
    
    def __init__(self, neo4j_conn_id='neo4j_default'):
        super().__init__()
        self.neo4j_conn_id = neo4j_conn_id
        self.driver = None
        self.logger = logging.getLogger(__name__)
    
    def get_conn(self):
        """Returns a Neo4j driver."""
        if self.driver is not None:
            return self.driver
            
        conn = self.get_connection(self.neo4j_conn_id)
        uri = conn.host  # URI comes from the host field
        username = conn.login
        password = conn.password
        
        if not all([uri, username, password]):
            raise ValueError("Neo4j URI, username, and password are required")
        
        try:
            self.driver = GraphDatabase.driver(uri, auth=(username, password))
            # Verify connection
            self.driver.verify_connectivity()
            self.logger.info("Successfully connected to Neo4j database")
            return self.driver
        except Exception as e:
            self.logger.error(f"Error connecting to Neo4j: {e}")
            raise
        
    def test_connection(self):
        """Test the Neo4j connection by running a simple query."""
        driver = self.get_conn()
        try:
            with driver.session() as session:
                result = session.run("MATCH (n) RETURN count(n) AS count LIMIT 1")
                count = result.single()["count"]
                return True, f"Connection successful. Graph has {count} nodes."
        except Exception as e:
            return False, str(e)
    
    def run_query(self, query, parameters=None):
        """Run a Cypher query on Neo4j."""
        driver = self.get_conn()
        with driver.session() as session:
            return session.run(query, parameters or {}).data()
    
    def setup_constraints(self):
        """Create constraints for the database."""
        try:
            driver = self.get_conn()
            with driver.session() as session:
                # Create constraints
                session.run("""
                CREATE CONSTRAINT entity_constraint IF NOT EXISTS
                FOR (e:Entity) REQUIRE e.canonical_name IS UNIQUE
                """)
                
                session.run("""
                CREATE CONSTRAINT document_constraint IF NOT EXISTS
                FOR (d:Document) REQUIRE d.url IS UNIQUE
                """)
                
                self.logger.info("Database constraints created successfully")
                return True
        except Exception as e:
            self.logger.error(f"Error creating constraints: {e}")
            return False
    
    def close(self):
        """Close the Neo4j connection."""
        if self.driver:
            self.driver.close()
            self.driver = None
            self.logger.info("Neo4j connection closed")