import sqlite3
import mysql.connector
import psycopg2
from typing import Dict, List, Any, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseConnector:
    """
    Manages connections to different database types and extracts schema information
    """
    def __init__(self):
        self.connections = {}
    
    def add_sqlite_connection(self, db_name: str, db_path: str) -> bool:
        """
        Add a SQLite database connection
        
        Args:
            db_name: A name to identify this database
            db_path: Path to the SQLite database file
            
        Returns:
            Success status
        """
        try:
            conn = sqlite3.connect(db_path)
            self.connections[db_name] = {
                'type': 'sqlite',
                'connection': conn,
                'path': db_path
            }
            logger.info(f"Added SQLite connection: {db_name}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to SQLite database {db_name}: {e}")
            return False
    
    def add_mysql_connection(self, db_name: str, host: str, user: str, password: str, 
                            database: str, port: int = 3306) -> bool:
        """
        Add a MySQL database connection
        
        Args:
            db_name: A name to identify this database
            host: MySQL server host
            user: Username
            password: Password
            database: Database name
            port: MySQL server port
            
        Returns:
            Success status
        """
        try:
            conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                port=port
            )
            self.connections[db_name] = {
                'type': 'mysql',
                'connection': conn,
                'host': host,
                'user': user,
                'password': password,
                'database': database,
                'port': port
            }
            logger.info(f"Added MySQL connection: {db_name}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to MySQL database {db_name}: {e}")
            return False
    
    def add_postgres_connection(self, db_name: str, host: str, user: str, password: str, 
                              database: str, port: int = 5432) -> bool:
        """
        Add a PostgreSQL database connection
        
        Args:
            db_name: A name to identify this database
            host: PostgreSQL server host
            user: Username
            password: Password
            database: Database name
            port: PostgreSQL server port
            
        Returns:
            Success status
        """
        try:
            conn = psycopg2.connect(
                host=host,
                user=user,
                password=password,
                dbname=database,
                port=port
            )
            self.connections[db_name] = {
                'type': 'postgres',
                'connection': conn,
                'host': host,
                'user': user,
                'password': password,
                'database': database,
                'port': port
            }
            logger.info(f"Added PostgreSQL connection: {db_name}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL database {db_name}: {e}")
            return False
    
    def get_connection(self, db_name: str) -> Optional[Any]:
        """Get a database connection by name"""
        if db_name in self.connections:
            return self.connections[db_name]['connection']
        return None
    
    def get_schema_information(self, db_name: str) -> Optional[Dict[str, List[str]]]:
        """
        Extract schema information (tables and columns) from a database
        
        Args:
            db_name: Name of the database connection
            
        Returns:
            Dictionary mapping table names to column lists
        """
        if db_name not in self.connections:
            logger.error(f"Database connection {db_name} not found")
            return None
        
        conn_info = self.connections[db_name]
        db_type = conn_info['type']
        conn = conn_info['connection']
        
        try:
            if db_type == 'sqlite':
                return self._get_sqlite_schema(conn)
            elif db_type == 'mysql':
                return self._get_mysql_schema(conn, conn_info['database'])
            elif db_type == 'postgres':
                return self._get_postgres_schema(conn, conn_info['database'])
            else:
                logger.error(f"Unsupported database type: {db_type}")
                return None
        except Exception as e:
            logger.error(f"Error extracting schema from {db_name}: {e}")
            return None
    
    def _get_sqlite_schema(self, conn) -> Dict[str, List[str]]:
        """Extract schema from SQLite database"""
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        schema = {}
        for table in tables:
            table_name = table[0]
            # Skip SQLite system tables
            if table_name.startswith('sqlite_'):
                continue
                
            # Get columns for each table
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = [row[1] for row in cursor.fetchall()]  # Column name is at index 1
            schema[table_name] = columns
        
        return schema
    
    def _get_mysql_schema(self, conn, database: str) -> Dict[str, List[str]]:
        """Extract schema from MySQL database"""
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute(f"SHOW TABLES FROM {database};")
        tables = cursor.fetchall()
        
        schema = {}
        for table in tables:
            table_name = table[0]
            # Get columns for each table
            cursor.execute(f"SHOW COLUMNS FROM {table_name} FROM {database};")
            columns = [row[0] for row in cursor.fetchall()]  # Column name is at index 0
            schema[table_name] = columns
        
        return schema
    
    def _get_postgres_schema(self, conn, database: str) -> Dict[str, List[str]]:
        """Extract schema from PostgreSQL database"""
        cursor = conn.cursor()
        
        # Get all tables in the public schema
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public';
        """)
        tables = cursor.fetchall()
        
        schema = {}
        for table in tables:
            table_name = table[0]
            # Get columns for each table
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = '{table_name}';
            """)
            columns = [row[0] for row in cursor.fetchall()]
            schema[table_name] = columns
        
        return schema
    
    def execute_query(self, db_name: str, query: str) -> Tuple[bool, Any]:
        """
        Execute a query on the specified database
        
        Args:
            db_name: Name of the database connection
            query: SQL query to execute
            
        Returns:
            Tuple of (success, results)
        """
        if db_name not in self.connections:
            logger.error(f"Database connection {db_name} not found")
            return False, "Database connection not found"
        
        conn = self.connections[db_name]['connection']
        
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            
            # Check if this is a SELECT query (has results to fetch)
            if query.strip().lower().startswith("select"):
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                return True, {"columns": columns, "data": results}
            else:
                conn.commit()
                return True, f"Query executed successfully. Rows affected: {cursor.rowcount}"
                
        except Exception as e:
            logger.error(f"Error executing query on {db_name}: {e}")
            return False, str(e)
    
    def close_all_connections(self):
        """Close all database connections"""
        for db_name, conn_info in self.connections.items():
            try:
                conn_info['connection'].close()
                logger.info(f"Closed connection to {db_name}")
            except Exception as e:
                logger.error(f"Error closing connection to {db_name}: {e}")
        
        self.connections = {} 