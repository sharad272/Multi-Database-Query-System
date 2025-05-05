import sqlite3
import mysql.connector
import psycopg2
import re
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
        
        conn_info = self.connections[db_name]
        conn = conn_info['connection']
        db_type = conn_info['type']
        
        try:
            # Log the original query we're trying to execute
            logger.info(f"Original query to execute on {db_name}: {query}")
            
            # Clean up the query - normalize whitespace but preserve the query
            query = query.strip()
            
            # Apply database-specific adaptations without changing the query's intent
            adapted_query = query
            
            if db_type == 'sqlite':
                # SQLite specific adaptations
                
                # Handle date functions that are SQL Server/MySQL specific but not in SQLite
                if 'GETDATE()' in adapted_query.upper():
                    adapted_query = adapted_query.replace('GETDATE()', "date('now')")
                    adapted_query = adapted_query.replace('getdate()', "date('now')")
                    logger.info(f"Adapted GETDATE() to SQLite syntax: {adapted_query}")
                
                # Handle TOP syntax which is SQL Server specific
                if 'TOP ' in adapted_query.upper() and ' LIMIT ' not in adapted_query.upper():
                    # Extract the TOP value and convert to LIMIT
                    top_match = re.search(r'TOP\s+(\d+)', adapted_query, re.IGNORECASE)
                    if top_match:
                        top_value = top_match.group(1)
                        # Remove TOP clause and add LIMIT at the end
                        adapted_query = re.sub(r'TOP\s+\d+', '', adapted_query, flags=re.IGNORECASE)
                        adapted_query = f"{adapted_query} LIMIT {top_value}"
                        logger.info(f"Converted TOP to LIMIT for SQLite: {adapted_query}")
                
                # Fix comparison operators < and > which can cause syntax errors in SQLite
                # This issue happens because SQLite can sometimes parse these characters as XML tags
                # Add spaces around these operators to prevent this issue
                adapted_query = self._fix_comparison_operators(adapted_query)
            
            # Log the adapted query
            if adapted_query != query:
                logger.info(f"Adapted query for {db_type}: {adapted_query}")
            
            # Execute the adapted query
            cursor = conn.cursor()
            cursor.execute(adapted_query)
            
            # Check if this is a SELECT query (has results to fetch)
            if adapted_query.strip().lower().startswith("select"):
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                return True, {"columns": columns, "data": results}
            else:
                conn.commit()
                return True, f"Query executed successfully. Rows affected: {cursor.rowcount}"
                
        except sqlite3.OperationalError as e:
            error_msg = str(e)
            logger.error(f"SQLite error executing query on {db_name}: {error_msg}")
            
            # Special handling for < and > operators in WHERE clauses
            if "syntax error" in error_msg and ("<" in error_msg or ">" in error_msg):
                try:
                    # Try to fix comparison operators and retry
                    fixed_query = self._fix_comparison_operators(query, aggressive=True)
                    logger.info(f"Retrying with fixed comparison operators: {fixed_query}")
                    
                    if fixed_query != query:
                        # Try executing with the fixed query
                        cursor = conn.cursor()
                        cursor.execute(fixed_query)
                        
                        # Check if this is a SELECT query (has results to fetch)
                        if fixed_query.strip().lower().startswith("select"):
                            columns = [desc[0] for desc in cursor.description]
                            results = cursor.fetchall()
                            return True, {"columns": columns, "data": results}
                        else:
                            conn.commit()
                            return True, f"Query executed successfully. Rows affected: {cursor.rowcount}"
                except Exception as inner_e:
                    logger.error(f"Error executing query with fixed operators: {inner_e}")
            
            # If it's a syntax error, but we're not going to auto-fallback anymore
            # We'll let the error propagate to the UI so the user can see exactly what went wrong
            return False, f"SQLite error: {error_msg}"
        
        except Exception as e:
            logger.error(f"Error executing query on {db_name}: {e}")
            return False, str(e)
    
    def _fix_comparison_operators(self, query: str, aggressive: bool = False) -> str:
        """
        Fix potential issues with comparison operators in SQLite
        
        Args:
            query: The SQL query to fix
            aggressive: Whether to use more aggressive fixing (for retry attempts)
            
        Returns:
            Fixed SQL query
        """
        fixed_query = query
        
        # Ensure spaces around comparison operators
        # For < and > which can be misinterpreted as XML tags
        comparison_operators = ['<', '>', '<=', '>=', '<>', '!=', '=']
        
        if aggressive:
            # More aggressive replacement for retry attempts
            for op in comparison_operators:
                # Replace with spaces on both sides
                fixed_query = fixed_query.replace(op, f" {op} ")
                
            # Normalize multiple spaces
            fixed_query = re.sub(r'\s+', ' ', fixed_query)
            
            # Attempt to fix WHERE clauses by adding quotes around string values
            # This is a heuristic and not perfect
            where_pos = fixed_query.upper().find("WHERE")
            if where_pos > 0:
                where_clause = fixed_query[where_pos:]
                # Find potential column-value comparisons
                comparisons = re.findall(r'(\w+)\s*([<>=!]+)\s*(\w+)', where_clause)
                for col, op, val in comparisons:
                    # If value is not a number, add quotes
                    if not val.isdigit() and val.lower() not in ['null', 'true', 'false']:
                        # Don't add quotes if it's already quoted
                        if not (val.startswith("'") and val.endswith("'")) and \
                           not (val.startswith('"') and val.endswith('"')):
                            replacement = f"{col} {op} '{val}'"
                            pattern = f"{col}\\s*{op}\\s*{val}"
                            fixed_query = re.sub(pattern, replacement, fixed_query)
        else:
            # Basic replacement - ensure there's at least one space around operators
            for op in comparison_operators:
                # Replace without surrounding spaces
                pattern = r'(\S)' + re.escape(op) + r'(\S)'
                replacement = r'\1 ' + op + r' \2'
                fixed_query = re.sub(pattern, replacement, fixed_query)
        
        return fixed_query
    
    def close_all_connections(self):
        """Close all database connections"""
        for db_name, conn_info in self.connections.items():
            try:
                conn_info['connection'].close()
                logger.info(f"Closed connection to {db_name}")
            except Exception as e:
                logger.error(f"Error closing connection to {db_name}: {e}")
        
        self.connections = {} 