import json
import os
from typing import Dict, List, Any, Optional, Set, Tuple
import logging
from vector_search import VectorSearch

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MetadataManager:
    """
    Manages metadata about databases, tables and columns.
    This metadata helps route user queries to the appropriate database.
    """
    def __init__(self, metadata_file: str = "db_metadata.json"):
        self.metadata_file = metadata_file
        self.metadata: Dict[str, Dict[str, List[str]]] = {}
        self.table_to_db_map: Dict[str, str] = {}  # Maps table names to database names
        
        # Initialize vector search without loading the model
        self.vector_search = VectorSearch()
        self.vector_search_available = self.vector_search.is_available()
        
        # Load metadata
        self._load_metadata()
        self._build_table_map()
    
    def _load_metadata(self) -> None:
        """Load metadata from file if it exists"""
        try:
            if os.path.exists(self.metadata_file):
                with open(self.metadata_file, 'r') as f:
                    self.metadata = json.load(f)
                logger.info(f"Loaded metadata from {self.metadata_file}")
                
                # No need to update vector search here - will be loaded on demand
            else:
                logger.info(f"No metadata file found at {self.metadata_file}")
        except Exception as e:
            logger.error(f"Error loading metadata: {e}")
            self.metadata = {}
    
    def _save_metadata(self) -> None:
        """Save metadata to file"""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(self.metadata, f, indent=2)
            logger.info(f"Saved metadata to {self.metadata_file}")
        except Exception as e:
            logger.error(f"Error saving metadata: {e}")
    
    def _build_table_map(self) -> None:
        """Build a map of table names to database names for quick lookups"""
        self.table_to_db_map = {}
        for db_name, tables in self.metadata.items():
            for table_name in tables.keys():
                # If multiple databases have the same table, the last one wins
                # This is intentional as we'll resolve conflicts during query time
                self.table_to_db_map[table_name] = db_name
    
    def update_database_metadata(self, db_name: str, tables_info: Dict[str, List[str]]) -> None:
        """
        Update metadata for a specific database
        
        Args:
            db_name: Name of the database
            tables_info: Dictionary mapping table names to their column names
        """
        self.metadata[db_name] = tables_info
        self._save_metadata()
        self._build_table_map()
        
        # Update vector search with the new metadata (model will load on demand)
        if self.vector_search_available:
            self.vector_search.update_from_metadata(self.metadata)
    
    def get_database_for_query(self, query: str) -> Optional[str]:
        """
        Determine which database to query based on the user input
        
        Args:
            query: The user's natural language query
            
        Returns:
            The name of the most relevant database or None if no match
        """
        # First try semantic search if available
        if self.vector_search_available:
            db_name, _ = self.vector_search.get_best_db_and_table(query)
            if db_name:
                logger.info(f"Found database {db_name} using semantic search")
                return db_name
        
        # Fall back to keyword matching if semantic search fails or is unavailable
        query = query.lower()
        matching_dbs = {}
        
        for db_name, tables in self.metadata.items():
            score = 0
            for table_name, columns in tables.items():
                if table_name.lower() in query:
                    score += 5  # Higher weight for table matches
                
                for column in columns:
                    if column.lower() in query:
                        score += 2  # Lower weight for column matches
            
            if score > 0:
                matching_dbs[db_name] = score
        
        if matching_dbs:
            # Return the database with the highest match score
            return max(matching_dbs.items(), key=lambda x: x[1])[0]
        
        return None
    
    def get_table_for_query(self, query: str, db_name: Optional[str] = None) -> Optional[str]:
        """
        Determine which table to query based on the user input
        
        Args:
            query: The user's natural language query
            db_name: Optional database name to limit the search
            
        Returns:
            The name of the most relevant table or None if no match
        """
        # First try semantic search if available
        if self.vector_search_available:
            search_db, table_name = self.vector_search.get_best_db_and_table(query)
            # If a database was specified, make sure the table is from that database
            if table_name and (db_name is None or search_db == db_name):
                logger.info(f"Found table {table_name} using semantic search")
                return table_name
        
        # Fall back to keyword matching
        query = query.lower()
        matching_tables = {}
        
        # Filter databases if one is specified
        dbs_to_check = [db_name] if db_name and db_name in self.metadata else self.metadata.keys()
        
        for current_db in dbs_to_check:
            tables = self.metadata[current_db]
            for table_name, columns in tables.items():
                score = 0
                
                # Check table name
                if table_name.lower() in query:
                    score += 5
                
                # Check column names
                for column in columns:
                    if column.lower() in query:
                        score += 2
                
                if score > 0:
                    matching_tables[table_name] = score
        
        if matching_tables:
            # Return the table with the highest match score
            return max(matching_tables.items(), key=lambda x: x[1])[0]
        
        return None
    
    def get_database_and_table(self, query: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Find the most relevant database and table for a query
        
        Args:
            query: User's natural language query
            
        Returns:
            Tuple of (database_name, table_name), either can be None
        """
        # First try semantic search if available
        if self.vector_search_available:
            db_name, table_name = self.vector_search.get_best_db_and_table(query)
            if db_name and table_name:
                logger.info(f"Found database {db_name} and table {table_name} using semantic search")
                return db_name, table_name
        
        # Fall back to traditional approach
        db_name = self.get_database_for_query(query)
        if not db_name:
            return None, None
        
        table_name = self.get_table_for_query(query, db_name)
        return db_name, table_name
    
    def get_database_for_table(self, table_name: str) -> Optional[str]:
        """
        Find which database contains a specific table
        
        Args:
            table_name: Name of the table to look for
            
        Returns:
            Name of the database containing the table, or None if not found
        """
        return self.table_to_db_map.get(table_name)
    
    def get_all_tables(self) -> Set[str]:
        """Get a set of all table names across all databases"""
        tables = set()
        for db_name, db_tables in self.metadata.items():
            tables.update(db_tables.keys())
        return tables
    
    def get_all_metadata(self) -> Dict[str, Dict[str, List[str]]]:
        """Get all metadata"""
        return self.metadata
    
    def get_database_metadata(self, db_name: str) -> Optional[Dict[str, List[str]]]:
        """Get metadata for a specific database"""
        return self.metadata.get(db_name) 