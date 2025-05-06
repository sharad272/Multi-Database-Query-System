import os
import json
import numpy as np
import logging
from typing import Dict, List, Tuple, Optional, Any
from sentence_transformers import SentenceTransformer
import faiss

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VectorSearch:
    """
    Provides semantic search capabilities for database metadata using embeddings
    """
    
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        """
        Initialize the vector search with settings but don't load the model yet
        
        Args:
            model_name: Name of the sentence-transformers model to use
        """
        self.model_name = model_name
        self.model = None
        self.dimension = 384  # Default dimension for all-MiniLM models
        self.index = None
        
        # Storage for metadata
        self.table_records = []
        self.column_records = []
        
        # Cache the embeddings file path
        self.embeddings_file = "db_embeddings.json"
        
        # Flag to track if model is loaded
        self._model_loaded = False
        
        # Try to load existing embeddings
        self._load_embeddings()
    
    def _load_model(self):
        """Lazy-load the model only when needed"""
        if not self._model_loaded:
            try:
                logger.info(f"Loading sentence transformer model: {self.model_name}")
                self.model = SentenceTransformer(self.model_name)
                self.dimension = self.model.get_sentence_embedding_dimension()
                logger.info(f"Embedding dimension: {self.dimension}")
                
                # Initialize empty index if needed
                if self.index is None:
                    self.index = faiss.IndexFlatL2(self.dimension)
                    logger.info("Initialized FAISS index")
                
                self._model_loaded = True
                return True
            except Exception as e:
                logger.error(f"Error loading model: {e}")
                self.model = None
                return False
        return True
    
    def is_available(self) -> bool:
        """Check if vector search is available"""
        # Just check if embeddings can be loaded, don't load the model yet
        return self.embeddings_file is not None
    
    def _load_embeddings(self) -> bool:
        """Load embeddings from file if exists"""
        if os.path.exists(self.embeddings_file):
            try:
                with open(self.embeddings_file, 'r') as f:
                    data = json.load(f)
                
                # Load table records
                self.table_records = data.get("tables", [])
                
                # Load column records
                self.column_records = data.get("columns", [])
                
                # Rebuild index
                if self.table_records or self.column_records:
                    self._rebuild_index()
                    return True
                    
            except Exception as e:
                logger.error(f"Error loading embeddings: {e}")
        
        return False
    
    def _save_embeddings(self) -> bool:
        """Save embeddings to file"""
        try:
            data = {
                "tables": self.table_records,
                "columns": self.column_records
            }
            
            with open(self.embeddings_file, 'w') as f:
                json.dump(data, f)
            
            logger.info(f"Saved embeddings to {self.embeddings_file}")
            return True
        except Exception as e:
            logger.error(f"Error saving embeddings: {e}")
            return False
    
    def _rebuild_index(self) -> None:
        """Rebuild the FAISS index from stored records"""
        # Combine table and column embeddings
        all_embeddings = []
        
        # Add table embeddings
        for record in self.table_records:
            if "embedding" in record:
                all_embeddings.append(record["embedding"])
        
        # Add column embeddings
        for record in self.column_records:
            if "embedding" in record:
                all_embeddings.append(record["embedding"])
        
        if all_embeddings:
            # Convert to numpy array
            embeddings_array = np.array(all_embeddings).astype('float32')
            
            # Create new index
            self.index = faiss.IndexFlatL2(self.dimension)
            
            # Add embeddings to index
            self.index.add(embeddings_array)
            
            logger.info(f"Rebuilt index with {len(all_embeddings)} embeddings")
    
    def update_from_metadata(self, metadata: Dict[str, Dict[str, List[str]]]) -> bool:
        """
        Update vector search index from database metadata
        
        Args:
            metadata: Dictionary mapping database names to tables to columns
            
        Returns:
            Success status
        """
        # Load the model if not already loaded
        if not self._load_model():
            logger.warning("Vector search is not available. Cannot update index.")
            return False
        
        try:
            # Clear existing records
            self.table_records = []
            self.column_records = []
            
            # Process all databases
            for db_name, tables in metadata.items():
                # Process all tables
                for table_name, columns in tables.items():
                    # Create description for the table
                    table_desc = f"Table {table_name} in database {db_name} containing columns: {', '.join(columns)}"
                    table_embedding = self.model.encode(table_desc).tolist()
                    
                    # Add table record
                    self.table_records.append({
                        "db_name": db_name,
                        "table_name": table_name,
                        "description": table_desc,
                        "embedding": table_embedding
                    })
                    
                    # Process all columns
                    for column in columns:
                        # Create description for the column - more detailed and specific
                        column_desc = f"Column named '{column}' within table '{table_name}' in database '{db_name}'"
                        column_embedding = self.model.encode(column_desc).tolist()
                        
                        # Add column record
                        self.column_records.append({
                            "db_name": db_name,
                            "table_name": table_name,
                            "column_name": column,
                            "description": column_desc,
                            "embedding": column_embedding
                        })
            
            # Rebuild the index
            self._rebuild_index()
            
            # Save embeddings to file
            self._save_embeddings()
            
            logger.info(f"Updated vector search index with {len(self.table_records)} tables and {len(self.column_records)} columns")
            return True
            
        except Exception as e:
            logger.error(f"Error updating vector search: {e}")
            return False
    
    def search(self, query: str, top_k: int = 3) -> List[Dict[str, Any]]:
        """
        Search for most relevant tables and columns for a query
        
        Args:
            query: User query
            top_k: Number of results to return
            
        Returns:
            List of matching records with scores
        """
        # Load the model if not already loaded
        if not self._load_model():
            logger.warning("Vector search is not available. Cannot perform search.")
            return []
        
        try:
            # Encode the query
            query_embedding = self.model.encode(query).reshape(1, -1).astype('float32')
            
            # Search the index
            distances, indices = self.index.search(query_embedding, min(top_k, self.index.ntotal))
            
            # Get the actual records
            results = []
            for i, idx in enumerate(indices[0]):
                score = 1.0 / (1.0 + distances[0][i])  # Convert distance to similarity score
                
                # Determine if this is a table or column record
                if idx < len(self.table_records):
                    record = self.table_records[idx]
                    result_type = "table"
                else:
                    # Adjust index for column records
                    col_idx = idx - len(self.table_records)
                    if col_idx < len(self.column_records):
                        record = self.column_records[col_idx]
                        result_type = "column"
                    else:
                        # This shouldn't happen, but just in case
                        continue
                
                # Add result with score
                results.append({
                    "db_name": record.get("db_name"),
                    "table_name": record.get("table_name"),
                    "column_name": record.get("column_name") if "column_name" in record else None,
                    "type": result_type,
                    "score": score
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Error during vector search: {e}")
            return []
    
    def get_best_db_and_table(self, query: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Get the most relevant database and table for a query
        
        Args:
            query: User query
            
        Returns:
            Tuple of (database_name, table_name) or (None, None) if no match
        """
        results = self.search(query, top_k=5)
        
        if not results:
            return None, None
        
        # Group by database and sum scores
        db_scores = {}
        for result in results:
            db_name = result.get("db_name")
            score = result.get("score", 0)
            
            if db_name in db_scores:
                db_scores[db_name] += score
            else:
                db_scores[db_name] = score
        
        # Find database with highest score
        best_db = max(db_scores.items(), key=lambda x: x[1])[0] if db_scores else None
        
        if not best_db:
            return None, None
        
        # Find best table in the best database
        table_scores = {}
        for result in results:
            if result.get("db_name") == best_db:
                table_name = result.get("table_name")
                score = result.get("score", 0)
                
                if table_name in table_scores:
                    table_scores[table_name] += score
                else:
                    table_scores[table_name] = score
        
        # Find table with highest score
        best_table = max(table_scores.items(), key=lambda x: x[1])[0] if table_scores else None
        
        return best_db, best_table 