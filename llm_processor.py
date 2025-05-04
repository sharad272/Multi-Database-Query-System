import requests
import json
import logging
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LLMProcessor:
    """
    Handles communication with LLM models for natural language to SQL conversion
    and other processing tasks using Ollama API
    """
    
    def __init__(self, model_name: str = "deepseek-r1:1.5b"):
        """
        Initialize the LLM processor with the specified model
        
        Args:
            model_name: Name of the Ollama model to use
        """
        self.model_name = model_name
        self.ollama_url = "http://localhost:11434/api/generate"
        self.sql_system_prompt = """
        You are a SQL expert. Your task is to convert natural language queries into valid SQL.
        Only return the SQL query without any explanation. Do not include markdown formatting or backticks.
        Make sure your SQL is compatible with SQLite, MySQL, or PostgreSQL.
        """
        self.summary_system_prompt = """
        You are a data analyst. Your task is to provide a clear, concise summary of query results.
        Focus on the most important insights and patterns in the data.
        Keep your response under 5 sentences unless more detail is absolutely necessary.
        """
    
    def generate_sql(self, 
                     query: str, 
                     db_name: str, 
                     table_info: Dict[str, List[str]]) -> Tuple[bool, str]:
        """
        Generate SQL from natural language query
        
        Args:
            query: Natural language query
            db_name: Name of the database
            table_info: Dictionary mapping table names to column lists
            
        Returns:
            Tuple of (success, result) where result is either SQL or error message
        """
        try:
            # Format table schema information for the prompt
            schema_info = self._format_schema_info(table_info)
            
            # Construct the prompt
            user_prompt = f"""
            Database: {db_name}
            
            Tables and columns:
            {schema_info}
            
            Convert this query to SQL: {query}
            """
            
            # Prepare the request
            payload = {
                "model": self.model_name,
                "prompt": user_prompt,
                "system": self.sql_system_prompt,
                "stream": False,
                "temperature": 0.1  # Lower temperature for more deterministic SQL generation
            }
            
            # Make the request
            response = requests.post(self.ollama_url, json=payload)
            
            if response.status_code == 200:
                result = response.json()
                sql = result.get("response", "").strip()
                return True, sql
            else:
                error_msg = f"Error calling Ollama API: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return False, error_msg
                
        except Exception as e:
            error_msg = f"Error generating SQL: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def generate_sql_stream(self, 
                          query: str, 
                          db_name: str, 
                          table_info: Dict[str, List[str]],
                          callback=None) -> Tuple[bool, str]:
        """
        Generate SQL from natural language query with streaming output
        
        Args:
            query: Natural language query
            db_name: Name of the database
            table_info: Dictionary mapping table names to column lists
            callback: Function to call with each chunk of text as it arrives
            
        Returns:
            Tuple of (success, final_sql) where final_sql is the complete SQL query
        """
        try:
            # Format table schema information for the prompt
            schema_info = self._format_schema_info(table_info)
            
            # Construct the prompt
            user_prompt = f"""
            Database: {db_name}
            
            Tables and columns:
            {schema_info}
            
            Convert this query to SQL: {query}
            """
            
            # Prepare the request with streaming enabled
            payload = {
                "model": self.model_name,
                "prompt": user_prompt,
                "system": self.sql_system_prompt,
                "stream": True,
                "temperature": 0.1  # Lower temperature for more deterministic SQL generation
            }
            
            # Make the streaming request
            with requests.post(self.ollama_url, json=payload, stream=True) as response:
                if response.status_code != 200:
                    error_msg = f"Error calling Ollama API: {response.status_code}"
                    logger.error(error_msg)
                    return False, error_msg
                
                # Initialize accumulator for the full SQL
                full_sql = ""
                
                # Process the stream
                for line in response.iter_lines():
                    if line:
                        try:
                            chunk = json.loads(line)
                            if "response" in chunk:
                                sql_chunk = chunk["response"]
                                full_sql += sql_chunk
                                
                                # Call the callback with the accumulated SQL so far
                                if callback:
                                    callback(full_sql)
                        except json.JSONDecodeError:
                            pass  # Skip invalid JSON lines
                
                return True, full_sql.strip()
                
        except Exception as e:
            error_msg = f"Error in streaming SQL generation: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def summarize_results(self, 
                        query: str, 
                        results_df: pd.DataFrame,
                        sql_query: str) -> Tuple[bool, str]:
        """
        Generate a natural language summary of query results
        
        Args:
            query: Original natural language query
            results_df: DataFrame containing query results
            sql_query: The SQL query that was executed
            
        Returns:
            Tuple of (success, result) where result is either summary or error message
        """
        try:
            # Convert DataFrame to readable format
            if len(results_df) > 0:
                # Get basic stats for numeric columns
                numeric_cols = results_df.select_dtypes(include=['number']).columns
                stats = {}
                for col in numeric_cols:
                    stats[col] = {
                        'min': results_df[col].min(),
                        'max': results_df[col].max(),
                        'avg': results_df[col].mean(),
                        'sum': results_df[col].sum()
                    }
                
                stats_text = ""
                for col, val in stats.items():
                    stats_text += f"Column {col} stats: min={val['min']}, max={val['max']}, avg={val['avg']:.2f}, sum={val['sum']}\n"
                
                # Convert first few rows to string representation
                max_rows = min(5, len(results_df))
                rows_text = results_df.head(max_rows).to_string()
                
                data_summary = f"""
                Number of rows: {len(results_df)}
                Number of columns: {len(results_df.columns)}
                Column names: {', '.join(results_df.columns)}
                
                Statistics:
                {stats_text}
                
                First {max_rows} rows:
                {rows_text}
                """
            else:
                data_summary = "The query returned no results."
            
            # Construct the prompt
            user_prompt = f"""
            Original user query: {query}
            
            SQL Query executed: {sql_query}
            
            Query results:
            {data_summary}
            
            Please provide a concise summary of these results.
            """
            
            # Prepare the request
            payload = {
                "model": self.model_name,
                "prompt": user_prompt,
                "system": self.summary_system_prompt,
                "stream": False,
                "temperature": 0.7  # Higher temperature for more natural language
            }
            
            # Make the request
            response = requests.post(self.ollama_url, json=payload)
            
            if response.status_code == 200:
                result = response.json()
                summary = result.get("response", "").strip()
                return True, summary
            else:
                error_msg = f"Error calling Ollama API: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return False, error_msg
                
        except Exception as e:
            error_msg = f"Error generating summary: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def summarize_results_stream(self, 
                               query: str, 
                               results_df: pd.DataFrame,
                               sql_query: str,
                               callback=None) -> bool:
        """
        Generate a natural language summary of query results with streaming output
        
        Args:
            query: Original natural language query
            results_df: DataFrame containing query results
            sql_query: The SQL query that was executed
            callback: Function to call with each chunk of text as it arrives
            
        Returns:
            Success status
        """
        try:
            # Convert DataFrame to readable format (same as non-streaming version)
            if len(results_df) > 0:
                # Get basic stats for numeric columns
                numeric_cols = results_df.select_dtypes(include=['number']).columns
                stats = {}
                for col in numeric_cols:
                    stats[col] = {
                        'min': results_df[col].min(),
                        'max': results_df[col].max(),
                        'avg': results_df[col].mean(),
                        'sum': results_df[col].sum()
                    }
                
                stats_text = ""
                for col, val in stats.items():
                    stats_text += f"Column {col} stats: min={val['min']}, max={val['max']}, avg={val['avg']:.2f}, sum={val['sum']}\n"
                
                # Convert first few rows to string representation
                max_rows = min(5, len(results_df))
                rows_text = results_df.head(max_rows).to_string()
                
                data_summary = f"""
                Number of rows: {len(results_df)}
                Number of columns: {len(results_df.columns)}
                Column names: {', '.join(results_df.columns)}
                
                Statistics:
                {stats_text}
                
                First {max_rows} rows:
                {rows_text}
                """
            else:
                data_summary = "The query returned no results."
            
            # Construct the prompt (same as non-streaming version)
            user_prompt = f"""
            Original user query: {query}
            
            SQL Query executed: {sql_query}
            
            Query results:
            {data_summary}
            
            Please provide a concise summary of these results.
            """
            
            # Prepare the request, but with streaming enabled
            payload = {
                "model": self.model_name,
                "prompt": user_prompt,
                "system": self.summary_system_prompt,
                "stream": True,  # Enable streaming
                "temperature": 0.7
            }
            
            # Make the streaming request
            with requests.post(self.ollama_url, json=payload, stream=True) as response:
                if response.status_code != 200:
                    error_msg = f"Error calling Ollama API: {response.status_code}"
                    logger.error(error_msg)
                    return False
                
                # Initialize accumulator for the full text
                full_text = ""
                
                # Process the stream
                for line in response.iter_lines():
                    if line:
                        try:
                            chunk = json.loads(line)
                            if "response" in chunk:
                                text_chunk = chunk["response"]
                                full_text += text_chunk
                                
                                # Call the callback with the accumulated text so far
                                if callback:
                                    callback(full_text)
                        except json.JSONDecodeError:
                            pass  # Skip invalid JSON lines
                
                return True
                
        except Exception as e:
            error_msg = f"Error in streaming summary: {str(e)}"
            logger.error(error_msg)
            return False
    
    def _format_schema_info(self, table_info: Dict[str, List[str]]) -> str:
        """Format table schema information for the prompt"""
        schema_text = ""
        for table_name, columns in table_info.items():
            schema_text += f"{table_name} ({', '.join(columns)})\n"
        return schema_text
    
    def is_available(self) -> bool:
        """Check if Ollama service is available"""
        try:
            # Simple check to see if Ollama is running
            response = requests.get("http://localhost:11434/api/tags")
            return response.status_code == 200
        except:
            return False


# Fallback SQL generation without LLM
def simple_sql_generation(query: str, table_name: str) -> str:
    """
    Generate a simple SQL query without using LLM
    For when Ollama/LLM is not available
    
    Args:
        query: Natural language query
        table_name: Name of the table to query
        
    Returns:
        A simple SQL query
    """
    # Convert query to lowercase and check for keywords
    query_lower = query.lower()
    
    # Check for count type queries
    if "how many" in query_lower or "count" in query_lower:
        return f"SELECT COUNT(*) FROM {table_name}"
    
    # Check for specific field selection
    if "select" in query_lower and "from" in query_lower:
        # User might have typed a full or partial SQL query
        # This is a simple approach and might need refinement
        return f"SELECT * FROM {table_name} LIMIT 10"
    
    # Check for filtering conditions
    for filter_word in ["where", "with", "has", "contains", "equals", "equal to", "greater than", "less than"]:
        if filter_word in query_lower:
            # This is a simplistic approach - in a real app, you'd use NLP to extract entities
            return f"SELECT * FROM {table_name} LIMIT 10"
    
    # Default to a simple select all query
    return f"SELECT * FROM {table_name} LIMIT 10" 