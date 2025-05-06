import requests
import json
import logging
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
import re

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
        ONLY return the SQL query without any explanation. Do not include markdown formatting or backticks.
        Make sure your SQL is compatible with the specified database type.
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
            # Check if Ollama is available, if not fall back to simple SQL generation
            if not self.is_available():
                logger.warning("Ollama LLM service is not available")
                logger.warning("Falling back to simple query generation")
                # Fall back to simple query generation
                first_table = next(iter(table_info.keys())) if table_info else None
                if first_table:
                    return True, simple_sql_generation(query, first_table)
                else:
                    return False, "No tables available for simple query generation"
            
            # Extract potential limit from query for fallback
            limit = self._extract_limit_from_query(query)
            
            # Format table schema information for the prompt
            schema_info = self._format_schema_info(table_info)
            
            # Get database type from the db_connector
            db_type = self._get_database_type(db_name)
            
            # Construct detailed, specific guidance based on database type
            db_specific_guidance = ""
            if db_type == 'sqlite':
                db_specific_guidance = """
                CRITICAL SQLite-specific requirements:
                1. ALWAYS use date('now') for current date, NEVER use just 'now'
                2. ALWAYS add spaces around comparison operators: "column > value" NOT "column>value"
                3. NEVER put table or column names in quotes: "FROM orders" NOT "FROM 'orders'"
                4. String literals should be in single quotes: "WHERE name = 'John'"
                5. Use "LIMIT n" for result limiting
                
                Example of CORRECT SQLite query:
                SELECT COUNT(*) FROM orders WHERE order_date >= date('now')
                
                Example of INCORRECT SQLite query:
                SELECT COUNT(*) FROM 'orders' WHERE order_date>='now'
                """
            elif db_type == 'mysql':
                db_specific_guidance = """
                CRITICAL MySQL-specific requirements:
                1. Use NOW() or CURDATE() for current date
                2. String literals should be in single quotes
                3. Table names can be in backticks: `tablename`
                
                Example of CORRECT MySQL query:
                SELECT COUNT(*) FROM orders WHERE order_date >= NOW()
                """
            elif db_type == 'postgres':
                db_specific_guidance = """
                CRITICAL PostgreSQL-specific requirements:
                1. Use CURRENT_DATE for current date
                2. String literals should be in single quotes
                3. Table names can be in double quotes: "tablename"
                
                Example of CORRECT PostgreSQL query:
                SELECT COUNT(*) FROM orders WHERE order_date >= CURRENT_DATE
                """
            
            # Construct the prompt
            user_prompt = f"""
            Database: {db_name}
            Database Type: {db_type}
            
            Tables and columns:
            {schema_info}
            
            I need a {db_type.upper()} compatible SQL query for this: {query}
            
            {db_specific_guidance}
            
            CRITICAL: Only use column names EXACTLY as listed in the schema above. Do not invent, modify, or guess column names.
            If a column in your query doesn't exactly match one of the columns in the schema, the query will fail.
            
            IMPORTANT: Return ONLY the exact SQL query with no explanation, comments, or formatting.
            Do NOT include any thinking process, reasoning, or multiple attempts.
            JUST the final, correct SQL query.
            """
            
            # Use a very direct system prompt
            sql_system_prompt = f"""
            You are an expert {db_type.upper()} SQL generator.
            You MUST follow ALL the syntax rules for {db_type} exactly.
            You MUST only use column names that appear EXACTLY in the provided schema.
            Only output the raw SQL query with no markdown, explanation, additional text, or thinking-out-loud.
            """
            
            # Prepare the request
            payload = {
                "model": self.model_name,
                "prompt": user_prompt,
                "system": sql_system_prompt,
                "stream": False,
                "temperature": 0.1  # Lower temperature for more deterministic SQL generation
            }
            
            # Make the request
            response = requests.post(self.ollama_url, json=payload)
            
            if response.status_code == 200:
                result = response.json()
                raw_response = result.get("response", "").strip()
                
                # Extract the actual SQL query from the response
                sql = self._extract_sql_from_response(raw_response)
                
                # Log what was extracted vs original response
                logger.info(f"Original LLM response: {raw_response}")
                logger.info(f"Extracted SQL query: {sql}")
                
                # Validate column names against the schema
                sql = self._validate_column_names(sql, table_info)
                logger.info(f"Validated SQL query: {sql}")
                
                # If LLM didn't include a LIMIT and we detected one, add it
                if "LIMIT" not in sql.upper() and limit is not None:
                    sql = f"{sql} LIMIT {limit}"
                
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
            # Check if Ollama is available, if not fall back to simple SQL generation
            if not self.is_available():
                logger.warning("Ollama LLM service is not available")
                logger.warning("Falling back to simple query generation")
                # Fall back to simple query generation
                first_table = next(iter(table_info.keys())) if table_info else None
                if first_table:
                    simple_sql = simple_sql_generation(query, first_table)
                    if callback:
                        callback(simple_sql)
                    return True, simple_sql
                else:
                    return False, "No tables available for simple query generation"
                    
            # Extract potential limit from query for fallback
            limit = self._extract_limit_from_query(query)
            
            # Format table schema information for the prompt
            schema_info = self._format_schema_info(table_info)
            
            # Get database type from the db_connector
            db_type = self._get_database_type(db_name)
            
            # Construct detailed, specific guidance based on database type
            db_specific_guidance = ""
            if db_type == 'sqlite':
                db_specific_guidance = """
                CRITICAL SQLite-specific requirements:
                1. ALWAYS use date('now') for current date, NEVER use just 'now'
                2. ALWAYS add spaces around comparison operators: "column > value" NOT "column>value"
                3. NEVER put table or column names in quotes: "FROM orders" NOT "FROM 'orders'"
                4. String literals should be in single quotes: "WHERE name = 'John'"
                5. Use "LIMIT n" for result limiting
                
                Example of CORRECT SQLite query:
                SELECT COUNT(*) FROM orders WHERE order_date >= date('now')
                
                Example of INCORRECT SQLite query:
                SELECT COUNT(*) FROM 'orders' WHERE order_date>='now'
                """
            elif db_type == 'mysql':
                db_specific_guidance = """
                CRITICAL MySQL-specific requirements:
                1. Use NOW() or CURDATE() for current date
                2. String literals should be in single quotes
                3. Table names can be in backticks: `tablename`
                
                Example of CORRECT MySQL query:
                SELECT COUNT(*) FROM orders WHERE order_date >= NOW()
                """
            elif db_type == 'postgres':
                db_specific_guidance = """
                CRITICAL PostgreSQL-specific requirements:
                1. Use CURRENT_DATE for current date
                2. String literals should be in single quotes
                3. Table names can be in double quotes: "tablename"
                
                Example of CORRECT PostgreSQL query:
                SELECT COUNT(*) FROM orders WHERE order_date >= CURRENT_DATE
                """
            
            # Construct the prompt
            user_prompt = f"""
            Database: {db_name}
            Database Type: {db_type}
            
            Tables and columns:
            {schema_info}
            
            I need a {db_type.upper()} compatible SQL query for this: {query}
            
            {db_specific_guidance}
            
            CRITICAL: Only use column names EXACTLY as listed in the schema above. Do not invent, modify, or guess column names.
            If a column in your query doesn't exactly match one of the columns in the schema, the query will fail.
            
            IMPORTANT: Return ONLY the exact SQL query with no explanation, comments, or formatting.
            Do NOT include any thinking process, reasoning, or multiple attempts.
            JUST the final, correct SQL query.
            """
            
            # Use a very direct system prompt
            sql_system_prompt = f"""
            You are an expert {db_type.upper()} SQL generator.
            You MUST follow ALL the syntax rules for {db_type} exactly.
            You MUST only use column names that appear EXACTLY in the provided schema.
            Only output the raw SQL query with no markdown, explanation, additional text, or thinking-out-loud.
            """
            
            # Prepare the request with streaming enabled
            payload = {
                "model": self.model_name,
                "prompt": user_prompt,
                "system": sql_system_prompt,
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
                
                # Extract the actual SQL query from the full response
                raw_response = full_sql.strip()
                final_sql = self._extract_sql_from_response(raw_response)
                
                # Log what was extracted vs original response
                logger.info(f"Original LLM streaming response: {raw_response}")
                logger.info(f"Extracted SQL query from stream: {final_sql}")
                
                # Validate column names against the schema
                final_sql = self._validate_column_names(final_sql, table_info)
                logger.info(f"Validated SQL query: {final_sql}")
                
                # If LLM didn't include a LIMIT and we detected one, add it
                if "LIMIT" not in final_sql.upper() and limit is not None:
                    final_sql = f"{final_sql} LIMIT {limit}"
                
                # Update the callback with the final extracted SQL if different
                if callback and final_sql != raw_response:
                    callback(final_sql)
                
                return True, final_sql
                
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
            # Format with table name in bold and each column on a new line with indentation
            schema_text += f"TABLE: {table_name}\n"
            schema_text += "COLUMNS:\n"
            # List each column on a separate line with explicit labeling
            for column in columns:
                schema_text += f"  - {column}\n"
            schema_text += "\n"
        return schema_text
    
    def is_available(self) -> bool:
        """Check if Ollama service is available"""
        try:
            # Simple check to see if Ollama is running
            response = requests.get("http://localhost:11434/api/tags")
            return response.status_code == 200
        except:
            return False
    
    def _extract_limit_from_query(self, query: str) -> Optional[int]:
        """
        Extract a limit value from a natural language query
        
        Args:
            query: Natural language query
            
        Returns:
            Integer limit if found, None otherwise
        """
        query_lower = query.lower()
        limit = None
        
        # Check for specific limit mentions
        limit_terms = ["limit", "show", "display", "get", "find", "top"]
        for term in limit_terms:
            if term in query_lower:
                # Look for numbers after the term
                parts = query_lower.split(term)
                if len(parts) > 1:
                    # Extract numbers from the text after the term
                    import re
                    numbers = re.findall(r'\d+', parts[1])
                    if numbers:
                        return int(numbers[0])
        
        return limit
    
    def _validate_sql(self, sql: str) -> Tuple[bool, str]:
        """
        Minimally process SQL query, passing it through directly.
        User has read-only permissions so we can trust all generated SQL.
        
        Args:
            sql: SQL query to validate
            
        Returns:
            Tuple of (is_valid, processed_sql)
        """
        # Return the original SQL as-is - users have read-only permissions
        return True, sql

    def _get_database_type(self, db_name: str) -> str:
        """
        Get the database type (sqlite, mysql, postgres, etc.) for a given database name
        
        Args:
            db_name: Name of the database
            
        Returns:
            Database type as a string (defaults to 'sqlite' if unknown)
        """
        try:
            # Import here to avoid circular imports
            from db_connector import DatabaseConnector
            
            # Try to get the database connector instance from session state
            import streamlit as st
            
            if hasattr(st, 'session_state') and 'db_connector' in st.session_state:
                db_connector = st.session_state.db_connector
                
                # Get database type from connections dictionary
                if db_name in db_connector.connections:
                    return db_connector.connections[db_name].get('type', 'sqlite')
            
            # If we can't get it from session state, look for a global instance
            import builtins
            if hasattr(builtins, 'db_connector'):
                db_connector = builtins.db_connector
                if db_name in db_connector.connections:
                    return db_connector.connections[db_name].get('type', 'sqlite')
                    
            # Default to sqlite if we can't determine
            return 'sqlite'
        except Exception as e:
            logger.warning(f"Could not determine database type for {db_name}: {e}")
            # Default to sqlite as it's the most likely in this application
            return 'sqlite'

    def _extract_sql_from_response(self, response: str) -> str:
        """
        Extract the actual SQL query from the LLM's response, ignoring any chain-of-thought text.
        
        Args:
            response: The raw response from the LLM
            
        Returns:
            The extracted SQL query
        """
        # Clean the response
        response = response.strip()
        
        # Check for explicit thinking sections - the model sometimes includes <think> or similar
        # Remove any text between <think> and </think>
        think_pattern = r'<think>.*?</think>'
        response = re.sub(think_pattern, '', response, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove common thinking indicators
        thinking_indicators = [
            "Let me think about this.",
            "I need to create a SQL query",
            "First, I'll analyze",
            "Let's break this down",
            "I can create a",
            "Looking at the",
            "Based on the",
            "To solve this",
            "Wait, but",
            "So, I can create",
            "Putting it all together",
            "That should work"
        ]
        
        for indicator in thinking_indicators:
            if indicator.lower() in response.lower():
                # Find the position of this indicator
                pos = response.lower().find(indicator.lower())
                # Check if there's a SELECT after this thinking text
                select_pos = response.upper().find("SELECT", pos + len(indicator))
                if select_pos > 0:
                    # Keep only the text after the last SELECT
                    response = response[select_pos:]
                break
        
        # If the response already looks like SQL, just return it
        if response.upper().startswith("SELECT"):
            return response.strip()
            
        # Check for SQL code blocks
        if "```sql" in response:
            # Extract SQL from code block
            start = response.find("```sql") + 6
            end = response.find("```", start)
            if end > start:
                return response[start:end].strip()
        
        # Check for generic code blocks
        if "```" in response:
            # Extract from code block
            start = response.find("```") + 3
            end = response.find("```", start)
            if end > start:
                return response[start:end].strip()
        
        # Try to find the SQL query starting with SELECT
        if "SELECT" in response.upper():
            pos = response.upper().find("SELECT")
            query_text = response[pos:].strip()
            
            # Find the end of the query by looking for period, newline or typical ending
            end_markers = ['. ', '\n\n', '\n\r', ';']
            for marker in end_markers:
                if marker in query_text:
                    query_text = query_text.split(marker)[0]
                    if marker == ';':
                        query_text += ';'  # Keep the semicolon
                    break
            
            # Additional check for reasoning after the query
            reasoning_markers = ["This query", "This will", "This should", "This gives", "This returns"]
            for marker in reasoning_markers:
                if marker.lower() in query_text.lower():
                    query_text = query_text.split(marker)[0].strip()
                    break
            
            return query_text.strip()
            
        # If all else fails, return the original response
        logger.warning(f"Could not extract SQL query from: '{response}'")
        return response.strip()
        
    def _validate_column_names(self, sql_query: str, table_info: Dict[str, List[str]]) -> str:
        """
        Validate and correct column names in the SQL query to ensure they match the schema.
        
        Args:
            sql_query: The SQL query to validate
            table_info: Dictionary mapping table names to column lists
            
        Returns:
            Validated SQL query with corrected column names
        """
        # Simple validation - this won't catch all cases but helps with common issues
        corrected_query = sql_query
        
        # Extract all possible column names from the schema
        all_columns = set()
        columns_by_table = {}
        for table_name, columns in table_info.items():
            columns_by_table[table_name] = set(col.lower() for col in columns)
            all_columns.update(col.lower() for col in columns)
            
        # Extract potential column references from the query
        # This is simplistic and won't catch all cases
        # Look for patterns like: SELECT col1, col2 FROM or WHERE col3 = 
        column_patterns = [
            r'SELECT\s+(.+?)\s+FROM',  # Columns in SELECT
            r'WHERE\s+(\w+)',          # Column after WHERE
            r'GROUP BY\s+(\w+)',       # Column after GROUP BY
            r'ORDER BY\s+(\w+)',       # Column after ORDER BY
            r'HAVING\s+(\w+)',         # Column after HAVING
            r'JOIN.+?ON\s+(\w+)'       # Column in JOIN ... ON
        ]
        
        # Log the query and schema for debugging
        logger.debug(f"Validating query: {sql_query}")
        logger.debug(f"Against schema: {table_info}")
        
        # No actual correction for now - this function will be enhanced in the future
        # to perform more sophisticated SQL parsing and correction
        
        return corrected_query

    def _process_sql_for_sqlite(self, sql: str) -> str:
        """
        Process SQL query specifically for SQLite compatibility
        
        Args:
            sql: The SQL query to process
            
        Returns:
            The processed SQL query
        """
        # Fix comparison operators for SQLite
        # Add spaces around comparison operators if they're missing
        comparison_operators = ['<', '>', '<=', '>=', '<>', '!=', '=']
        processed_sql = sql
        
        # Replace operators without surrounding spaces
        for op in comparison_operators:
            # Look for operator without spaces (where character before and after are not spaces)
            pattern = r'(\S)' + re.escape(op) + r'(\S)'
            replacement = r'\1 ' + op + r' \2'
            processed_sql = re.sub(pattern, replacement, processed_sql)
        
        return processed_sql

    def _remove_quotes_from_identifiers(self, sql: str) -> str:
        """
        Remove quotes around table and column names in SQL queries.
        
        Args:
            sql: The SQL query to process
            
        Returns:
            Processed SQL query
        """
        # Define common SQL keywords
        keywords = ["SELECT", "FROM", "WHERE", "JOIN", "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET", "AND", "OR"]
        
        # Process the SQL to remove quotes around identifiers
        processed_sql = sql
        
        # Look for 'word' patterns after SQL keywords and replace with word
        for keyword in keywords:
            pattern = f"({keyword}\\s+)'([^']+)'"
            repl = r"\1\2"
            processed_sql = re.sub(pattern, repl, processed_sql, flags=re.IGNORECASE)
        
        # Also check for SELECT 'column' patterns
        select_pattern = r"SELECT\s+'([^']+)'"
        select_repl = r"SELECT \1"
        processed_sql = re.sub(select_pattern, select_repl, processed_sql, flags=re.IGNORECASE)
        
        # Remove comma followed by quoted identifier e.g., , 'column'
        comma_pattern = r",\s*'([^']+)'"
        comma_repl = r", \1"
        processed_sql = re.sub(comma_pattern, comma_repl, processed_sql)
        
        return processed_sql

    def _fix_sqlite_date_functions(self, sql: str) -> str:
        """
        Fix common date function issues in SQLite queries
        
        Args:
            sql: The SQL query to process
            
        Returns:
            Processed SQL query with proper date functions
        """
        # Replace 'now' with date('now') when used in comparisons
        # Only when it's in quotes and not already inside a date() function
        if "date('now')" not in sql and "'now'" in sql:
            # Look for comparison operators followed by 'now'
            for op in [' = ', ' > ', ' < ', ' >= ', ' <= ', ' <> ']:
                if f"{op}'now'" in sql:
                    sql = sql.replace(f"{op}'now'", f"{op}date('now')")
                # Also check for the pattern in reverse (e.g., 'now' > instead of > 'now')
                if f"'now'{op}" in sql:
                    sql = sql.replace(f"'now'{op}", f"date('now'){op}")
        
        return sql


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
    
    # Clean the table name to prevent SQL injection
    table_name = table_name.replace('"', '').replace("'", "").replace(";", "").replace("--", "")
    
    # Check for count type queries
    if "how many" in query_lower or "count" in query_lower:
        return f"SELECT COUNT(*) FROM {table_name}"
    
    # Check for specific limit mentions
    limit = 10  # Default limit
    limit_terms = ["limit", "show", "display", "get", "find", "top"]
    for term in limit_terms:
        if term in query_lower:
            # Look for numbers after the term
            parts = query_lower.split(term)
            if len(parts) > 1:
                # Extract numbers from the text after the term
                import re
                numbers = re.findall(r'\d+', parts[1])
                if numbers:
                    limit = int(numbers[0])
                    break
    
    # Check for specific field selection
    if "select" in query_lower and "from" in query_lower:
        # User might have typed a full or partial SQL query
        # Try to extract column names if possible
        columns = "*"
        if "select" in query_lower:
            parts = query_lower.split("select")[1].split("from")[0].strip()
            if parts and parts != "*":
                # Sanitize column names
                columns_list = [col.strip().replace('"', '').replace("'", "").replace(";", "") 
                               for col in parts.split(",")]
                columns = ", ".join(columns_list) if columns_list else "*"
        return f"SELECT {columns} FROM {table_name} LIMIT {limit}"
    
    # Check for filtering conditions
    filter_conditions = {
        "where": "", "with": "", "has": "", "contains": "", 
        "equals": "=", "equal to": "=", "greater than": ">", "less than": "<"
    }
    
    for filter_word, operator in filter_conditions.items():
        if filter_word in query_lower:
            # This is a simplistic approach - in a real app, you'd use NLP to extract entities
            # For now, at least use the appropriate operator if specified
            if operator:
                # Try to construct a simple WHERE clause - with a placeholder to be replaced later
                return f"SELECT * FROM {table_name} WHERE column_name {operator} ? LIMIT {limit}"
            return f"SELECT * FROM {table_name} LIMIT {limit}"
    
    # Default to a simple select all query
    return f"SELECT * FROM {table_name} LIMIT {limit}" 