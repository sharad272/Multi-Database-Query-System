import streamlit as st
import os
import json
import pandas as pd
import glob
import threading
import time
from metadata_manager import MetadataManager
from db_connector import DatabaseConnector
from llm_processor import LLMProcessor, simple_sql_generation
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize session state variables if they don't exist
if 'db_connector' not in st.session_state:
    st.session_state.db_connector = DatabaseConnector()
    
if 'metadata_manager' not in st.session_state:
    st.session_state.metadata_manager = MetadataManager()
    
if 'llm_processor' not in st.session_state:
    # Default to DeepSeek model, but allow override
    model_name = os.environ.get("OLLAMA_MODEL", "deepseek-r1:1.5b")
    st.session_state.llm_processor = LLMProcessor(model_name=model_name)
    
if 'background_thread' not in st.session_state:
    st.session_state.background_thread = None
    
if 'auto_refresh_enabled' not in st.session_state:
    st.session_state.auto_refresh_enabled = True
    
if 'last_refresh_time' not in st.session_state:
    st.session_state.last_refresh_time = None

if 'show_metadata' not in st.session_state:
    st.session_state.show_metadata = False

def load_db_config():
    """Load database configuration from config file"""
    config_file = "db_config.json"
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading database config: {e}")
    return []

def save_db_config(db_configs):
    """Save database configuration to config file"""
    config_file = "db_config.json"
    try:
        with open(config_file, 'w') as f:
            json.dump(db_configs, f, indent=2)
        logger.info(f"Saved database configuration to {config_file}")
        return True
    except Exception as e:
        logger.error(f"Error saving database configuration: {e}")
        return False

def scan_for_databases(show_messages=True):
    """Scan for databases in the databases directory and update db_config.json"""
    # Look for SQLite databases
    sqlite_files = glob.glob('databases/*.db')
    
    # Load existing config
    db_configs = load_db_config()
    
    # Create a dictionary of existing configs by path for easier lookups
    existing_configs_by_path = {config['path']: config for config in db_configs if 'path' in config}
    
    updated = False
    
    # Add any new databases found
    for db_path in sqlite_files:
        if db_path not in existing_configs_by_path:
            # Create a name from the filename (without extension)
            db_name = os.path.splitext(os.path.basename(db_path))[0] + "_db"
            
            # Add to config
            db_configs.append({
                "name": db_name,
                "type": "sqlite",
                "path": db_path
            })
            logger.info(f"Added new database to config: {db_name} at {db_path}")
            updated = True
    
    # Save updated config if changes were made
    if updated:
        save_db_config(db_configs)
        if show_messages:
            st.success("Database configuration updated with newly found databases!")
    else:
        if show_messages:
            st.info("No new databases found.")
    
    return db_configs, updated

def init_database_connections():
    """Initialize database connections from config"""
    db_configs = load_db_config()
    connector = st.session_state.db_connector
    
    for db_config in db_configs:
        db_name = db_config.get('name')
        db_type = db_config.get('type')
        
        if db_type == 'sqlite':
            connector.add_sqlite_connection(
                db_name=db_name,
                db_path=db_config.get('path')
            )
        elif db_type == 'mysql':
            connector.add_mysql_connection(
                db_name=db_name,
                host=db_config.get('host'),
                user=db_config.get('user'),
                password=db_config.get('password'),
                database=db_config.get('database'),
                port=db_config.get('port', 3306)
            )
        elif db_type == 'postgres':
            connector.add_postgres_connection(
                db_name=db_name,
                host=db_config.get('host'),
                user=db_config.get('user'),
                password=db_config.get('password'),
                database=db_config.get('database'),
                port=db_config.get('port', 5432)
            )

def sync_all_database_metadata(show_messages=True):
    """Sync metadata from all connected databases"""
    connector = st.session_state.db_connector
    metadata_manager = st.session_state.metadata_manager
    
    db_configs = load_db_config()
    changes_detected = False
    
    for db_config in db_configs:
        db_name = db_config.get('name')
        schema_info = connector.get_schema_information(db_name)
        
        if schema_info:
            # Check if schema changed
            old_schema = metadata_manager.get_database_metadata(db_name) or {}
            has_changes = False
            
            # Check for new tables
            for table_name in schema_info:
                if table_name not in old_schema:
                    logger.info(f"New table detected: {table_name} in {db_name}")
                    has_changes = True
                    changes_detected = True
                else:
                    # Check for new columns
                    for column in schema_info[table_name]:
                        if column not in old_schema[table_name]:
                            logger.info(f"New column detected: {column} in {db_name}.{table_name}")
                            has_changes = True
                            changes_detected = True
            
            # Update metadata if changes detected
            if has_changes or not old_schema:
                metadata_manager.update_database_metadata(db_name, schema_info)
                logger.info(f"Updated metadata for {db_name} due to schema changes")
            else:
                logger.info(f"No schema changes detected for {db_name}")
        else:
            logger.warning(f"Failed to update metadata for {db_name}")
    
    if show_messages:
        if changes_detected:
            st.success("Database metadata synchronized - schema changes detected and updated!")
        else:
            st.success("Database metadata synchronized - no schema changes detected.")
    
    st.session_state.last_refresh_time = time.strftime("%Y-%m-%d %H:%M:%S")
    return changes_detected

def background_metadata_refresh():
    """Background thread function to periodically refresh metadata"""
    while st.session_state.auto_refresh_enabled:
        try:
            # Scan for new databases
            _, db_updated = scan_for_databases(show_messages=False)
            
            # If new databases were found, reinitialize connections
            if db_updated:
                logger.info("New databases found in background scan, reinitializing connections")
                st.session_state.db_connector = DatabaseConnector()
                init_database_connections()
            
            # Sync metadata
            changes = sync_all_database_metadata(show_messages=False)
            
            if changes or db_updated:
                logger.info("Background refresh detected database changes")
                # We'll let the UI show the updates on next refresh
        except Exception as e:
            logger.error(f"Error in background refresh: {e}")
        
        # Sleep for 5 minutes before next check
        time.sleep(300)

def start_background_refresh():
    """Start the background refresh thread if not already running"""
    # Make sure the thread exists in session state
    if 'background_thread' not in st.session_state:
        st.session_state.background_thread = None
        
    # Check if we need to create a new thread (either none exists or the existing one is not alive)
    thread_needs_start = (st.session_state.background_thread is None)
    
    if not thread_needs_start and hasattr(st.session_state.background_thread, 'is_alive'):
        thread_needs_start = not st.session_state.background_thread.is_alive()
    
    if thread_needs_start:
        st.session_state.auto_refresh_enabled = True
        st.session_state.background_thread = threading.Thread(target=background_metadata_refresh, daemon=True)
        st.session_state.background_thread.start()
        logger.info("Started background refresh thread")

def stop_background_refresh():
    """Stop the background refresh thread"""
    st.session_state.auto_refresh_enabled = False
    logger.info("Stopped background refresh thread (will terminate after current cycle)")

def display_metadata():
    """Display the current metadata in a structured way"""
    metadata = st.session_state.metadata_manager.get_all_metadata()
    
    if not metadata:
        st.info("No database metadata available. Please sync the metadata first.")
        return
    
    for db_name, tables in metadata.items():
        st.subheader(f"Database: {db_name}")
        
        for table_name, columns in tables.items():
            with st.expander(f"Table: {table_name}"):
                st.write("Columns:")
                for col in columns:
                    st.write(f"- {col}")

def process_user_query(query):
    """Process a user query by finding the right database and executing the query"""
    metadata_manager = st.session_state.metadata_manager
    db_connector = st.session_state.db_connector
    llm_processor = st.session_state.llm_processor
    
    # First attempt to find the right database and table using metadata manager
    with st.spinner("Finding relevant database and table..."):
        # Use the metadata manager to identify the database and table based on the query
        db_name, table_name = metadata_manager.get_database_and_table(query)
        
        if not db_name:
            # If no match by direct method, try to find explicit table mentions in the query
            db_name = metadata_manager.get_database_for_query(query)
            
            if db_name:
                # Try to get the table for this database
                table_name = metadata_manager.get_table_for_query(query, db_name)
    
    if not db_name:
        st.error("Couldn't determine which database to query. Please try a more specific query that includes a table name.")
        
        # Show available tables to help the user
        all_tables = metadata_manager.get_all_tables()
        if all_tables:
            st.write("Available tables across all databases:")
            for table in sorted(all_tables):
                st.write(f"- {table}")
        return
    
    st.info(f"Querying database: {db_name}")
    
    # Get the database schema for the selected database
    db_schema = metadata_manager.get_database_metadata(db_name)
    
    # If we don't have a table name yet, try to find one
    if not table_name and db_schema:
        # Get all table names from the schema
        table_names = list(db_schema.keys())
        
        # If there's only one table, use it
        if len(table_names) == 1:
            table_name = table_names[0]
            st.info(f"Using table: {table_name}")
        # Otherwise, use the first table but warn the user
        elif len(table_names) > 1:
            table_name = table_names[0]
            st.warning(f"Multiple tables detected. Using {table_name}. For better results, specify a table in your query.")
        else:
            st.error(f"No tables found in database {db_name}.")
            return
    
    # Try to use LLM to generate SQL if Ollama is available
    use_llm = llm_processor.is_available()
    
    # IMPORTANT: Track the actual SQL query that will be executed
    sql_to_execute = None
    
    if use_llm:
        with st.spinner("Generating SQL using DeepSeek-r1..."):
            try:
                # Create a placeholder for the SQL query
                sql_container = st.container()
                with sql_container:
                    st.subheader("Generated SQL")
                    sql_text = st.empty()
                
                # First try streaming SQL generation
                if hasattr(llm_processor, 'generate_sql_stream'):
                    streaming_success, sql_query = llm_processor.generate_sql_stream(
                        query, db_name, db_schema,
                        callback=lambda text: sql_text.code(text, language="sql")
                    )
                    if not streaming_success:
                        st.warning(f"Streaming SQL generation failed: {sql_query}. Falling back to simple SQL generation.")
                        sql_query = simple_sql_generation(query, table_name)
                        sql_text.code(sql_query, language="sql")
                    else:
                        # Make sure the display shows the extracted SQL
                        sql_text.code(sql_query, language="sql")
                    
                    # Set the SQL to execute
                    sql_to_execute = sql_query
                    
                else:
                    # Fall back to non-streaming method
                    success, sql_query = llm_processor.generate_sql(query, db_name, db_schema)
                    if not success:
                        st.warning(f"LLM error: {sql_query}. Falling back to simple SQL generation.")
                        sql_query = simple_sql_generation(query, table_name)
                        sql_text.code(sql_query, language="sql")
                    else:
                        # Display the extracted SQL
                        sql_text.code(sql_query, language="sql")
                    
                    # Set the SQL to execute
                    sql_to_execute = sql_query
                    
                # Log the final SQL
                logger.info(f"Final SQL from LLM extraction: {sql_to_execute}")
                    
            except Exception as e:
                st.warning(f"Error during LLM SQL generation: {str(e)}. Falling back to simple SQL generation.")
                sql_query = simple_sql_generation(query, table_name)
                sql_text.code(sql_query, language="sql")
                sql_to_execute = sql_query
    else:
        st.info("Ollama not available. Using simple SQL generation.")
        sql_query = simple_sql_generation(query, table_name)
        st.subheader("Generated SQL")
        st.code(sql_query, language="sql")
        sql_to_execute = sql_query
    
    # IMPORTANT: Make absolutely sure we're using the correct SQL query
    # Display the final SQL query that will be executed
    st.text("Executing query exactly as shown above")
    
    # Log the query that will be executed
    logger.info(f"SQL query to execute: {sql_to_execute}")
    
    # Execute the query
    with st.spinner("Executing query..."):
        try:
            success, results = db_connector.execute_query(db_name, sql_to_execute)
        
            if success:
                if isinstance(results, dict) and "columns" in results and "data" in results:
                    df = pd.DataFrame(results["data"], columns=results["columns"])
                    
                    # Display the data
                    st.subheader("Query Results")
                    st.dataframe(df)
                    
                    # Generate summary if LLM is available and we have results
                    if use_llm and len(df) > 0:
                        with st.spinner("Generating insights using DeepSeek-r1..."):
                            try:
                                # Create a placeholder for streaming output
                                insights_container = st.empty()
                                insights_placeholder = st.container()
                                
                                with insights_placeholder:
                                    st.subheader("AI Insights")
                                    insights_text = st.empty()
                                    
                                # First try streaming approach
                                try:
                                    # If summarize_results_stream is available, use it
                                    if hasattr(llm_processor, 'summarize_results_stream'):
                                        streaming_success = llm_processor.summarize_results_stream(
                                            query, df, sql_to_execute, 
                                            callback=lambda text: insights_text.markdown(text)
                                        )
                                        if streaming_success:
                                            # Streaming was successful
                                            pass
                                        else:
                                            # Fall back to non-streaming
                                            summary_success, summary = llm_processor.summarize_results(query, df, sql_to_execute)
                                            if summary_success:
                                                insights_text.markdown(summary)
                                            else:
                                                st.warning(f"Couldn't generate summary: {summary}")
                                    else:
                                        # Use traditional approach if streaming not available
                                        summary_success, summary = llm_processor.summarize_results(query, df, sql_to_execute)
                                        if summary_success:
                                            insights_text.markdown(summary)
                                        else:
                                            st.warning(f"Couldn't generate summary: {summary}")
                                except Exception as e:
                                    st.warning(f"Error with streaming insights: {str(e)}")
                                    # Fall back to traditional approach
                                    summary_success, summary = llm_processor.summarize_results(query, df, sql_to_execute)
                                    if summary_success:
                                        insights_text.markdown(summary)
                                    else:
                                        st.warning(f"Couldn't generate summary: {summary}")
                            except Exception as e:
                                st.warning(f"Error generating insights: {str(e)}")
                else:
                    st.success(results)
            else:
                error_msg = results
                # Show the error to the user
                st.error(f"Query execution failed: {error_msg}")
                
                # Log the exact query that failed
                logger.error(f"Failed query: {sql_to_execute}")
                
                # Special handling for specific errors
                if "syntax error" in error_msg.lower():
                    # Show error details but don't auto-fallback to simple query
                    # Instead give advice on fixing the query
                    db_type = db_connector.connections[db_name].get('type', 'unknown')
                    st.warning(f"""
                    This appears to be a syntax error in the SQL query. The database type is {db_type.upper()}.
                    
                    Common issues:
                    - Date functions differ across databases (GETDATE() vs NOW() vs date('now'))
                    - String comparison operators might need quotes
                    - Some operators like TOP vs LIMIT vary across databases
                    
                    Try reformulating your query or providing more details.
                    """)
        except Exception as e:
            st.error(f"Error during query execution: {str(e)}")
            # Log the query that failed for debugging
            logger.error(f"Failed query: {sql_to_execute}")

def main():
    st.title("Multi-Database Query System")
    
    # Initialize database connections on app startup
    init_database_connections()
    
    # Sidebar for actions
    with st.sidebar:
        st.header("Actions")
        
        # LLM model selection
        st.subheader("LLM Configuration")
        if st.session_state.llm_processor.is_available():
            st.success("✅ Ollama is available")
            model_name = st.text_input("Model Name", 
                                     value=st.session_state.llm_processor.model_name,
                                     help="Enter the name of the Ollama model to use")
            
            if model_name != st.session_state.llm_processor.model_name:
                st.session_state.llm_processor = LLMProcessor(model_name=model_name)
                st.success(f"Model changed to {model_name}")
        else:
            st.error("❌ Ollama is not available")
            st.info("Install Ollama and run it with 'ollama serve'")
            st.info("Then pull the model with 'ollama pull deepseek-r1:1.5b'")
        
        st.markdown("---")
        
        if st.button("Scan for New Databases"):
            # Scan for new databases and update config
            scan_for_databases()
            # Reinitialize connections with updated config
            st.session_state.db_connector = DatabaseConnector()
            init_database_connections()
            # Sync metadata for any new databases
            sync_all_database_metadata()
        
        if st.button("Resync Database Metadata"):
            sync_all_database_metadata()
        
        st.markdown("---")
        
        # Background refresh toggle
        auto_refresh = st.checkbox("Enable Background Auto-Update", 
                                  value=st.session_state.auto_refresh_enabled,
                                  help="Automatically check for database schema changes in the background")
        
        if auto_refresh != st.session_state.auto_refresh_enabled:
            if auto_refresh:
                start_background_refresh()
            else:
                stop_background_refresh()
            st.session_state.auto_refresh_enabled = auto_refresh
        
        if st.session_state.last_refresh_time:
            st.write(f"Last refresh: {st.session_state.last_refresh_time}")
        
        st.markdown("---")
        
        if st.button("Show Current Metadata"):
            st.session_state.show_metadata = True
        else:
            st.session_state.show_metadata = False
    
    # Main content area
    if hasattr(st.session_state, 'show_metadata') and st.session_state.show_metadata:
        display_metadata()
    else:
        # Query input
        st.subheader("Enter your query")
        st.write("Example queries:")
        st.write("- Show me all customers")
        st.write("- How many orders do we have?")
        st.write("- Get all products in the Electronics category")
        
        query = st.text_input("What would you like to know?", 
                            placeholder="Example: Show me all customers from New York")
        
        if st.button("Run Query") and query:
            process_user_query(query)

if __name__ == "__main__":
    # On first run, scan for databases and update config
    if not os.path.exists("db_metadata.json"):
        # Scan for databases and update config
        scan_for_databases(show_messages=False)
        # Initialize connections
        init_database_connections()
        # Sync metadata
        sync_all_database_metadata(show_messages=False)
    
    # Start background refresh thread
    start_background_refresh()
    
    main() 