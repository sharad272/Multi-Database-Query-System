# Multi-Database Query System

A Streamlit application that allows users to query multiple databases using natural language. The system intelligently routes queries to the appropriate database based on schema metadata and semantic understanding.

## Features

- **Natural Language Queries**: Ask questions in plain English to query your databases
- **Multi-Database Support**: Works with SQLite, MySQL, and PostgreSQL databases
- **Intelligent Query Routing**: Uses both semantic search and keyword matching to direct queries to the right database
- **Automatic Schema Discovery**: Extracts schema information from connected databases
- **Background Auto-Updates**: Automatically monitors databases and updates metadata when schemas change
- **AI-Powered Insights**: Generates SQL queries from natural language and provides summaries of query results
- **Vector Search**: Semantic matching of queries to relevant databases and tables using embeddings
- **Performance Optimized**: Lazy loading of machine learning models for faster startup

## How it Works

1. The system maintains a metadata store of all connected databases, their tables, and columns
2. When a user submits a natural language query, the system:
   - Uses vector search to find semantically relevant databases and tables
   - Generates SQL code using a local LLM (DeepSeek-r1)
   - Executes the query against the appropriate database
   - Summarizes the results with AI-generated insights
3. A background process continuously monitors for schema changes and new databases

## Semantic Search

The system uses vector embeddings to match natural language queries to the most relevant database and table:

- **Sentence Transformers**: Generates embeddings of table/column descriptions using the all-MiniLM-L6-v2 model
- **FAISS Vector Index**: Provides fast similarity search for efficiently matching queries to database objects
- **Lazy Loading**: The embedding model is loaded only when needed to improve application startup time
- **Multiple Database Routing**: Identifies the most relevant database and table across all connected data sources

## Setup

1. Install the required dependencies:

```bash
pip install -r requirements.txt
```

2. Install Ollama for local LLM support:
   - Download from https://ollama.ai/
   - Run `ollama serve` to start the service
   - Pull the DeepSeek model with `ollama pull deepseek-r1:1.5b`

3. Configure your databases in `db_config.json`:

```json
[
  {
    "name": "sales_db",
    "type": "sqlite",
    "path": "databases/sales.db"
  },
  {
    "name": "customers_db",
    "type": "mysql",
    "host": "localhost",
    "user": "username",
    "password": "password",
    "database": "customers",
    "port": 3306
  }
]
```

4. Run the application:

```bash
streamlit run app.py
```

## Usage

1. Enter a natural language query in the input field (e.g., "Show me all customers from New York")
2. The system will:
   - Automatically determine the appropriate database and table
   - Generate and execute SQL
   - Display the results
   - Provide AI-generated insights about the data
3. Use the sidebar controls to:
   - Change the LLM model
   - Scan for new databases
   - Manually resync database metadata
   - Toggle background auto-updates
   - View current metadata

## Components

- `app.py`: Main Streamlit application
- `metadata_manager.py`: Manages database metadata and query routing
- `db_connector.py`: Handles database connections and queries
- `llm_processor.py`: Processes natural language with LLM for SQL generation and insights
- `vector_search.py`: Provides semantic search capabilities using embeddings
- `db_config.json`: Configuration for database connections
- `db_metadata.json`: Automatically generated metadata file
- `db_embeddings.json`: Stored embeddings for vector search 