import os
import tomli
import asyncio
import psycopg2
from psycopg2.extras import RealDictCursor
from neo4j import GraphDatabase
import mcp.server.stdio
from mcp.server import Server
from mcp.types import Tool, TextContent

# Load secrets from Streamlit
secrets_path = os.path.join(os.path.dirname(__file__), ".streamlit", "secrets.toml")
secrets = {}
try:
    with open(secrets_path, "rb") as f:
        secrets = tomli.load(f)
except Exception as e:
    print(f"Failed to load secrets: {e}")

# Connection helpers
def get_pg_connection():
    return psycopg2.connect(
        host=secrets.get("PG_HOST", "localhost"),
        port=int(secrets.get("PG_PORT", 5432)),
        dbname=secrets.get("PG_DATABASE", "postgres"),
        user=secrets.get("PG_USER", "postgres"),
        password=secrets.get("PG_PASSWORD", "")
    )

def get_neo4j_driver():
    return GraphDatabase.driver(
        secrets.get("NEO4J_URI", "bolt://localhost:7687"),
        auth=(secrets.get("NEO4J_USER", "neo4j"), secrets.get("NEO4J_PASSWORD", ""))
    )

# Initialize MCP Server
server = Server("docent-mcp-server")

@server.list_tools()
async def handle_list_tools() -> list[Tool]:
    return [
        Tool(
            name="query_postgres",
            description="Execute a SELECT SQL query against the PostgreSQL database.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The SQL SELECT query to execute."}
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="get_postgres_schema",
            description="Get the list of tables and columns in the PostgreSQL database.",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="query_neo4j",
            description="Execute a Cypher query against the Neo4j database.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The Cypher query to execute."}
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="get_neo4j_schema",
            description="Get the list of node labels and relationship types in the Neo4j database.",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[TextContent]:
    if name == "query_postgres":
        query = arguments.get("query", "")
        if not query.lower().strip().startswith("select") and not query.lower().strip().startswith("with"):
            return [TextContent(type="text", text="Error: Only SELECT/WITH queries are allowed for safety.")]
        try:
            with get_pg_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query)
                    rows = cur.fetchall()
                    return [TextContent(type="text", text=str([dict(r) for r in rows]))]
        except Exception as e:
            return [TextContent(type="text", text=f"PostgreSQL Error: {e}")]

    elif name == "get_postgres_schema":
        query = """
            SELECT table_schema, table_name, column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """
        try:
            with get_pg_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query)
                    rows = cur.fetchall()
                    schema_info = {}
                    for r in rows:
                        tname = f"{r['table_schema']}.{r['table_name']}"
                        if tname not in schema_info:
                            schema_info[tname] = []
                        schema_info[tname].append(f"{r['column_name']} ({r['data_type']})")
                    
                    result_str = "PostgreSQL Schema:\n"
                    for t, cols in schema_info.items():
                        result_str += f"- Table: {t}\n  Columns: {', '.join(cols)}\n"
                    return [TextContent(type="text", text=result_str)]
        except Exception as e:
            return [TextContent(type="text", text=f"PostgreSQL Error: {e}")]

    elif name == "query_neo4j":
        query = arguments.get("query", "")
        try:
            driver = get_neo4j_driver()
            with driver.session() as session:
                result = session.run(query)
                records = [record.data() for record in result]
            driver.close()
            return [TextContent(type="text", text=str(records))]
        except Exception as e:
            return [TextContent(type="text", text=f"Neo4j Error: {e}")]

    elif name == "get_neo4j_schema":
        try:
            driver = get_neo4j_driver()
            with driver.session() as session:
                labels_res = session.run("CALL db.labels()")
                labels = [r[0] for r in labels_res]
                
                rel_res = session.run("CALL db.relationshipTypes()")
                rels = [r[0] for r in rel_res]
                
            driver.close()
            result_str = f"Neo4j Schema:\n- Node Labels: {labels}\n- Relationship Types: {rels}"
            return [TextContent(type="text", text=result_str)]
        except Exception as e:
            return [TextContent(type="text", text=f"Neo4j Error: {e}")]
    else:
        raise ValueError(f"Unknown tool: {name}")

async def main():
    # Run the server using stdin/stdout streams
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main())
