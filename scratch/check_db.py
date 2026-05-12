import os
import sys

# Add parent directory to path to import mcp_server
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from mcp_server import get_pg_connection, get_neo4j_driver
from psycopg2.extras import RealDictCursor
import json

def check_pgsql():
    print("=== PostgreSQL Checks ===")
    try:
        with get_pg_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Find tables with a 'tei' column
                cur.execute("""
                    SELECT table_schema, table_name 
                    FROM information_schema.columns 
                    WHERE column_name = 'tei' 
                    AND table_schema NOT IN ('pg_catalog', 'information_schema')
                """)
                tables = cur.fetchall()
                print(f"Tables with TEI column: {[t['table_name'] for t in tables]}")
                
                # Sample some TEI data
                for table in tables:
                    schema, tname = table['table_schema'], table['table_name']
                    cur.execute(f'SELECT * FROM "{schema}"."{tname}" WHERE tei IS NOT NULL LIMIT 2')
                    rows = cur.fetchall()
                    print(f"\nSample from {tname}:")
                    for row in rows:
                        tei = row.get('tei', '')
                        # Just print a snippet of TEI
                        print(f"Row ID/Name: {row.get('명칭', row.get('id', 'Unknown'))} | TEI Snippet: {tei[:100]}...")
    except Exception as e:
        print(f"PG Error: {e}")

def check_neo4j():
    print("\n=== Neo4j Checks (CIDOC-CRM) ===")
    try:
        driver = get_neo4j_driver()
        with driver.session() as session:
            # Get Labels
            labels = session.run("CALL db.labels()").value()
            print(f"Node Labels: {labels}")
            
            # Get Relationship Types
            rels = session.run("CALL db.relationshipTypes()").value()
            print(f"Relationship Types: {rels}")
            
            # Sample nodes (check for cidoc CRM labels like E21_Person, E53_Place, etc. or Korean equivalents)
            for label in ['인물', '사건', '장소', '기관', '문건', 'E21_Person', 'E53_Place', 'E5_Event', 'E74_Group']:
                if label in labels:
                    res = session.run(f"MATCH (n:`{label}`) RETURN n LIMIT 1")
                    record = res.single()
                    if record:
                        node = record['n']
                        print(f"\nSample Node for label '{label}':")
                        print(dict(node))
            
            # Count nodes and relationships
            node_count = session.run("MATCH (n) RETURN count(n) AS c").single()['c']
            rel_count = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()['c']
            print(f"\nTotal Nodes: {node_count}, Total Relationships: {rel_count}")
            
        driver.close()
    except Exception as e:
        print(f"Neo4j Error: {e}")

if __name__ == "__main__":
    check_pgsql()
    check_neo4j()
