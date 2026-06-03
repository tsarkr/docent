#!/usr/bin/env python3
"""Load raw_* tables from PostgreSQL → Neo4j graph.
Instead of parsing XML files (which don't exist or are stale),
this reads directly from the PostgreSQL raw_* tables populated by upload_data.py.

Flow:
  1. Read from PostgreSQL raw_event_info, raw_detail_place, etc.
  2. Extract person/place/event entities based on heuristics on column names
  3. Create nodes in Neo4j
  4. Create relationships (person carried_out_by event, event took_place_at place)
"""
import os
import sys
import argparse
import json
from pathlib import Path

# Optional DB drivers
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except Exception:
    psycopg2 = None

try:
    import tomllib
except ImportError:
    import tomli as tomllib


def _load_secrets(secret_file=None):
    if secret_file is None:
        # 1. Try current working directory
        path1 = os.path.join(os.getcwd(), '.streamlit', 'secrets.toml')
        # 2. Try relative to this script
        # Assuming this script is in 'scripts/' and secrets.toml is in '.streamlit/' in the root
        path2 = Path(__file__).resolve().parent.parent / '.streamlit' / 'secrets.toml'
        
        if os.path.exists(path1):
            secret_file = path1
        elif path2.exists():
            secret_file = str(path2)

    if secret_file and os.path.exists(secret_file):
        try:
            with open(secret_file, 'rb') as f:
                return tomllib.load(f)
        except Exception:
            return {}
    return {}


def _secret_or_env(key, default='', secrets_dict=None):
    val = os.getenv(key)
    if val:
        return str(val)
    if secrets_dict and key in secrets_dict:
        return str(secrets_dict[key])
    return str(default) if default else ''


SECRETS = _load_secrets()

try:
    from neo4j import GraphDatabase
    from neo4j.exceptions import ClientError
except Exception:
    print('neo4j driver not installed. Install with pip install neo4j')
    sys.exit(1)

# Neo4j settings
NEO_URI = _secret_or_env('NEO4J_URI', 'bolt://localhost:7687', SECRETS)
NEO_USER = _secret_or_env('NEO4J_USER', 'neo4j', SECRETS)
NEO_PW = _secret_or_env('NEO4J_PASSWORD', 'password', SECRETS)

# Postgres settings
PG_HOST = _secret_or_env('PG_HOST', '127.0.0.1', SECRETS)
PG_PORT = int(_secret_or_env('PG_PORT', '5432', SECRETS) or 5432)
PG_USER = _secret_or_env('PG_USER', 'postgres', SECRETS)
PG_PASSWORD = _secret_or_env('PG_PASSWORD', '', SECRETS)
PG_DATABASE = _secret_or_env('PG_DATABASE', 'postgres', SECRETS)

HEURISTICS = {
    'person': ['명칭', '피고인', '작성자', '발신자', '수신자', '수신자2', '저자', '인물', '성명', '이름'],
    'place': ['장소', '명칭', '지역', '주소', '세부장소'],
    'event': ['사건', '사건명', '사건번호']
}

# Map table names to description
TABLE_INFO = {
    'raw_event_info': 'Event information',
    'raw_detail_place': 'Detailed place information',
    'raw_event_place_link': 'Event-place links',
}


def _env_int(name, default):
    try:
        return int(os.getenv(name, str(default)) or default)
    except Exception:
        return int(default)


PG_FETCH_LIMIT = _env_int('TEI_NEO4J_FETCH_LIMIT', 10000)
NEO4J_BATCH_SIZE = _env_int('TEI_NEO4J_BATCH_SIZE', 50000)
NEO4J_DELETE_BATCH = _env_int('TEI_NEO4J_DELETE_BATCH', 20000)


def detect_type(colname):
    """Detect entity type (person/place/event) from column name."""
    if colname is None:
        return None
    colname = str(colname).lower()
    for t, kws in HEURISTICS.items():
        for k in kws:
            if k.lower() in colname:
                return t
    return None


def load_from_postgres():
    """Connect to PostgreSQL and fetch raw data."""
    if psycopg2 is None:
        print('❌ psycopg2 not installed. Install with: pip install psycopg2-binary')
        return None

    try:
        display_host = PG_HOST if PG_HOST in ['localhost', '127.0.0.1'] else f"{PG_HOST[:3]}***"
        print(f"🔌 PostgreSQL 연결 시도 중... (host={display_host})")
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            database=PG_DATABASE,
            connect_timeout=10
        )
        return conn
    except Exception as e:
        print(f'❌ PostgreSQL 연결 실패: {e}')
        return None


def fetch_table_data(conn, table_name):
    """Fetch all rows from a table."""
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM {table_name} LIMIT {PG_FETCH_LIMIT}")
            rows = cur.fetchall()
        return rows
    except Exception as e:
        print(f'⚠️ {table_name} 읽기 실패: {e}')
        return []


def safe_run(session, cypher, **params):
    """Defensive wrapper for session.run."""
    try:
        return session.run(cypher, **params)
    except Exception as e:
        print(f'⚠️ Cypher실행 실패: {e}')
        # print(f'  Cypher: {cypher[:200]}...')
        raise


def _ensure_schema(session):
    """Create constraints for faster MERGE."""
    import re
    constraints = [
        "CREATE CONSTRAINT tei_person_name IF NOT EXISTS FOR (n:Person) REQUIRE n.name IS UNIQUE",
        "CREATE CONSTRAINT tei_place_name IF NOT EXISTS FOR (n:Place) REQUIRE n.name IS UNIQUE",
        "CREATE CONSTRAINT tei_event_title IF NOT EXISTS FOR (n:Event) REQUIRE n.title IS UNIQUE",
    ]
    for c in constraints:
        try:
            session.run(c)
        except ClientError as e:
            if "AlreadyExists" in e.code:
                match = re.search(r'(?:CONSTRAINT|INDEX)\s+([^\s]+)', c, re.IGNORECASE)
                if match:
                    name = match.group(1)
                    print(f"⚠️ Schema conflict ('{name}'): dropping and recreating.")
                    try:
                        if "CONSTRAINT" in c.upper():
                            session.run(f"DROP CONSTRAINT {name} IF EXISTS")
                        else:
                            session.run(f"DROP INDEX {name} IF EXISTS")
                        session.run(c)
                    except Exception as retry_err:
                        print(f"❌ Retry failed: {retry_err}")
                else:
                    print(f"⚠️ Constraint creation failed: {e}")
            else:
                print(f"⚠️ Constraint creation failed: {e}")
        except Exception as e:
            print(f"⚠️ Constraint creation failed: {e}")


def _run_batches(session, cypher, records, batch_size=20000, extra_params=None):
    extra_params = extra_params or {}
    total = len(records)
    if total == 0:
        return
    batch_size = max(1, int(batch_size or 1))
    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        params = {**extra_params, "batch": batch}
        safe_run(session, cypher, **params)


def load_from_postgres_to_neo4j(wipe=False):
    """Main flow: read PostgreSQL raw_* tables → create Neo4j nodes."""
    conn = load_from_postgres()
    if conn is None:
        return 1

    try:
        driver = GraphDatabase.driver(NEO_URI, auth=(NEO_USER, NEO_PW))
    except Exception as e:
        print(f"❌ Neo4j 연결 실패: {e}")
        print(f"확인: .streamlit/secrets.toml의 NEO4J_URI/NEO4J_USER/NEO4J_PASSWORD 또는 환경변수를 설정하세요.")
        print(f"현재 URI: {NEO_URI}")
        return 1

    try:
        with driver.session() as session:
            _ensure_schema(session)
            
            if wipe:
                print('🗑️ Wipe mode: deleting existing Person/Place/Event nodes...')
                # Use batched delete for safety
                for label in ('Person', 'Place', 'Event'):
                    while True:
                        res = session.run(f"MATCH (n:{label}) WITH n LIMIT {NEO4J_DELETE_BATCH} DETACH DELETE n RETURN count(*) as c")
                        if res.single()['c'] == 0:
                            break
                print('✅ Wipe complete.')

            # Process each raw_* table
            for table_name, desc in TABLE_INFO.items():
                print(f'\n📖 Processing {table_name} ({desc})...')
                rows = fetch_table_data(conn, table_name)
                if not rows:
                    print(f'  (no data or table does not exist)')
                    continue

                print(f'  Loaded {len(rows)} rows')
                # Get column names from first row
                columns = list(rows[0].keys())

                person_cols = [c for c in columns if detect_type(c) == 'person']
                place_cols = [c for c in columns if detect_type(c) == 'place']
                event_cols = [c for c in columns if detect_type(c) == 'event']

                print(f'  Detected columns: person={len(person_cols)}, place={len(place_cols)}, event={len(event_cols)}')

                all_persons = set()
                all_places = set()
                all_events = set()
                rel_p_e = set() # (person, event)
                rel_e_p = set() # (event, place)

                # Collect all entities and relationships in memory first
                for row in rows:
                    row_persons = []
                    row_places = []
                    row_events = []

                    for col in person_cols:
                        val = row.get(col)
                        if val and str(val).strip() and str(val).lower() != 'none':
                            row_persons.append(str(val).strip())

                    for col in place_cols:
                        val = row.get(col)
                        if val and str(val).strip() and str(val).lower() != 'none':
                            row_places.append(str(val).strip())

                    for col in event_cols:
                        val = row.get(col)
                        if val and str(val).strip() and str(val).lower() != 'none':
                            row_events.append(str(val).strip())

                    # Track unique entities
                    for p in row_persons: all_persons.add(p)
                    for pl in row_places: all_places.add(pl)
                    for e in row_events: all_events.add(e)

                    # Track relationships
                    for p in set(row_persons):
                        for e in set(row_events):
                            rel_p_e.add((p, e))
                    for e in set(row_events):
                        for pl in set(row_places):
                            rel_e_p.add((e, pl))

                # Batch Create Nodes
                if all_persons:
                    print(f"  → Creating {len(all_persons)} Person nodes...")
                    _run_batches(session, "UNWIND $batch as name MERGE (n:Person {name:name})", list(all_persons), batch_size=NEO4J_BATCH_SIZE)
                if all_places:
                    print(f"  → Creating {len(all_places)} Place nodes...")
                    _run_batches(session, "UNWIND $batch as name MERGE (n:Place {name:name})", list(all_places), batch_size=NEO4J_BATCH_SIZE)
                if all_events:
                    print(f"  → Creating {len(all_events)} Event nodes...")
                    _run_batches(session, "UNWIND $batch as title MERGE (n:Event {title:title})", list(all_events), batch_size=NEO4J_BATCH_SIZE)

                # Batch Create Relationships
                if rel_p_e:
                    print(f"  → Creating {len(rel_p_e)} P14 relationships...")
                    recs = [{"p": p, "e": e} for p, e in rel_p_e]
                    _run_batches(session, """
                        UNWIND $batch as row
                        MATCH (a:Person {name:row.p})
                        MATCH (b:Event {title:row.e})
                        MERGE (b)-[:P14_carried_out_by]->(a)
                    """, recs, batch_size=NEO4J_BATCH_SIZE)
                
                if rel_e_p:
                    print(f"  → Creating {len(rel_e_p)} P7 relationships...")
                    recs = [{"e": e, "pl": pl} for e, pl in rel_e_p]
                    _run_batches(session, """
                        UNWIND $batch as row
                        MATCH (a:Event {title:row.e})
                        MATCH (b:Place {name:row.pl})
                        MERGE (a)-[:P7_took_place_at]->(b)
                    """, recs, batch_size=NEO4J_BATCH_SIZE)

                print(f'  ✅ {table_name} complete')

        print('\n✨ Done! All raw_* tables loaded to Neo4j.')
        return 0

    except Exception as e:
        print(f'❌ Error: {e}')
        return 1
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load PostgreSQL raw_* tables into Neo4j')
    parser.add_argument('--wipe', action='store_true', help='Wipe existing Person/Place/Event nodes before loading')
    args = parser.parse_args()

    exit_code = load_from_postgres_to_neo4j(wipe=args.wipe)
    sys.exit(exit_code)
