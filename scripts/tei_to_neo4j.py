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
    root = os.getcwd()
    if secret_file is None:
        secret_file = os.path.join(root, '.streamlit', 'secrets.toml')
    if os.path.exists(secret_file):
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
except Exception:
    print('neo4j driver not installed. Install with pip install neo4j')
    sys.exit(1)

# Neo4j settings
NEO_URI = _secret_or_env('NEO4J_URI', 'bolt://localhost:7687', SECRETS)
NEO_USER = _secret_or_env('NEO4J_USER', 'neo4j', SECRETS)
NEO_PW = _secret_or_env('NEO4J_PASSWORD', 'password', SECRETS)

# Postgres settings
PG_HOST = _secret_or_env('PG_HOST', 'localhost', SECRETS)
PG_PORT = int(_secret_or_env('PG_PORT', '5432', SECRETS) or 5432)
PG_USER = _secret_or_env('PG_USER', 'postgres', SECRETS)
PG_PASSWORD = _secret_or_env('PG_PASSWORD', '', SECRETS)
PG_DATABASE = _secret_or_env('PG_DATABASE', 'postgres', SECRETS)

HEURISTICS = {
    'person': ['명칭', '피고인', '작성자', '저자', '인물', '성명', '이름'],
    'place': ['장소', '명칭', '지역', '주소', '세부장소'],
    'event': ['사건', '사건명', '사건번호']
}

# Map table names to description
TABLE_INFO = {
    'raw_event_info': 'Event information',
    'raw_detail_place': 'Detailed place information',
    'raw_event_place_link': 'Event-place links',
}


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
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            database=PG_DATABASE
        )
        return conn
    except Exception as e:
        print(f'❌ PostgreSQL 연결 실패: {e}')
        return None


def fetch_table_data(conn, table_name):
    """Fetch all rows from a table."""
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM {table_name} LIMIT 10000")
            rows = cur.fetchall()
        return rows
    except Exception as e:
        print(f'⚠️ {table_name} 읽기 실패: {e}')
        return []


def safe_run(session, cypher, **params):
    """Defensive wrapper for session.run."""
    try:
        if isinstance(cypher, str) and 'MATCH' in cypher and '),(' in cypher:
            new = cypher.replace('),(', ') MATCH (')
            if new != cypher:
                cypher = new
        return session.run(cypher, **params)
    except Exception as e:
        print(f'⚠️ Cypher실행 실패: {e}')
        print(f'  Cypher: {cypher[:100]}...')
        raise


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
            if wipe:
                print('🗑️ Wipe mode: deleting existing Person/Place/Event nodes...')
                for label in ('Person', 'Place', 'Event'):
                    try:
                        safe_run(session, f"MATCH (n:{label}) DETACH DELETE n")
                    except Exception as _e:
                        pass
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
                if rows:
                    columns = list(rows[0].keys())
                else:
                    continue

                person_cols = [c for c in columns if detect_type(c) == 'person']
                place_cols = [c for c in columns if detect_type(c) == 'place']
                event_cols = [c for c in columns if detect_type(c) == 'event']

                print(f'  Detected columns: person={len(person_cols)}, place={len(place_cols)}, event={len(event_cols)}')

                # Create nodes from rows
                for i, row in enumerate(rows):
                    # Collect entities from this row
                    persons = []
                    places = []
                    events = []

                    for col in person_cols:
                        val = row.get(col)
                        if val and str(val).strip() and str(val).lower() != 'none':
                            persons.append(str(val).strip())

                    for col in place_cols:
                        val = row.get(col)
                        if val and str(val).strip() and str(val).lower() != 'none':
                            places.append(str(val).strip())

                    for col in event_cols:
                        val = row.get(col)
                        if val and str(val).strip() and str(val).lower() != 'none':
                            events.append(str(val).strip())

                    # Create nodes
                    for person in set(persons):
                        try:
                            safe_run(session, "MERGE (n:Person {name:$name}) RETURN id(n)", name=person)
                        except Exception:
                            pass

                    for place in set(places):
                        try:
                            safe_run(session, "MERGE (n:Place {name:$name}) RETURN id(n)", name=place)
                        except Exception:
                            pass

                    for event in set(events):
                        try:
                            safe_run(session, "MERGE (n:Event {title:$title}) RETURN id(n)", title=event)
                        except Exception:
                            pass

                    # Create relationships
                    for person in set(persons):
                        for event in set(events):
                            try:
                                safe_run(
                                    session,
                                    "MATCH (a:Person {name:$p}) MATCH (b:Event {title:$e}) MERGE (b)-[:P14_carried_out_by]->(a)",
                                    p=person, e=event)
                            except Exception:
                                pass

                    for event in set(events):
                        for place in set(places):
                            try:
                                safe_run(
                                    session,
                                    "MATCH (a:Event {title:$e}) MATCH (b:Place {name:$pl}) MERGE (a)-[:P7_took_place_at]->(b)",
                                    e=event, pl=place)
                            except Exception:
                                pass

                    if (i + 1) % 100 == 0:
                        print(f'  ... processed {i + 1} rows')

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
