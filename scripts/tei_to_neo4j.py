#!/usr/bin/env python3
"""Simple TEI -> Neo4j loader guided by minimal CIDOC mapping.
- Scans tei/ for *.tei.xml
- For each <div type='record'> it creates nodes for columns that match common types
  (heuristic: column names containing '명칭','피고인','작성자','사건')
- Optionally applies CIDOC mappings stored in Postgres table `tei_cidoc_mappings`.

This script is pragmatic: it uses MERGE to avoid duplicates and provides a
`--wipe` mode to delete Person/Place/Event nodes before loading.
"""
import os
import sys
import argparse
from xml.etree import ElementTree as ET

# Optional DB drivers
try:
    import psycopg2
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
    raise

ROOT = os.getcwd()
TEI_DIR = os.path.join(ROOT, 'tei')

NEO_URI = _secret_or_env('NEO4J_URI', 'bolt://localhost:7687', SECRETS)
NEO_USER = _secret_or_env('NEO4J_USER', 'neo4j', SECRETS)
NEO_PW = _secret_or_env('NEO4J_PASSWORD', 'password', SECRETS)


def safe_run(session, cypher, **params):
    """Defensive wrapper for session.run that rewrites disconnected MATCH
    patterns like "MATCH (a...),(b...)" to "MATCH (a...) MATCH (b...)" to
    avoid accidental cartesian products.
    """
    try:
        if isinstance(cypher, str) and 'MATCH' in cypher and '),(' in cypher:
            new = cypher.replace('),(', ') MATCH (')
            if new != cypher:
                print('⚠️ 안전조치: 분리된 MATCH 패턴을 더 안전한 형태로 변환합니다.')
                cypher = new
        return session.run(cypher, **params)
    except Exception:
        raise

# Postgres settings (for tei_cidoc_mappings)
PGHOST = _secret_or_env('PG_HOST', 'localhost', SECRETS)
PGPORT = int(_secret_or_env('PG_PORT', '5432', SECRETS) or 5432)
PGUSER = _secret_or_env('PG_USER', 'postgres', SECRETS)
PGPASSWORD = _secret_or_env('PG_PASSWORD', '', SECRETS)
PGDATABASE = _secret_or_env('PG_DATABASE', 'postgres', SECRETS)

heuristics = {
    'person': ['명칭','피고인','작성자','저자','인물','성명','이름'],
    'place': ['장소','명칭','지역','주소','세부장소'],
    'event': ['사건','사건명','사건번호']
}


def detect_type(colname):
    for t, kws in heuristics.items():
        for k in kws:
            if k in colname:
                return t
    return None


def apply_cidoc_mappings(session, mappings):
    """Heuristically apply CIDOC TTL mappings stored in DB to Neo4j.
    This does not attempt to parse full RDF; it looks for `ex:...` ids,
    `rdfs:label` strings and CIDOC predicates used by our generator.
    """
    import re
    count = 0
    for table_name, rowid, mapping_label, ttl in mappings:
        if not ttl:
            continue

        # extract all ex: identifiers (flexible)
        ids = re.findall(r'ex:([A-Za-z0-9_\-]+)', ttl)
        # extract labels per resource if present (rdfs:label)
        labels = re.findall(r'ex:([A-Za-z0-9_\-]+)[^\n]*?rdfs:label\s+"([^"]+)"', ttl)
        label_map = {rid: lbl for rid, lbl in labels}

        # Build uids list with guessed type
        uids = []  # list of tuples (kind, uid, short_id)
        for rid in ids:
            short = rid
            kind = None
            if short.lower().startswith('person') or short.lower().startswith('p') and 'person' in ttl.lower():
                kind = 'Person'
            elif short.lower().startswith('place'):
                kind = 'Place'
            elif short.lower().startswith('event') or short.lower().startswith('item'):
                kind = 'Event'
            else:
                # fallback: decide by presence of keywords in ttl
                if 'person' in ttl.lower() or 'ex:person' in ttl.lower():
                    kind = 'Person'
                elif 'place' in ttl.lower() or 'ex:place' in ttl.lower():
                    kind = 'Place'
                elif 'event' in ttl.lower() or 'ex:event' in ttl.lower():
                    kind = 'Event'
                else:
                    # as last resort, infer by prefix
                    if short.lower().startswith('p') and short[1:].isdigit():
                        kind = 'Person'
                    else:
                        kind = 'Event'

            uid = f'ex:{short}'
            uids.append((kind, uid, short))

            lbl = label_map.get(short)
            if kind == 'Person':
                if lbl:
                    safe_run(session, "MERGE (p:Person {uid:$uid}) SET p.name = $label", uid=uid, label=lbl)
                else:
                    safe_run(session, "MERGE (p:Person {uid:$uid})", uid=uid)
            elif kind == 'Place':
                if lbl:
                    safe_run(session, "MERGE (pl:Place {uid:$uid}) SET pl.name = $label", uid=uid, label=lbl)
                else:
                    safe_run(session, "MERGE (pl:Place {uid:$uid})", uid=uid)
            else:
                if lbl:
                    safe_run(session, "MERGE (e:Event {uid:$uid}) SET e.title = $label", uid=uid, label=lbl)
                else:
                    safe_run(session, "MERGE (e:Event {uid:$uid})", uid=uid)

        # create relationships based on known CIDOC predicates or explicit patterns
        # find all predicate uses like 'cidoc:P14_carried_out_by' or plain 'P14_carried_out_by'
        if 'P14_carried_out_by' in ttl:
            ev = next((u for k,u,short in uids if k == 'Event'), None)
            pe = next((u for k,u,short in uids if k == 'Person'), None)
            if ev and pe:
                # avoid cartesian product by using two MATCH clauses
                safe_run(session, "MATCH (a:Event {uid:$ev}) MATCH (b:Person {uid:$pe}) MERGE (a)-[:P14_carried_out_by]->(b)", ev=ev, pe=pe)

        if 'P7_took_place_at' in ttl:
            ev = next((u for k,u,short in uids if k == 'Event'), None)
            pl = next((u for k,u,short in uids if k == 'Place'), None)
            if ev and pl:
                safe_run(session, "MATCH (a:Event {uid:$ev}) MATCH (b:Place {uid:$pl}) MERGE (a)-[:P7_took_place_at]->(b)", ev=ev, pl=pl)

        count += 1
    print(f'Applied {count} CIDOC mappings')


def load_tei_files(wipe=False):
    files = [f for f in os.listdir(TEI_DIR) if f.endswith('.tei.xml')]
    if not files:
        print('No TEI files in', TEI_DIR)
        return 1

    try:
        driver = GraphDatabase.driver(NEO_URI, auth=(NEO_USER, NEO_PW))
    except Exception as e:
        print(f"❌ Neo4j 연결 생성 실패: {e}")
        print("확인: .streamlit/secrets.toml의 NEO4J_URI/NEO4J_USER/NEO4J_PASSWORD 또는 환경변수를 설정했는지 확인하세요.")
        print(f"현재 사용중인 URI: {NEO_URI}")
        return 1

    try:
        with driver.session() as session:
            if wipe:
                print('Wipe mode enabled: deleting existing Person/Place/Event nodes...')
                for label in ('Person', 'Place', 'Event'):
                    try:
                        safe_run(session, f"MATCH (n:{label}) DETACH DELETE n")
                    except Exception as _e:
                        print(f'Warning: failed to delete nodes with label {label}: {_e}')
                print('Wipe complete.')

            for f in files:
                path = os.path.join(TEI_DIR, f)
                print('Loading', f)
                tree = ET.parse(path)
                root = tree.getroot()
                for div in root.findall('.//div'):
                    if div.get('type') != 'record':
                        continue
                    props = {}
                    for p in div.findall('p'):
                        col = p.get('data-col') or 'col'
                        props[col] = (p.text or '').strip()
                    for col, val in props.items():
                        if not val:
                            continue
                        typ = detect_type(col)
                        if typ == 'person':
                            safe_run(session, "MERGE (n:Person {name:$name}) RETURN id(n)", name=val)
                        elif typ == 'place':
                            safe_run(session, "MERGE (n:Place {name:$name}) RETURN id(n)", name=val)
                        elif typ == 'event':
                            safe_run(session, "MERGE (n:Event {title:$title}) RETURN id(n)", title=val)
                    persons = [v for k,v in props.items() if detect_type(k)=='person' and v]
                    events = [v for k,v in props.items() if detect_type(k)=='event' and v]
                    places = [v for k,v in props.items() if detect_type(k)=='place' and v]
                    for p in persons:
                        for e in events:
                            # avoid disconnected-pattern cartesian product
                            safe_run(
                                session,
                                "MATCH (a:Person {name:$p}) MATCH (b:Event {title:$e}) MERGE (b)-[:P14_carried_out_by]->(a)",
                                p=p, e=e)
                    for e in events:
                        for pl in places:
                            safe_run(
                                session,
                                "MATCH (a:Event {title:$e}) MATCH (b:Place {name:$pl}) MERGE (a)-[:P7_took_place_at]->(b)",
                                e=e, pl=pl)

            # apply CIDOC mappings from DB if available
            if psycopg2 is None:
                print('psycopg2 not installed; skipping tei_cidoc_mappings application')
            else:
                try:
                    pg_conn = psycopg2.connect(host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE)
                    pg_cur = pg_conn.cursor()
                    pg_cur.execute("SELECT table_name, rowid, mapping_label, cidoc_ttl FROM tei_cidoc_mappings ORDER BY id")
                    mappings = pg_cur.fetchall()
                    print(f'Applying {len(mappings)} CIDOC mappings from DB...')
                    apply_cidoc_mappings(session, mappings)
                    pg_cur.close()
                    pg_conn.close()
                except Exception as _e:
                    print('Warning: could not fetch/apply tei_cidoc_mappings:', _e)

    except Exception as e:
        print(f"❌ Neo4j 세션 작업 중 오류: {e}")
        print("Neo4j가 실행중인지, 포트(기본 7687)가 열려 있는지 확인하세요.")
        return 1

    print('Done')
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load TEI files into Neo4j (MERGE semantics by default).')
    parser.add_argument('--wipe', action='store_true', help='Delete existing Person/Place/Event nodes before loading')
    args = parser.parse_args()
    sys.exit(load_tei_files(wipe=args.wipe))
