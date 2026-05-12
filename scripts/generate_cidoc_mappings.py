#!/usr/bin/env python3
"""Generate one-or-more CIDOC-CRM TTL snippets per TEI row and store them in Postgres.
- Reads tables produced by `pg_to_pg_with_tei.py`
- Creates table `tei_cidoc_mappings` with columns:
    id serial, table_name text, rowid int, mapping_label text, cidoc_ttl text, created_at timestamptz
- For each row with non-empty tei, generates N mappings (heuristic) and inserts.

This generator is intentionally simple: it creates small TTL examples referencing `ex:Person`, `ex:Event`, `ex:Place`.
"""
import os, sys, re, datetime
from xml.etree import ElementTree as ET
try:
    import psycopg2
except Exception:
    print('psycopg2 not installed. Install with: pip install psycopg2-binary')
    raise

# TOML 읽기 (Python 3.11+ 또는 tomli)
try:
    import tomllib
except ImportError:
    import tomli as tomllib

ROOT = os.getcwd()
DATA_DIR = os.path.join(ROOT, 'data')

def _load_secrets(secret_file=None):
    if secret_file is None:
        secret_file = os.path.join(ROOT, '.streamlit', 'secrets.toml')
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

PGHOST = _secret_or_env('PG_HOST', 'localhost', SECRETS)
PGPORT = int(_secret_or_env('PG_PORT', '5432', SECRETS))
PGUSER = _secret_or_env('PG_USER', 'postgres', SECRETS)
PGPASSWORD = _secret_or_env('PG_PASSWORD', '', SECRETS)
PGDATABASE = _secret_or_env('PG_DATABASE', 'postgres', SECRETS)


def qname(name):
    name = re.sub(r"\.[^.]+$", '', name)
    name = re.sub(r"[^0-9a-zA-Z]+", '_', name)
    return name.lower()


def connect():
    conn = psycopg2.connect(host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE)
    conn.autocommit = True
    return conn


def ensure_mapping_table(cur):
    cur.execute('''
    CREATE TABLE IF NOT EXISTS tei_cidoc_mappings (
        id serial PRIMARY KEY,
        table_name text,
        rowid integer,
        mapping_label text,
        cidoc_ttl text,
        created_at timestamptz DEFAULT now()
    );
    ''')


def generate_simple_ttls(table, rowid, tei_text):
    # heuristics: parse TEI XML for <persName>, events/actions and person-person relation candidates
    ttls = []
    persons = []
    relations = []
    events = []
    ACTION_KEYWORDS = [
        '체포','구속','처형','투옥','사형','투옥되','사망','고문','시위','참여','선고','구타','처벌','수감','재판','심문','살해','암살','방화','폭행','탄압','만세','시위','구류'
    ]
    PARENT_KEYWORDS = ['아버지','부친','父','父親','부','아비','어머니','모친','母']
    SPOUSE_KEYWORDS = ['부인','아내','남편','부부','妻','夫']
    try:
        root = ET.fromstring(tei_text)
        # build parent map to allow climbing from element to ancestor
        parent_map = {c: p for p in root.iter() for c in p}
        # find all persName elements (namespace-agnostic)
        pers_nodes = root.findall('.//{*}persName') + root.findall('.//persName')
        for idx, pn in enumerate(pers_nodes):
            text = ''.join(pn.itertext()).strip()
            if not text:
                continue
            pid = pn.get('{http://www.w3.org/XML/1998/namespace}id') or pn.get('id') or f'{table}_{rowid}_p{idx+1}'
            hanja = None
            if any('\u4e00' <= ch <= '\u9fff' for ch in text):
                hanja = text
            persons.append({'id': pid, 'text': text, 'hanja': hanja, 'node': pn})

        # For each person, search ancestor text for action keywords to generate person-event-action mappings
        for p in persons:
            # climb up a few ancestors to gather context
            ctx_texts = []
            cur = p['node']
            for _ in range(4):
                parent = parent_map.get(cur)
                if parent is None:
                    break
                ctx = ' '.join(t.strip() for t in parent.itertext() if t and t.strip())
                if ctx:
                    ctx_texts.append(ctx)
                cur = parent
            combined_ctx = '\n'.join(ctx_texts)
            # find action keyword in context
            for kw in ACTION_KEYWORDS:
                if kw in combined_ctx:
                    ev_id = f"{table}_{rowid}_ev_{qname(p['id'])}_{qname(kw)}"
                    events.append({'id': ev_id, 'label': kw, 'actor': p['id']})
                    break

        # Candidate person-person relations: if two persName nodes occur within the same ancestor text and relation keywords present
        # scan parent elements that contain multiple persName children
        for elem in root.iter():
            # collect persName children under this elem
            child_pers = [p for p in persons if p['node'] in list(elem.iter())]
            if len(child_pers) < 2:
                continue
            text_block = ' '.join(t.strip() for t in elem.itertext() if t and t.strip())
            # parent-like relation
            for kw in PARENT_KEYWORDS:
                if kw in text_block:
                    a = child_pers[0]['id']
                    b = child_pers[1]['id']
                    relations.append(('parentOf', a, b, kw))
                    break
            for kw in SPOUSE_KEYWORDS:
                if kw in text_block:
                    a = child_pers[0]['id']
                    b = child_pers[1]['id']
                    relations.append(('spouseOf', a, b, kw))
                    break
    except ET.ParseError:
        # not valid XML — fall back to keyword heuristics below
        pass

    # produce TTLs for discovered persons
    if persons:
        for p in persons:
            ttl = f"""@prefix ex: <http://example.org/docent/> .
@prefix cidoc: <http://www.cidoc-crm.org/cidoc-crm/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:person_{qname(p['id'])} a ex:Person ;
    rdfs:label "{p['text']}" .
"""
            # add hanja as comment/altLabel if present
            if p.get('hanja'):
                ttl += f"\n# hanja: {p['hanja']}\n"
            ttls.append((f'person-{qname(p["id"]) }', ttl))

        # produce event TTLs (person-action-event)
        for ev in events:
            ev_label = ev.get('label')
            actor_id = ev.get('actor')
            ttl = f"""@prefix ex: <http://example.org/docent/> .
@prefix cidoc: <http://www.cidoc-crm.org/cidoc-crm/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:event_{qname(ev['id'])} a ex:Event ;
    rdfs:label "{ev_label} (from {table}-{rowid})" ;
    cidoc:P14_carried_out_by ex:person_{qname(actor_id)} .
"""
            ttls.append((f'event-action-{qname(ev["id"]) }', ttl))

        # add relations as candidate TTL triples (do not enforce direction blindly)
        for rel in relations:
            rel_name, a_id, b_id, kw = rel
            ttl = f"""@prefix ex: <http://example.org/docent/> .
@prefix cidoc: <http://www.cidoc-crm.org/cidoc-crm/> .

ex:person_{qname(a_id)} ex:{rel_name} ex:person_{qname(b_id)} .
# detected_relation_keyword: {kw}
"""
            ttls.append((f'relation-{rel_name}-{qname(a_id)}-{qname(b_id)}', ttl))
        return ttls

    # fallback to previous keyword heuristics when no persName found
    # mapping 1: generic event with actor if '피고인' or '명칭' appears
    if '피고인' in tei_text or '피고' in tei_text or '명칭' in tei_text:
        ttl = f"""@prefix ex: <http://example.org/docent/> .
@prefix cidoc: <http://www.cidoc-crm.org/cidoc-crm/> .

ex:event_{table}_{rowid} a ex:Event ;
    cidoc:P14_carried_out_by ex:person_{table}_{rowid} .

ex:person_{table}_{rowid} a ex:Person ;
    rdfs:label "Actor for {table}-{rowid}" .
"""
        ttls.append(('event-with-actor', ttl))
    # mapping 2: place mapping if '장소' or '세부장소' in TEI
    if '장소' in tei_text or '세부장소' in tei_text or '주소' in tei_text:
        ttl = f"""@prefix ex: <http://example.org/docent/> .
@prefix cidoc: <http://www.cidoc-crm.org/cidoc-crm/> .

ex:place_{table}_{rowid} a ex:Place ;
    rdfs:label "Place for {table}-{rowid}" .
"""
        ttls.append(('place', ttl))
    # fallback: generic mapping
    if not ttls:
        ttl = f"""@prefix ex: <http://example.org/docent/> .
@prefix cidoc: <http://www.cidoc-crm.org/cidoc-crm/> .

ex:item_{table}_{rowid} a ex:Event ;
    rdfs:label "Generic mapping for {table}-{rowid}" .
"""
        ttls.append(('generic', ttl))
    return ttls


def main():
    conn = connect()
    cur = conn.cursor()
    ensure_mapping_table(cur)
    # Find all user tables in the database that have a `tei` column.
    cur.execute("""
    SELECT table_schema, table_name
    FROM information_schema.columns
    WHERE column_name = 'tei'
      AND table_schema NOT IN ('pg_catalog', 'information_schema')
    GROUP BY table_schema, table_name
    ORDER BY table_schema, table_name
    """)
    rows = cur.fetchall()
    if not rows:
        print('No tables with a tei column found in the database.')
    for schema, table_name in rows:
        fq_table = f'"{schema}"."{table_name}"' if schema and schema != 'public' else f'"{table_name}"'
        print('Processing table:', fq_table)
        cur.execute(f'SELECT rowid, tei FROM {fq_table} WHERE tei IS NOT NULL')
        fetched = cur.fetchall()
        if not fetched:
            print('  no rows with tei in', fq_table)
            continue
        for row in fetched:
            rowid, tei = row
            if not tei or not tei.strip():
                continue
            mappings = generate_simple_ttls(table_name, rowid, tei)
            for label, ttl in mappings:
                cur.execute('INSERT INTO tei_cidoc_mappings (table_name, rowid, mapping_label, cidoc_ttl) VALUES (%s,%s,%s,%s)', (table_name, rowid, label, ttl))
                print('Inserted mapping', table_name, rowid, label)
    cur.close()
    conn.close()
    print('Done')

if __name__ == '__main__':
    main()
