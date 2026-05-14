#!/usr/bin/env python3
"""Generate one-or-more CIDOC-CRM TTL snippets per TEI row and store them in Postgres.
- Reads tables produced by `pg_to_pg_with_tei.py`
- Creates table `tei_cidoc_mappings` with columns:
    id serial, table_name text, rowid int, mapping_label text, cidoc_ttl text, created_at timestamptz
- For each row with non-empty tei, generates N mappings (heuristic) and inserts.

This generator is intentionally simple: it creates small TTL examples referencing `ex:Person`, `ex:Event`, `ex:Place`.
"""
import os, sys, re, datetime, argparse, logging
from xml.etree import ElementTree as ET
from bs4 import BeautifulSoup
import html

try:
    import psycopg2
except Exception:
    raise RuntimeError('psycopg2 not installed. Install with: pip install psycopg2-binary')

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


def safe_fragment(name: str) -> str:
    """Create a URI-safe fragment from a name: keep letters/digits and CJK, replace others with underscore, collapse."""
    if not name:
        return ''
    # unescape HTML entities
    name = html.unescape(str(name))
    # normalize whitespace
    name = re.sub(r"\s+", '_', name.strip())
    # allow ASCII letters/digits and CJK Unified Ideographs, replace other chars with '_'
    name = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", '_', name)
    # collapse multiple underscores
    name = re.sub(r"_+", '_', name)
    # strip leading/trailing underscores
    return name.strip('_') or 'n'


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
        # Use BeautifulSoup for more robust XML handling (and easy extract())
        soup = BeautifulSoup(tei_text or '', 'xml')
        # find persName elements
        pers_nodes = soup.find_all('persName')
        for idx, pn in enumerate(pers_nodes):
            # extract gloss if present
            gloss_tag = pn.find('gloss')
            gloss_text = ''
            if gloss_tag:
                try:
                    gloss_text = gloss_tag.get_text(' ', strip=True) or ''
                except Exception:
                    gloss_text = ''
                try:
                    gloss_tag.extract()
                except Exception:
                    pass

            text = pn.get_text(' ', strip=True)
            if not text:
                continue
            pid = pn.get('id') or pn.get('{http://www.w3.org/XML/1998/namespace}id') or f'{table}_{rowid}_p{idx+1}'
            hanja = None
            if any('\u4e00' <= ch <= '\u9fff' for ch in text):
                hanja = text
            persons.append({'id': pid, 'text': text, 'hanja': hanja, 'node': pn, 'gloss': gloss_text})

        # For each person, search ancestor text (using parent elements) for action keywords
        for p in persons:
            ctx_texts = []
            cur = p['node']
            for _ in range(4):
                if cur is None or cur.parent is None:
                    break
                parent = cur.parent
                if not parent:
                    break
                ctx = ' '.join(t.strip() for t in parent.strings if t and t.strip())
                if ctx:
                    ctx_texts.append(ctx)
                cur = parent
            combined_ctx = '\n'.join(ctx_texts)
            for kw in ACTION_KEYWORDS:
                if kw in combined_ctx:
                    ev_id = f"{table}_{rowid}_ev_{qname(p['id'])}_{qname(kw)}"
                    events.append({'id': ev_id, 'label': kw, 'actor': p['id']})
                    break

        # Candidate person-person relations: find parent tags that contain multiple persName children
        for elem in soup.find_all():
            child_pers = [p for p in persons if p['node'] in elem.find_all()]
            if len(child_pers) < 2:
                continue
            text_block = ' '.join(t.strip() for t in elem.strings if t and t.strip())
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
    except Exception:
        # parsing fallback handled below
        pass

    # produce TTLs for discovered persons
    if persons:
        for p in persons:
            # choose fragment base: prefer hanja/main text cleaned, fallback to pid qname
            base_name = p.get('hanja') or p.get('text') or p.get('id')
            frag = safe_fragment(base_name) or qname(p.get('id'))
            ttl = f"""@prefix ex: <http://example.org/docent/> .
@prefix cidoc: <http://www.cidoc-crm.org/cidoc-crm/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .

ex:person_{frag} a ex:Person ;
    rdfs:label "{p['text']}" .
"""
            # add gloss as skos:altLabel if present
            gloss = p.get('gloss')
            if gloss:
                ttl = ttl.rstrip() + f"\nex:person_{frag} skos:altLabel \"{gloss}\" .\n"
            # add hanja comment
            if p.get('hanja'):
                ttl += f"\n# hanja: {p['hanja']}\n"
            ttls.append((f'person-{frag}', ttl))

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
    parser = argparse.ArgumentParser(description='Generate CIDOC-CRM TTL snippets from TEI stored in DB')
    parser.add_argument('--dry-run', action='store_true', help='Do not insert into DB; print sample mappings')
    parser.add_argument('--table', help='Only process this table name (unqualified)')
    parser.add_argument('--limit', type=int, default=0, help='Limit number of rows processed per table (0 = no limit)')
    parser.add_argument('--log-level', default='INFO', help='Logging level (DEBUG, INFO, WARNING)')
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format='%(levelname)s: %(message)s')

    conn = connect()
    cur = conn.cursor()

    # Find tables with tei column
    cur.execute("""
        SELECT table_schema, table_name
        FROM information_schema.columns
        WHERE column_name IN ('tei', 'tei_status')
            AND table_schema NOT IN ('pg_catalog', 'information_schema')
        GROUP BY table_schema, table_name
        HAVING COUNT(DISTINCT column_name) = 2
        ORDER BY table_schema, table_name
        """)
    rows = cur.fetchall()
    if not rows:
        logging.info('No tables with a tei column found in the database.')
        cur.close()
        conn.close()
        return

    # create mapping table only if we're going to insert
    if not args.dry_run:
        ensure_mapping_table(cur)

    for schema, table_name in rows:
        if args.table and args.table != table_name:
            continue
        fq_table = f'"{schema}"."{table_name}"' if schema and schema != 'public' else f'"{table_name}"'
        logging.info('Processing table: %s', fq_table)
        sql = f"SELECT rowid, tei FROM {fq_table} WHERE tei IS NOT NULL AND tei_status = 'REFINED'"
        # Ensure only refined TEI rows are processed
        if args.limit and args.limit > 0:
            sql += f' LIMIT {args.limit}'
        cur.execute(sql)
        fetched = cur.fetchall()
        if not fetched:
            logging.info('  no rows with tei in %s', fq_table)
            continue
        for row in fetched:
            rowid, tei = row
            if not tei or not tei.strip():
                continue
            mappings = generate_simple_ttls(table_name, rowid, tei)
            for label, ttl in mappings:
                if args.dry_run:
                    print('DRY:', table_name, rowid, label)
                    print(ttl)
                else:
                    cur.execute('INSERT INTO tei_cidoc_mappings (table_name, rowid, mapping_label, cidoc_ttl) VALUES (%s,%s,%s,%s)', (table_name, rowid, label, ttl))
                    logging.debug('Inserted mapping %s %s %s', table_name, rowid, label)

    cur.close()
    conn.close()
    logging.info('Done')

if __name__ == '__main__':
    main()
