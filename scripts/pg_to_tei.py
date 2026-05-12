#!/usr/bin/env python3
"""Export TEI XML files from PostgreSQL tables (PG -> TEI files).
- One TEI file per table named <table>.tei.xml
- Each row's stored `tei` content is parsed and its <div type="record"> is appended
This is a pragmatic export step for ontology mapping.
"""
import os, sys, datetime, copy
from xml.etree import ElementTree as ET

# TOML 읽기 (Python 3.11+ 또는 tomli)
try:
    import tomllib
except ImportError:
    import tomli as tomllib

try:
    import psycopg2
except Exception:
    print('psycopg2 not installed. Install with: pip install psycopg2-binary')
    raise

ROOT = os.getcwd()
OUT_DIR = os.path.join(ROOT, 'tei')
os.makedirs(OUT_DIR, exist_ok=True)

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
TEI_TABLES = os.getenv('TEI_TABLES', '').strip()
TEI_TABLE_PREFIX = os.getenv('TEI_TABLE_PREFIX', 'raw_').strip()


def connect():
    conn = psycopg2.connect(host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE)
    conn.autocommit = True
    return conn


def list_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    tables = [r[0] for r in cur.fetchall()]
    cur.close()
    if TEI_TABLES:
        wanted = [t.strip() for t in TEI_TABLES.split(',') if t.strip()]
        return [t for t in tables if t in wanted]
    if TEI_TABLE_PREFIX:
        return [t for t in tables if t.startswith(TEI_TABLE_PREFIX)]
    return tables


def _extract_record_divs(tei_text):
    try:
        root = ET.fromstring(tei_text)
    except Exception:
        return []
    divs = []
    for div in root.findall('.//div'):
        if div.get('type') == 'record':
            divs.append(div)
    return divs


def make_tei_doc(table, record_divs):
    TEI = ET.Element('TEI')
    teiHeader = ET.SubElement(TEI, 'teiHeader')
    fileDesc = ET.SubElement(teiHeader, 'fileDesc')
    titleStmt = ET.SubElement(fileDesc, 'titleStmt')
    title = ET.SubElement(titleStmt, 'title')
    title.text = table
    publicationStmt = ET.SubElement(fileDesc, 'publicationStmt')
    publisher = ET.SubElement(publicationStmt, 'publisher')
    publisher.text = 'docent-export'
    sourceDesc = ET.SubElement(fileDesc, 'sourceDesc')
    p = ET.SubElement(sourceDesc, 'p')
    p.text = f'Exported from {table} on {datetime.datetime.utcnow().isoformat()}Z'

    text = ET.SubElement(TEI, 'text')
    body = ET.SubElement(text, 'body')
    for div in record_divs:
        body.append(copy.deepcopy(div))
    return ET.ElementTree(TEI)


def export_all():
    conn = connect()
    tables = list_tables(conn)
    if not tables:
        print('No tables found to process')
        conn.close()
        return 1
    cur = conn.cursor()
    for table in tables:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s AND column_name = 'tei'
        """, (table,))
        if not cur.fetchone():
            print('Skipping table without tei column:', table)
            continue
        print('Processing', table)
        cur.execute(f'SELECT tei FROM "{table}" WHERE tei IS NOT NULL')
        record_divs = []
        for (tei_text,) in cur.fetchall():
            if not tei_text:
                continue
            record_divs.extend(_extract_record_divs(tei_text))
        if not record_divs:
            print('  no record divs found; skipping', table)
            continue
        tree = make_tei_doc(table, record_divs)
        out_path = os.path.join(OUT_DIR, f'{table}.tei.xml')
        tree.write(out_path, encoding='utf-8', xml_declaration=True)
        print(' -> wrote', out_path)
    cur.close()
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(export_all())
