#!/usr/bin/env python3
"""Update TEI inside PostgreSQL tables (PG -> PG).
- Uses env vars: PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE
- For each target table:
    - ensure `rowid` exists and is populated for existing rows
    - add `tei` text column if not exists
    - update `tei` for each row with a TEI fragment for that row
- Writes minimal TEI with rule-based TEI tagging.
"""
import os, csv, sys, datetime, re
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape, quoteattr

# TOML 읽기 (Python 3.11+ 또는 tomli)
try:
    import tomllib
except ImportError:
    import tomli as tomllib

try:
    import psycopg2
    from psycopg2.extras import execute_values
except Exception:
    print('psycopg2 not installed. Install with: pip install psycopg2-binary')
    raise

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
TEI_TABLES = os.getenv('TEI_TABLES', '').strip()
TEI_TABLE_PREFIX = os.getenv('TEI_TABLE_PREFIX', 'raw_').strip()

ENCODINGS = ['utf-8-sig', 'cp949', 'utf-16', 'euc-kr']

TAGGABLE_COLS = {
    '사건명', '문서요약', '판결문', '판결문설명', '제목', '비고', '참고노트',
    '관련인물', '피고인', '작성자', '발신자', '수신자', '수신자2',
    '시위_행정구역명', '시위_시작일자', '시위_종료일자',
    '작성일', '발신일', '수신일', '수신일2'
}


def qname(name):
    # normalize filename -> table name
    name = re.sub(r"\.[^.]+$", '', name)
    name = re.sub(r"[^0-9a-zA-Z]+", '_', name)
    return name.lower()


def _read_csv_rows(path):
    for enc in ENCODINGS:
        try:
            with open(path, newline='', encoding=enc, errors='replace') as fh:
                reader = csv.DictReader(fh)
                headers = reader.fieldnames or []
                rows = list(reader)
            return headers, rows
        except Exception:
            continue
    return [], []


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


def get_table_columns(conn, table):
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """, (table,))
    cols = [r[0] for r in cur.fetchall()]
    cur.close()
    return cols


def _normalize_colname(name):
    return re.sub(r"\s+", "", str(name)).lower()


def _find_columns_by_keywords(cols, keywords):
    hits = []
    norm_cols = [(c, _normalize_colname(c)) for c in cols]
    norm_keywords = [_normalize_colname(k) for k in keywords]
    for col, ncol in norm_cols:
        for kw in norm_keywords:
            if kw and kw in ncol:
                hits.append(col)
                break
    return hits


def _select_column_values(conn, table, cols):
    if not cols:
        return []
    cur = conn.cursor()
    cols_sql = ','.join([f'"{c}"' for c in cols])
    cur.execute(f'SELECT {cols_sql} FROM "{table}"')
    rows = cur.fetchall()
    cur.close()
    return rows


def _split_names(value):
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    parts = re.split(r'[;,]', text)
    return [p.strip() for p in parts if p and p.strip()]


def build_entity_catalog(conn):
    entities = []
    tables = set(list_tables(conn))

    # Persons
    if 'raw_event_info' in tables:
        cols = get_table_columns(conn, 'raw_event_info')
        person_cols = _find_columns_by_keywords(cols, ['관련인물', '인물', '피고인'])
        for row in _select_column_values(conn, 'raw_event_info', person_cols):
            for val in row:
                for name in _split_names(val):
                    entities.append({'text': name, 'tag': 'persName', 'attrs': {}})

    if 'raw_bibliography' in tables:
        cols = get_table_columns(conn, 'raw_bibliography')
        person_cols = _find_columns_by_keywords(cols, ['피고인', '관련인물', '인물'])
        for row in _select_column_values(conn, 'raw_bibliography', person_cols):
            for val in row:
                for name in _split_names(val):
                    entities.append({'text': name, 'tag': 'persName', 'attrs': {}})

    # Places
    if 'raw_detail_place' in tables:
        cols = get_table_columns(conn, 'raw_detail_place')
        id_cols = _find_columns_by_keywords(cols, ['세부장소아이디', '장소아이디', 'place_id'])
        name_cols = _find_columns_by_keywords(cols, ['명칭', '장소명', '이름'])
        alias_cols = _find_columns_by_keywords(cols, ['이칭', '별칭'])
        admin_code_cols = _find_columns_by_keywords(cols, ['행정구역코드', '행정코드'])
        admin_name_cols = _find_columns_by_keywords(cols, ['행정구역명', '행정구역'])

        cols_sql = id_cols[:1] + name_cols[:1] + alias_cols[:1] + admin_code_cols[:1] + admin_name_cols[:1]
        for row in _select_column_values(conn, 'raw_detail_place', cols_sql):
            place_id = row[0] if len(row) > 0 else ''
            place_name = row[1] if len(row) > 1 else ''
            alias_val = row[2] if len(row) > 2 else ''
            admin_code = row[3] if len(row) > 3 else ''
            admin_name = row[4] if len(row) > 4 else ''

            pid = str(place_id or '').strip()
            pname = str(place_name or '').strip()
            aliases = _split_names(alias_val)
            acode = str(admin_code or '').strip()
            aname = str(admin_name or '').strip()
            if pid and pname:
                entities.append({'text': pname, 'tag': 'placeName', 'attrs': {'ref': f'#{pid}'}})
            if pid:
                for alias in aliases:
                    entities.append({'text': alias, 'tag': 'placeName', 'attrs': {'ref': f'#{pid}'}})
            if acode and aname:
                entities.append({'text': aname, 'tag': 'placeName', 'attrs': {'ref': f'#{acode}'}})

    # Orgs
    org_tables = {
        'raw_oppression_org_police': 'police',
        'raw_oppression_org_military': 'military',
        'raw_oppression_org_gendarme': 'gendarme'
    }
    for tname, org_type in org_tables.items():
        if tname not in tables:
            continue
        cols = get_table_columns(conn, tname)
        name_cols = _find_columns_by_keywords(cols, ['명칭', '기구명', '기관명', '부서명'])
        if not name_cols and cols:
            name_cols = [cols[1] if len(cols) > 1 else cols[0]]
        for row in _select_column_values(conn, tname, name_cols[:1]):
            org_name = row[0] if row else ''
            name = str(org_name or '').strip()
            if name:
                entities.append({'text': name, 'tag': 'orgName', 'attrs': {'type': org_type}})

    # Events
    if 'raw_event_info' in tables:
        cols = get_table_columns(conn, 'raw_event_info')
        event_cols = _find_columns_by_keywords(cols, ['사건명', '사건'])
        for row in _select_column_values(conn, 'raw_event_info', event_cols[:1]):
            event_name = row[0] if row else ''
            name = str(event_name or '').strip()
            if name:
                entities.append({'text': name, 'tag': 'event', 'attrs': {}})

    # Deduplicate by (tag, text, attrs)
    seen = set()
    unique = []
    for e in entities:
        key = (e['tag'], e['text'], tuple(sorted(e['attrs'].items())))
        if key in seen:
            continue
        seen.add(key)
        unique.append(e)
    unique.sort(key=lambda x: len(x['text']), reverse=True)
    return unique


def _format_attrs(attrs):
    if not attrs:
        return ''
    parts = []
    for k, v in attrs.items():
        parts.append(f' {k}={quoteattr(str(v))}')
    return ''.join(parts)


def _find_spans(text, entities):
    spans = []

    def overlaps(a_start, a_end):
        for s, e, _ in spans:
            if a_start < e and a_end > s:
                return True
        return False

    for ent in entities:
        needle = ent['text']
        if not needle:
            continue
        start = 0
        while True:
            idx = text.find(needle, start)
            if idx == -1:
                break
            end = idx + len(needle)
            if not overlaps(idx, end):
                attrs = _format_attrs(ent['attrs'])
                repl = f"<{ent['tag']}{attrs}>{escape(needle)}</{ent['tag']}>"
                spans.append((idx, end, repl))
            start = end

    # Dates
    date_patterns = [
        re.compile(r'\b(\d{4})[./-](\d{1,2})[./-](\d{1,2})\b'),
        re.compile(r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일')
    ]
    for pat in date_patterns:
        for m in pat.finditer(text):
            idx, end = m.start(), m.end()
            if overlaps(idx, end):
                continue
            y, mo, d = m.group(1), m.group(2), m.group(3)
            when = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
            raw = text[idx:end]
            repl = f"<date when={quoteattr(when)}>{escape(raw)}</date>"
            spans.append((idx, end, repl))

    spans.sort(key=lambda x: x[0])
    return spans


def tag_text(text, entities):
    if text is None:
        return ''
    raw = str(text)
    if not raw:
        return ''
    spans = _find_spans(raw, entities)
    if not spans:
        return escape(raw)

    out = []
    pos = 0
    for start, end, repl in spans:
        if start > pos:
            out.append(escape(raw[pos:start]))
        out.append(repl)
        pos = end
    if pos < len(raw):
        out.append(escape(raw[pos:]))
    return ''.join(out)


def make_tei_for_row(table, rowid, row, headers, entities):
    TEI = ET.Element('TEI')
    teiHeader = ET.SubElement(TEI, 'teiHeader')
    fileDesc = ET.SubElement(teiHeader, 'fileDesc')
    titleStmt = ET.SubElement(fileDesc, 'titleStmt')
    title = ET.SubElement(titleStmt, 'title')
    title.text = f'{table}-{rowid}'
    sourceDesc = ET.SubElement(fileDesc, 'sourceDesc')
    p = ET.SubElement(sourceDesc, 'p')
    # Use timezone-aware UTC timestamp to avoid deprecation warning
    p.text = f'Generated from {table} row {rowid} on {datetime.datetime.now(datetime.timezone.utc).isoformat()}'

    text = ET.SubElement(TEI, 'text')
    body = ET.SubElement(text, 'body')
    div = ET.SubElement(body, 'div', {'type':'record', 'xml:id': f'{table}-{rowid}'})
    for h in headers:
        v = row.get(h, '')
        if v is None:
            v = ''
        p = ET.SubElement(div, 'p')
        p.set('data-col', h)
        if h in TAGGABLE_COLS:
            tagged = tag_text(v, entities)
        else:
            tagged = escape(str(v))
        wrapper = ET.fromstring(f'<wrapper>{tagged}</wrapper>')
        p.text = wrapper.text
        for child in list(wrapper):
            p.append(child)
    return ET.tostring(TEI, encoding='utf-8').decode('utf-8')


def ensure_connection():
    conn = psycopg2.connect(host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE)
    conn.autocommit = True
    return conn


def load_table_to_tei(table, conn, entities):
    print('-> table:', table)
    headers = [c for c in get_table_columns(conn, table) if c not in ('tei', 'rowid')]
    if not headers:
        print('  no columns, skipping', table)
        return
    cur = conn.cursor()

    # ensure rowid
    cur.execute(f"ALTER TABLE \"{table}\" ADD COLUMN IF NOT EXISTS rowid bigint;")
    cur.execute(f"UPDATE \"{table}\" SET rowid = gen.rowid FROM (SELECT ctid, row_number() OVER () AS rowid FROM \"{table}\") gen WHERE \"{table}\".ctid = gen.ctid AND \"{table}\".rowid IS NULL;")

    # add tei column
    cur.execute(f"ALTER TABLE \"{table}\" ADD COLUMN IF NOT EXISTS tei text;")

    # retrieve rowids and full rows to generate TEI
    cur.execute(f'SELECT rowid, {",".join([f"\"{c}\"" for c in headers])} FROM "{table}"')
    allrows = cur.fetchall()
    updates = []
    for rec in allrows:
        rowid = rec[0]
        row = {headers[i]: (rec[i+1] or '') for i in range(len(headers))}
        tei = make_tei_for_row(table, rowid, row, headers, entities)
        updates.append((tei, rowid))
    print('  updating TEI for', len(updates), 'rows')
    execute_values(cur, f'UPDATE "{table}" AS t SET tei = v.tei FROM (VALUES %s) AS v(tei, rowid) WHERE t.rowid = v.rowid', updates)
    cur.close()


def main():
    conn = ensure_connection()
    tables = list_tables(conn)
    if not tables:
        print('No tables found to process')
        conn.close()
        return 1
    entities = build_entity_catalog(conn)
    for t in tables:
        try:
            print('Processing', t)
            load_table_to_tei(t, conn, entities)
        except Exception as e:
            print(' Error processing', t, e)
    conn.close()
    print('Done')
    return 0

if __name__ == '__main__':
    sys.exit(main())
