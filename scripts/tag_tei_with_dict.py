"""Tag TEI content using dictionary matches from PostgreSQL.

Usage:
    python scripts/tag_tei_with_dict.py --table raw_source_info --limit 5
    python scripts/tag_tei_with_dict.py --all --limit 0

The script reads from PostgreSQL and updates the `tei` column in-place. It builds a
name dictionary from reference tables, tags matching names and dates inside the TEI,
adds hanja glosses, and updates tei_status='REFINED'.
"""

import argparse
import os
import re
import sys
import subprocess
from pathlib import Path

# Attempt to import transformers NER pipeline once; fall back gracefully if unavailable
try:
    from transformers import pipeline
    try:
        # Load grouped entities so we get `entity_group` like 'PER'
        ner_pipeline = pipeline("ner", model="kykim/bert-kor-base", grouped_entities=True)
        NER_AVAILABLE = True
    except Exception as e:
        print(f'⚠️ NER 모델 로드 실패: {e} — 사전 기반 태깅만 사용합니다.')
        ner_pipeline = None
        NER_AVAILABLE = False
except Exception:
    ner_pipeline = None
    NER_AVAILABLE = False
try:
    import tomllib
except Exception:
    import tomli as tomllib

try:
    import psycopg2
except Exception:
    psycopg2 = None


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

PGHOST = _secret_or_env('PG_HOST', 'localhost', SECRETS)
PGPORT = int(_secret_or_env('PG_PORT', '5432', SECRETS) or 5432)
PGUSER = _secret_or_env('PG_USER', 'postgres', SECRETS)
PGPASSWORD = _secret_or_env('PG_PASSWORD', '', SECRETS)
PGDATABASE = _secret_or_env('PG_DATABASE', 'postgres', SECRETS)

try:
    import hanja
except Exception:
    hanja = None


# best-effort per-character fallback map for hanja -> hangul readings
HANJA_TO_HANGUL = {
    '柳': '유', '寬': '관', '順': '순',
    '明': '명', '治': '치', '光': '광', '州': '주', '海': '해', '子': '자', '浦': '포',
    '德': '덕', '沼': '소', '里': '리', '憲': '헌', '兵': '병', '駐': '주', '在': '재', '所': '소',
}


def _hanja_char_by_char(token: str) -> str:
    if not token:
        return ''
    readings = []
    mapped_count = 0
    for ch in token:
        if ch in HANJA_TO_HANGUL:
            readings.append(HANJA_TO_HANGUL[ch])
            mapped_count += 1
        else:
            # keep ASCII as-is, otherwise skip unknown CJK
            if ord(ch) < 128:
                readings.append(ch)
            else:
                readings.append('')
    if mapped_count == 0:
        return ''
    return ''.join(readings)


def _connect():
    if not psycopg2:
        print('psycopg2 not available; cannot connect to DB')
        sys.exit(1)
    conn = psycopg2.connect(host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE)
    conn.autocommit = True
    return conn


def _get_columns(cur, table_name):
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table_name,),
    )
    return [r[0] for r in cur.fetchall()]


def _list_tei_tables(cur):
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND column_name = 'tei'
        ORDER BY table_name
        """
    )
    return [r[0] for r in cur.fetchall()]


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


def _split_names(value):
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    parts = re.split(r'[;,/]', text)
    return [p.strip() for p in parts if p and p.strip()]


def _hanja_translate(token: str) -> str:
    if not token:
        return ''
    if hanja is None:
        return ''
    try:
        return hanja.translate(token, 'substitution')
    except Exception:
        return ''


def _add_entry(dictionary, name, tag, priority):
    if not name:
        return
    name = str(name).strip()
    if not name:
        return
    existing = dictionary.get(name)
    if not existing:
        dictionary[name] = tag
        return
    if priority.get(tag, 0) > priority.get(existing, 0):
        dictionary[name] = tag


def build_dictionary(conn):
    cur = conn.cursor()
    dictionary = {}
    priority = {'persName': 3, 'placeName': 2, 'orgName': 1, 'term': 0}

    # raw_event_info: person and event names
    try:
        cols = _get_columns(cur, 'raw_event_info')
        person_cols = _find_columns_by_keywords(cols, ['관련인물', '인물', '피고인'])
        event_cols = _find_columns_by_keywords(cols, ['사건명', '사건'])
        for col in person_cols + event_cols:
            cur.execute(f'SELECT "{col}" FROM raw_event_info')
            for (val,) in cur.fetchall():
                for name in _split_names(val):
                    tag = 'persName' if col in person_cols else 'term'
                    _add_entry(dictionary, name, tag, priority)
    except Exception as e:
        print(f'⚠️ raw_event_info 로딩 실패: {e}')

    # raw_detail_place: place names and aliases
    try:
        cols = _get_columns(cur, 'raw_detail_place')
        name_cols = _find_columns_by_keywords(cols, ['명칭', '장소명', '이름'])
        alias_cols = _find_columns_by_keywords(cols, ['이칭', '별칭'])
        for col in name_cols + alias_cols:
            cur.execute(f'SELECT "{col}" FROM raw_detail_place')
            for (val,) in cur.fetchall():
                for name in _split_names(val):
                    _add_entry(dictionary, name, 'placeName', priority)
    except Exception as e:
        print(f'⚠️ raw_detail_place 로딩 실패: {e}')

    # org tables
    org_tables = ['raw_oppression_org_police', 'raw_oppression_org_military', 'raw_oppression_org_gendarme']
    for table in org_tables:
        try:
            cols = _get_columns(cur, table)
            name_cols = _find_columns_by_keywords(cols, ['명칭', '기구명', '기관명', '부서명'])
            if not name_cols and cols:
                name_cols = [cols[0]]
            for col in name_cols[:1]:
                cur.execute(f'SELECT "{col}" FROM {table}')
                for (val,) in cur.fetchall():
                    name = str(val).strip() if val is not None else ''
                    if name:
                        _add_entry(dictionary, name, 'orgName', priority)
        except Exception as e:
            print(f'⚠️ {table} 로딩 실패: {e}')

    cur.close()
    # longest match first
    sorted_terms = sorted(dictionary.items(), key=lambda x: len(x[0]), reverse=True)
    return dictionary, sorted_terms


def _build_date_spans(text):
    spans = []
    date_patterns = [
        re.compile(r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일'),
    ]
    for pat in date_patterns:
        for m in pat.finditer(text):
            start, end = m.start(), m.end()
            y, mo, d = m.group(1), m.group(2), m.group(3)
            when = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
            raw = text[start:end]
            repl = f"<date when=\"{when}\">{raw}</date>"
            spans.append((start, end, repl))
    return spans


def _find_spans(text, term_list):
    spans = []

    def overlaps(a_start, a_end):
        for s, e, _ in spans:
            if a_start < e and a_end > s:
                return True
        return False

    # date spans first
    for s, e, repl in _build_date_spans(text):
        if not overlaps(s, e):
            spans.append((s, e, repl))

    for term, tag in term_list:
        start = 0
        while True:
            idx = text.find(term, start)
            if idx == -1:
                break
            end = idx + len(term)
            if not overlaps(idx, end):
                repl = f"<{tag}>{term}</{tag}>"
                spans.append((idx, end, repl))
            start = end

    spans.sort(key=lambda x: x[0])
    return spans


def _find_spans_with_ner(text, term_list):
    """Find spans using dictionary terms first, then run NER on leftover regions to
    detect person names (PER) and add <persName> spans without overlapping existing spans."""
    spans = _find_spans(text, term_list)

    if not NER_AVAILABLE or not text:
        return spans

    # compute covered intervals
    covered = []
    for s, e, _ in spans:
        covered.append((s, e))

    # invert to get uncovered intervals
    uncovered = []
    pos = 0
    for s, e in sorted(covered, key=lambda x: x[0]):
        if pos < s:
            uncovered.append((pos, s))
        pos = max(pos, e)
    if pos < len(text):
        uncovered.append((pos, len(text)))

    # run NER on each uncovered piece
    new_spans = []
    try:
        for a, b in uncovered:
            piece = text[a:b]
            if not piece.strip():
                continue
            # pipeline will return offsets relative to piece
            try:
                ents = ner_pipeline(piece)
            except Exception:
                # if ner_pipeline fails for this piece, skip
                continue
            if not ents:
                continue
            for ent in ents:
                # support both old 'entity' format (B-PER) and grouped 'entity_group'
                label = ent.get('entity_group') or ent.get('entity') or ent.get('label')
                if not label:
                    continue
                up = str(label).upper()
                if 'PER' in up or 'PERSON' in up:
                    # offsets may be in 'start'/'end' or 'word' structures
                    start_off = ent.get('start')
                    end_off = ent.get('end')
                    if start_off is None or end_off is None:
                        continue
                    global_start = a + int(start_off)
                    global_end = a + int(end_off)
                    # ensure no overlap with existing spans
                    overlap = False
                    for s0, e0, _ in spans + new_spans:
                        if global_start < e0 and global_end > s0:
                            overlap = True
                            break
                    if not overlap and global_start < global_end:
                        name_text = text[global_start:global_end]
                        repl = f"<persName>{name_text}</persName>"
                        new_spans.append((global_start, global_end, repl))
    except Exception as e:
        print(f'⚠️ NER 처리 중 오류: {e}')

    # merge and sort
    all_spans = spans + new_spans
    all_spans.sort(key=lambda x: x[0])
    return all_spans


def _tag_text_segment(text, term_list):
    if not text:
        return text
    spans = _find_spans_with_ner(text, term_list)
    if not spans:
        return text
    out = []
    pos = 0
    for start, end, repl in spans:
        if start > pos:
            out.append(text[pos:start])
        out.append(repl)
        pos = end
    if pos < len(text):
        out.append(text[pos:])
    return ''.join(out)


def _split_xml_segments(text):
    segments = []
    buf = ''
    in_tag = False
    for ch in text:
        if ch == '<':
            if buf:
                segments.append((buf, in_tag))
                buf = ''
            in_tag = True
            buf += ch
        elif ch == '>':
            buf += ch
            segments.append((buf, in_tag))
            buf = ''
            in_tag = False
        else:
            buf += ch
    if buf:
        segments.append((buf, in_tag))
    return segments


def _tag_fragment(fragment, term_list):
    if not fragment:
        return fragment
    parts = []
    for seg, is_tag in _split_xml_segments(fragment):
        if is_tag:
            parts.append(seg)
        else:
            parts.append(_tag_text_segment(seg, term_list))
    return ''.join(parts)


def _add_hanja_gloss_to_text(text: str) -> str:
    if not text:
        return text

    cjk_re = re.compile(r'([\u4E00-\u9FFF]+)(?!\s*<gloss>)')

    def repl(m):
        tok = m.group(1)
        # try library translation first, then per-character fallback
        reading = _hanja_translate(tok)
        if not reading:
            reading = _hanja_char_by_char(tok)
        if reading:
            return f"{tok}<gloss>{reading}</gloss>"
        return f"{tok}<gloss>미상</gloss>"

    return cjk_re.sub(repl, text)


def _clip_text(text, limit=400):
    if text is None:
        return ''
    s = str(text)
    if len(s) <= limit:
        return s
    return s[:limit] + '...'


def process_table(conn, table_name, term_list, limit=5):
    cur = conn.cursor()

    try:
        cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS tei_status text')
    except Exception as e:
        print(f'⚠️ tei_status 컬럼 생성 실패: {table_name} ({e})')
        cur.close()
        return

    limit_sql = f' LIMIT {int(limit)}' if limit and int(limit) > 0 else ''
    cur.execute(
        f'SELECT rowid, tei FROM "{table_name}" '
        "WHERE tei IS NOT NULL AND (tei_status IS NULL OR tei_status != 'REFINED') "
        'ORDER BY rowid' + limit_sql
    )
    rows = cur.fetchall()

    pattern = re.compile(r'(<p[^>]*>)(.*?)(</p>)', re.IGNORECASE | re.DOTALL)

    rollback_samples = []

    for i, (rowid, tei) in enumerate(rows, 1):
        tei = tei or ''
        if 'data-col=""' in tei:
            tei = tei.replace('data-col=""', 'data-col="')
        if not tei:
            continue

        def repl(m):
            open_tag = m.group(1)
            inner = m.group(2)
            close_tag = m.group(3)
            tagged_inner = _tag_fragment(inner, term_list)
            tagged_inner = _add_hanja_gloss_to_text(tagged_inner)
            return f"{open_tag}{tagged_inner}{close_tag}"

        new_tei, nsubs = pattern.subn(repl, tei)
        tei_len = len(tei)
        new_tei_len = len(new_tei)

        if i <= 5:
            print(f"[debug] table={table_name} row={i} rowid={rowid} nsubs={nsubs} tei_len={tei_len} new_tei_len={new_tei_len}")

        if tei_len > 0 and new_tei_len > tei_len * 3.0:
            print('⚠️ 과도한 태깅 감지: 텍스트 확장이 커서 원본 TEI로 롤백합니다.')
            if len(rollback_samples) < 3:
                rollback_samples.append((tei, new_tei))
            new_tei = tei
        # If we made substitutions, prefer to set REFINED only when glosses exist.
        if nsubs > 0:
            has_gloss = '<gloss>' in (new_tei or '')
            status = 'REFINED' if has_gloss else 'NEEDS_GLOSS'
            if not has_gloss:
                print(f"⚠️ rowid={rowid} — 태깅됐지만 <gloss> 없음, tei_status={status}로 설정합니다.")
            cur.execute(
                f'UPDATE "{table_name}" SET tei = %s, tei_status = %s WHERE rowid = %s',
                (new_tei, status, rowid)
            )
        else:
            # no substitutions; mark as REFINED to avoid reprocessing unchanged rows
            cur.execute(
                f'UPDATE "{table_name}" SET tei_status = %s WHERE rowid = %s',
                ('REFINED', rowid)
            )

    if rollback_samples:
        print('--- ROLLBACK SAMPLES (up to 3) ---')
        for idx, (orig, tagged) in enumerate(rollback_samples, 1):
            print(f"[rollback-sample {idx}] original: {_clip_text(orig)}")
            print(f"[rollback-sample {idx}] tagged:   {_clip_text(tagged)}")

    cur.close()


def process_db(table_name='raw_source_info', limit=5, all_tables=False):
    conn = _connect()

    dictionary, term_list = build_dictionary(conn)
    if not dictionary:
        print('⚠️ 사전이 비어있습니다. 태깅을 중단합니다.')
        conn.close()
        return

    if all_tables:
        cur = conn.cursor()
        tables = _list_tei_tables(cur)
        cur.close()
    else:
        tables = [table_name]

    for tbl in tables:
        print(f'--- START: {tbl}')
        process_table(conn, tbl, term_list, limit=limit)
        print(f'--- DONE: {tbl}')

        # 자동으로 TEI를 Neo4j로 로드하는 스크립트를 호출합니다.
        try:
            tei_to_neo4j = Path(__file__).resolve().parent / 'tei_to_neo4j.py'
            if tei_to_neo4j.exists():
                print(f"자동호출: {tei_to_neo4j.name} 실행")
                # run without wipe by default
                subprocess.run([sys.executable, str(tei_to_neo4j)], check=True)
            else:
                print(f"tei_to_neo4j 스크립트가 없습니다: {tei_to_neo4j}")
        except subprocess.CalledProcessError as e:
            print(f"ERROR: tei_to_neo4j failed with exit {e.returncode}")
        except Exception as e:
            print(f"Warning: 자동 tei_to_neo4j 호출 중 오류: {e}")

    conn.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--table', default='raw_source_info')
    p.add_argument('--limit', type=int, default=5, help='처리할 최대 행 수 (기본: 5)')
    p.add_argument('--all', action='store_true', help='tei 컬럼이 있는 모든 테이블을 처리합니다')
    args = p.parse_args()

    try:
        process_db(args.table, limit=args.limit, all_tables=args.all)
    except Exception as e:
        print(f"오류: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
