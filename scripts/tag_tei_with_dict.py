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

# Optimize transformers behavior
os.environ['TRANSFORMERS_OFFLINE'] = '1'

# Attempt to import transformers NER pipeline once; fall back gracefully if unavailable
NER_AVAILABLE = False
ner_pipeline = None
_NER_MODEL_NAME = None
NER_GROUPED = False
try:
    from transformers import pipeline
    # Try multiple candidate models (the environment may not have all downloaded)
    _model_candidates = [
        "monologg/koelectra-base-finetuned-naver-ner",
        "monologg/kocharelectra-base-modu-ner-all",
        "monologg/kocharelectra-base-modu-ner-nx",
        "monologg/kocharelectra-base-modu-ner-sx",
        "kykim/bert-kor-base",
    ]
    for _m in _model_candidates:
        try:
            # Check for Apple Silicon MPS acceleration
            import torch
            device = "mps" if torch.backends.mps.is_available() else -1
            
            # some transformers versions don't accept grouped_entities at init; try and fall back
            try:
                # Set batch_size for better GPU utilization
                ner_pipeline = pipeline("ner", model=_m, aggregation_strategy="none", device=device, batch_size=32)
                NER_GROUPED = False
            except Exception:
                # fallback: create pipeline without aggregation_strategy and we will group manually
                ner_pipeline = pipeline("ner", model=_m, device=device, batch_size=32)
                NER_GROUPED = False
            NER_AVAILABLE = True
            _NER_MODEL_NAME = _m
            print(f'✅ NER 모델 로드 성공: {_m} (device={device}, batch_size=32)')
            break
        except Exception as e:
            # Only print error if it's not a "local files not found" error for the first few candidates
            if "local_files_only=True" not in str(e):
                print(f'⚠️ NER 모델({_m}) 로드 실패: {e}')
    if not NER_AVAILABLE:
        print('⚠️ 모든 NER 모델 로드에 실패했습니다 — 사전 기반 태깅만 사용합니다.')
except Exception as e:
    print(f"❌ NER 시스템 초기화 중 치명적 오류: {e}")
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
    if secret_file is None:
        # 1. Try current working directory
        path1 = os.path.join(os.getcwd(), '.streamlit', 'secrets.toml')
        # 2. Try relative to this script
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

PGHOST = _secret_or_env('PG_HOST', '127.0.0.1', SECRETS)
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
    try:
        # Mask host for logging if it's not localhost/127.0.0.1
        display_host = PGHOST if PGHOST in ['localhost', '127.0.0.1'] else f"{PGHOST[:3]}***"
        print(f"🔌 PostgreSQL 연결 시도 중... (host={display_host}, database={PGDATABASE})")
        conn = psycopg2.connect(host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE, connect_timeout=10)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f'❌ PostgreSQL 연결 실패: {e}')
        sys.exit(1)


def _ensure_connection(conn):
    """Check if connection is alive, reconnect if not."""
    if conn is None:
        return _connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT 1')
        cur.close()
        return conn
    except Exception:
        print("🔄 DB 연결이 끊어졌습니다. 재연결을 시도합니다...")
        try:
            conn.close()
        except Exception:
            pass
        return _connect()


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


from functools import lru_cache
from concurrent.futures import ProcessPoolExecutor

@lru_cache(maxsize=10000)
def _hanja_translate(token: str) -> str:
    if not token:
        return ''
    if hanja is None:
        return ''
    try:
        res = hanja.translate(token, 'substitution')
    except Exception:
        res = ''

    # If hanja.translate returned nothing, try a conservative per-char fallback
    if not res:
        fb = _hanja_char_by_char(token)
        if fb:
            res = fb

    # Apply manual corrections for common Korean surname readings
    try:
        # 김씨 예외 단어 (금으로 시작해도 성으로 바꾸면 안 되는 경우)
        kim_exceptions = {"金剛", "金剛山", "金融", "金屬", "金庫", "金鑛"}
        if token.startswith('金') and len(token) >= 2 and res.startswith('금'):
            if not any(token.startswith(exc) for exc in kim_exceptions):
                res = '김' + (res[1:] if len(res) > 1 else '')

        # 두음법칙 강제 교정 (성씨에 대해서만 적용)
        if token.startswith('李') and res.startswith('리'):
            res = '이' + (res[1:] if len(res) > 1 else '')
        if token.startswith('柳') and res.startswith('류'):
            res = '유' + (res[1:] if len(res) > 1 else '')
        if token.startswith('林') and res.startswith('림'):
            res = '임' + (res[1:] if len(res) > 1 else '')
        if token.startswith('盧') and res.startswith('로'):
            res = '노' + (res[1:] if len(res) > 1 else '')
        if token.startswith('羅') and res.startswith('라'):
            res = '나' + (res[1:] if len(res) > 1 else '')
    except Exception:
        # Fail-safe: if any unexpected error occurs, return the original res
        pass

    return res


def _tag_single_tei(rowid, tei, term_list):
    """Worker function for multiprocessing.
    Note: Globals like ner_pipeline are re-initialized or shared depending on OS.
    On macOS (spawn), they might be re-initialized in each process.
    """
    tei = tei or ''
    if 'data-col=""' in tei:
        tei = tei.replace('data-col=""', 'data-col="')
    if not tei:
        return None

    # Each worker might need its own pattern if not thread-safe, but here it's process-safe.
    pattern = re.compile(r'(<p[^>]*>)(.*?)(</p>)', re.IGNORECASE | re.DOTALL)

    def repl(m):
        open_tag = m.group(1)
        inner = m.group(2)
        close_tag = m.group(3)
        tagged_inner = _tag_fragment(inner, term_list)
        tagged_inner = _add_hanja_gloss_to_text(tagged_inner)
        tagged_inner = _tag_persons_in_glosses(tagged_inner)
        return f"{open_tag}{tagged_inner}{close_tag}"

    new_tei, nsubs = pattern.subn(repl, tei)
    tei_len = len(tei)
    new_tei_len = len(new_tei)

    # Rollback logic
    if tei_len > 0 and new_tei_len > tei_len * 3.0:
        new_tei = tei
        nsubs = 0

    if nsubs > 0:
        return (rowid, new_tei, 'REFINED')
    else:
        return (rowid, None, 'REFINED')


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
    if not text:
        return []
    spans = []

    # date spans first
    for s, e, repl in _build_date_spans(text):
        spans.append((s, e, repl))

    # Pre-compile regex for dictionary terms if not already done
    # Note: Using a single large regex for all terms is generally faster.
    # However, to maintain priority (persName > placeName etc.), we use term_list.
    # To optimize, we can use a single regex for each tag type or one big one.
    # Let's try a single regex for all terms for maximum speed.
    if term_list:
        # Longest match first is naturally handled by sorted term_list
        pattern = re.compile('|'.join(re.escape(term) for term, _ in term_list))
        
        # Build term to tag mapping for quick lookup
        term_to_tag = dict(term_list)

        def overlaps(a_start, a_end):
            for s, e, _ in spans:
                if a_start < e and a_end > s:
                    return True
            return False

        for m in pattern.finditer(text):
            idx, end = m.start(), m.end()
            if not overlaps(idx, end):
                term = m.group(0)
                tag = term_to_tag.get(term, 'term')
                repl = f"<{tag}>{term}</{tag}>"
                spans.append((idx, end, repl))

    spans.sort(key=lambda x: x[0])
    return spans


def _group_token_entities(ents, offset):
    groups = []
    cur_s = None
    cur_e = None
    for ent in ents:
        label = ent.get('entity') or ent.get('label') or ''
        if not label:
            continue
        up = str(label).upper()
        if 'PER' in up or 'PERSON' in up:
            s = ent.get('start')
            e = ent.get('end')
            if s is None or e is None:
                continue
            s = int(s) + offset
            e = int(e) + offset
            if cur_s is None:
                cur_s, cur_e = s, e
            else:
                if s <= cur_e:
                    cur_e = max(cur_e, e)
                else:
                    groups.append((cur_s, cur_e))
                    cur_s, cur_e = s, e
    if cur_s is not None:
        groups.append((cur_s, cur_e))
    return groups


def _find_spans_with_ner(text, term_list):
    """Find spans using dictionary terms first, then run NER on the whole text to
    detect person names (PER) and add <persName> spans without overlapping existing spans."""
    spans = _find_spans(text, term_list)

    if not NER_AVAILABLE or not text or len(text) < 2:
        return spans

    new_spans = []
    try:
        # Run NER once on the entire text
        # This is much faster than running on multiple small fragments.
        ents = ner_pipeline(text)
        if not ents:
            return spans

        # Build a helper to check overlaps
        def is_overlapping(s, e):
            for s0, e0, _ in spans + new_spans:
                if s < e0 and e > s0:
                    return True
            return False

        if NER_GROUPED:
            for ent in ents:
                label = ent.get('entity_group') or ent.get('entity') or ent.get('label')
                if not label:
                    continue
                up = str(label).upper()
                if 'PER' in up or 'PERSON' in up:
                    s = int(ent.get('start', -1))
                    e = int(ent.get('end', -1))
                    if s < 0 or e < 0 or (e - s) < 2:
                        continue
                    if not is_overlapping(s, e):
                        name_text = text[s:e]
                        # NER skip if it contains Hanja (covered by dictionary/gloss usually)
                        if re.search(r'[\u4E00-\u9FFF]', name_text):
                            continue
                        new_spans.append((s, e, f"<persName>{name_text}</persName>"))
        else:
            # Group token-level entities
            groups = _group_token_entities(ents, 0)
            for s, e in groups:
                if (e - s) < 2:
                    continue
                if not is_overlapping(s, e):
                    name_text = text[s:e]
                    if re.search(r'[\u4E00-\u9FFF]', name_text):
                        continue
                    new_spans.append((s, e, f"<persName>{name_text}</persName>"))

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
            # If this is a gloss tag, allow NER inside the gloss content
            m = re.match(r"^<gloss([^>]*)>(.*?)</gloss>$", seg, flags=re.DOTALL)
            if m:
                inner = m.group(2)
                # run tagging (which includes dictionary+NER) on the gloss text
                tagged_inner = _tag_text_segment(inner, term_list)
                parts.append(f"<gloss{m.group(1)}>{tagged_inner}</gloss>")
            else:
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


def _tag_persons_in_glosses(text: str) -> str:
    if not text:
        return text

    def best_person_end(piece: str) -> int:
        if not piece or re.search(r'[\s\u4E00-\u9FFF]', piece):
            return 0

        best_end = 0
        probe_texts = [piece + suffix for suffix in ['은', '는', '이', '가', '의', '도']] + [piece]
        try:
            # Batch inference for all probe texts at once
            batch_ents = ner_pipeline(probe_texts)
        except Exception:
            return 0

        for ents in batch_ents:
            if not ents:
                continue
            if NER_GROUPED:
                for ent in ents:
                    label = ent.get('entity_group') or ent.get('entity') or ent.get('label')
                    if not label:
                        continue
                    up = str(label).upper()
                    if 'PER' in up or 'PERSON' in up:
                        start_off = ent.get('start')
                        end_off = ent.get('end')
                        if start_off == 0 and end_off is not None:
                            best_end = max(best_end, min(int(end_off), len(piece)))
            else:
                groups = _group_token_entities(ents, 0)
                for global_start, global_end in groups:
                    if global_start == 0:
                        best_end = max(best_end, min(global_end, len(piece)))
        return best_end

    def repl(m):
        prefix = m.string[max(0, m.start() - 60):m.start()]
        def is_open(tag_name):
            return prefix.count(f'<{tag_name}') > prefix.count(f'</{tag_name}>')

        if is_open('placeName') or is_open('orgName') or is_open('date'):
            return m.group(0)
        inner = m.group(1)
        end = best_person_end(inner)
        if end >= 2:
            return f"<gloss><persName>{inner[:end]}</persName></gloss>"
        return m.group(0)

    return re.sub(r'<gloss>([가-힣]{2,4})</gloss>', repl, text)


def _clip_text(text, limit=400):
    if text is None:
        return ''
    s = str(text)
    if len(s) <= limit:
        return s
    return s[:limit] + '...'


def process_table(conn, table_name, term_list, limit=5, rowid=None):
    cur = conn.cursor()

    try:
        cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS tei_status text')
    except Exception as e:
        print(f'⚠️ tei_status 컬럼 생성 실패: {table_name} ({e})')
        cur.close()
        return

    if rowid is not None:
        where_clause = "WHERE tei IS NOT NULL AND rowid = %s"
    else:
        where_clause = "WHERE tei IS NOT NULL AND (tei_status IS NULL OR tei_status != 'REFINED')"
    params = [int(rowid)] if rowid is not None else []
    limit_sql = '' if rowid is not None else (f' LIMIT {int(limit)}' if limit and int(limit) > 0 else '')
    cur.execute(
        f'SELECT rowid, tei FROM "{table_name}" {where_clause} ORDER BY rowid' + limit_sql,
        params,
    )
    rows = cur.fetchall()
    total = len(rows)
    if total == 0:
        cur.close()
        return

    print(f"🚀 {table_name}: {total}개 레코드 대량 병렬 처리 시작 (M5 Pro + MPS 최적화)...")
    
    updates = []
    status_only_updates = []

    # Use multiprocessing with chunking to reduce overhead
    # 14 workers for M5 Pro's performance cores
    with ProcessPoolExecutor(max_workers=14) as executor:
        # Process in larger chunks (e.g., 100 rows per job) to minimize IPC overhead
        chunk_size = 100
        futures = []
        for i in range(0, total, chunk_size):
            chunk = rows[i:i + chunk_size]
            futures.append(executor.submit(_process_chunk, chunk, term_list))
            
        for i, future in enumerate(futures, 1):
            try:
                chunk_results = future.result()
                for res in chunk_results:
                    if res:
                        rid, new_tei, status = res
                        if new_tei:
                            updates.append((new_tei, status, rid))
                        else:
                            status_only_updates.append((status, rid))
            except Exception as e:
                print(f"⚠️ 청크 처리 중 오류: {e}")
            
            prog = min(i * chunk_size, total)
            if i % 5 == 0 or prog == total:
                print(f"  → 진행률: {prog}/{total} ({prog/total*100:.1f}%)")

    # Perform batch updates with larger page size and retry logic
    from psycopg2.extras import execute_batch
    import time
    
    # Ensure connection is alive before updates
    conn = _ensure_connection(conn)
    cur = conn.cursor()

    def _safe_execute_batch(cursor, sql, data, page_size=2000):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                execute_batch(cursor, sql, data, page_size=page_size)
                return True
            except Exception as e:
                print(f"⚠️ 배치 업데이트 중 오류 (시도 {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    nonlocal conn, cur
                    conn = _ensure_connection(conn)
                    cur = conn.cursor()
                    cursor = cur
                    time.sleep(2)
                else:
                    raise e
        return False

    if updates:
        _safe_execute_batch(cur, f'UPDATE "{table_name}" SET tei = %s, tei_status = %s WHERE rowid = %s', updates, page_size=2000)
        print(f"  ✓ {len(updates)}개 레코드 대량 업데이트 완료 (tei + status)")
    
    if status_only_updates:
        _safe_execute_batch(cur, f'UPDATE "{table_name}" SET tei_status = %s WHERE rowid = %s', status_only_updates, page_size=2000)
        print(f"  ✓ {len(status_only_updates)}개 레코드 상태 대량 업데이트 완료")

    cur.close()


def _process_chunk(chunk, term_list):
    """Process a chunk of rows in a single worker job, batching NER for GPU efficiency"""
    results = []
    
    # 1. Extract all paragraphs from all documents in this chunk
    chunk_data = []
    all_texts_to_ner = []
    
    pattern = re.compile(r'(<p[^>]*>)(.*?)(</p>)', re.IGNORECASE | re.DOTALL)
    
    for rowid, tei in chunk:
        tei = tei or ''
        if 'data-col=""' in tei:
            tei = tei.replace('data-col=""', 'data-col="')
        
        matches = list(pattern.finditer(tei))
        paragraphs = [m.group(2) for m in matches]
        chunk_data.append({
            'rowid': rowid,
            'tei': tei,
            'matches': matches,
            'paragraphs': paragraphs
        })
        all_texts_to_ner.extend(paragraphs)

    # 2. Batch NER inference for the entire chunk
    ner_map = {}
    if NER_AVAILABLE and all_texts_to_ner:
        try:
            # unique texts to avoid redundant inference
            unique_texts = [t for t in list(set(all_texts_to_ner)) if t and len(t) > 2]
            if unique_texts:
                print(f"  [worker] {len(unique_texts)}개 문단에 대해 NER 추론 수행 중...")
                # The pipeline with batch_size=32 will handle the heavy lifting on GPU
                ner_results = ner_pipeline(unique_texts)
                ner_map = dict(zip(unique_texts, ner_results))
        except Exception as e:
            print(f"⚠️ 배치 NER 추론 중 오류: {e}")

    # 3. Process each document using the pre-computed NER results
    for data in chunk_data:
        rowid = data['rowid']
        tei = data['tei']
        if not tei:
            results.append(None)
            continue
            
        def repl_with_ner(m):
            open_tag = m.group(1)
            inner = m.group(2)
            close_tag = m.group(3)
            
            # Use pre-computed NER results for this specific text
            ents = ner_map.get(inner, [])
            
            # Custom tagging logic that uses the provided entities
            tagged_inner = _tag_fragment_optimized(inner, term_list, ents)
            tagged_inner = _add_hanja_gloss_to_text(tagged_inner)
            tagged_inner = _tag_persons_in_glosses_optimized(tagged_inner, ents)
            return f"{open_tag}{tagged_inner}{close_tag}"

        new_tei, nsubs = pattern.subn(repl_with_ner, tei)
        tei_len = len(tei)
        new_tei_len = len(new_tei)

        if tei_len > 0 and new_tei_len > tei_len * 3.0:
            new_tei = tei
            nsubs = 0

        if nsubs > 0:
            results.append((rowid, new_tei, 'REFINED'))
        else:
            results.append((rowid, None, 'REFINED'))
            
    return results


def _tag_fragment_optimized(fragment, term_list, precomputed_ents):
    if not fragment:
        return fragment
    parts = []
    for seg, is_tag in _split_xml_segments(fragment):
        if is_tag:
            m = re.match(r"^<gloss([^>]*)>(.*?)</gloss>$", seg, flags=re.DOTALL)
            if m:
                inner = m.group(2)
                tagged_inner = _tag_text_segment_optimized(inner, term_list, []) 
                parts.append(f"<gloss{m.group(1)}>{tagged_inner}</gloss>")
            else:
                parts.append(seg)
        else:
            parts.append(_tag_text_segment_optimized(seg, term_list, precomputed_ents))
    return ''.join(parts)


def _tag_text_segment_optimized(text, term_list, precomputed_ents):
    if not text:
        return text
    spans = _find_spans_with_precomputed_ner(text, term_list, precomputed_ents)
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


def _find_spans_with_precomputed_ner(text, term_list, ents):
    spans = _find_spans(text, term_list)
    if not ents or not text or len(text) < 2:
        return spans

    new_spans = []
    def is_overlapping(s, e):
        for s0, e0, _ in spans + new_spans:
            if s < e0 and e > s0:
                return True
        return False

    if NER_GROUPED:
        for ent in ents:
            label = ent.get('entity_group') or ent.get('entity') or ent.get('label')
            if not label: continue
            if 'PER' in str(label).upper():
                s, e = int(ent.get('start', -1)), int(ent.get('end', -1))
                if s < 0 or e < 0 or (e-s) < 2: continue
                if not is_overlapping(s, e):
                    name = text[s:e]
                    if not re.search(r'[\u4E00-\u9FFF]', name):
                        new_spans.append((s, e, f"<persName>{name}</persName>"))
    else:
        groups = _group_token_entities(ents, 0)
        for s, e in groups:
            if (e - s) < 2: continue
            if not is_overlapping(s, e):
                name = text[s:e]
                if not re.search(r'[\u4E00-\u9FFF]', name):
                    new_spans.append((s, e, f"<persName>{name}</persName>"))

    all_spans = spans + new_spans
    all_spans.sort(key=lambda x: x[0])
    return all_spans


def _tag_persons_in_glosses_optimized(text, precomputed_ents):
    return _tag_persons_in_glosses(text)


def process_db(table_name='raw_source_info', limit=5, all_tables=False, rowid=None, skip_neo4j=False):
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
        process_table(conn, tbl, term_list, limit=limit, rowid=rowid)
        print(f'--- DONE: {tbl}')

        if skip_neo4j:
            continue

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
    p.add_argument('--rowid', type=int, help='특정 rowid 하나만 처리합니다')
    p.add_argument('--skip-neo4j', action='store_true', help='tei_to_neo4j 후속 실행을 건너뜁니다')
    args = p.parse_args()

    try:
        process_db(args.table, limit=args.limit, all_tables=args.all, rowid=args.rowid, skip_neo4j=args.skip_neo4j)
    except Exception as e:
        print(f"오류: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
