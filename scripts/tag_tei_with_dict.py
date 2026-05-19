"""Tag TEI content using dictionary matches from PostgreSQL.
[Clean Architecture Version - Single Thread Sequential Building with Autocommit]
"""

import argparse
import os
import re
import threading
import sys
import subprocess
from pathlib import Path

# Optimize transformers behavior
os.environ['TRANSFORMERS_OFFLINE'] = '1'
os.environ['TOKENIZERS_PARALLELISM'] = 'false'
ner_lock = threading.Lock()
DISABLE_NER = os.getenv('DISABLE_NER', '').lower() in ('1', 'true', 'yes')

# NER Initialization
NER_AVAILABLE = False
ner_pipeline = None
NER_BATCH_SIZE = int(os.getenv('NER_BATCH_SIZE', '256'))

if not DISABLE_NER:
    try:
        from transformers import pipeline
        import torch
        device = "mps" if torch.backends.mps.is_available() else -1
        _model_candidates = [
            "monologg/koelectra-base-finetuned-naver-ner",
            "kykim/bert-kor-base",
        ]
        for _m in _model_candidates:
            try:
                try:
                    ner_pipeline = pipeline("ner", model=_m, aggregation_strategy="simple", device=device, batch_size=NER_BATCH_SIZE)
                except Exception:
                    ner_pipeline = pipeline("ner", model=_m, device=device, batch_size=NER_BATCH_SIZE)
                NER_AVAILABLE = True
                print(f'✅ NER 모델 로드 성공: {_m} (device={device})')
                break
            except Exception as e:
                pass
    except Exception as e:
        print(f"❌ NER 시스템 초기화 중 오류: {e}")

try: import psycopg2
except Exception: psycopg2 = None
try: import tomllib
except Exception: import tomli as tomllib
try: import hanja
except Exception: hanja = None

HANJA_TO_HANGUL = {
    '忠': '충', '淸': '청', '南': '남', '天': '천', '安': '안', '郡': '군', '葛': '갈', '田': '전', '面': '면', '並': '병', '川': '천',
    '市': '시', '場': '장', '京': '경', '東': '동', '龍': '용', '頭': '두', '里': '리', '山': '산', '柳': '유', '寬': '관', '順': '순',
    '重': '중', '武': '무', '金': '김', '用': '용', '伊': '이', '白': '백', '正': '정', '云': '운', '朴': '박', '濟': '제', '奭': '석',
    '萬': '만', '衡': '형', '相': '상', '勳': '훈', '仁': '인', '元': '원', '炳': '병', '鎬': '호', '鳳': '봉', '來': '래', '權': '권',
    '孟': '맹', '星': '성', '鄭': '정', '春': '춘', '永': '영', '溱': '진', '太': '태', '部': '부', '官': '관', '駐': '주', '在': '재', '所': '소'
}

def _hanja_char_by_char(token: str) -> str:
    if not token: return ''
    return ''.join([HANJA_TO_HANGUL.get(ch, ch if ord(ch) < 128 else '') for ch in token])

def _load_secrets():
    paths = [Path.cwd() / '.streamlit' / 'secrets.toml', Path(__file__).resolve().parent.parent / '.streamlit' / 'secrets.toml']
    for p in paths:
        if p.exists():
            with open(p, 'rb') as f: return tomllib.load(f)
    return {}

SECRETS = _load_secrets()
PGHOST = os.getenv('PG_HOST', SECRETS.get('PG_HOST', '127.0.0.1'))
PGPORT = int(os.getenv('PG_PORT', SECRETS.get('PG_PORT', 5432)))
PGUSER = os.getenv('PG_USER', SECRETS.get('PG_USER', 'postgres'))
PGPASSWORD = os.getenv('PG_PASSWORD', SECRETS.get('PG_PASSWORD', ''))
PGDATABASE = os.getenv('PG_DATABASE', SECRETS.get('PG_DATABASE', 'postgres'))

def _connect():
    try: 
        conn = psycopg2.connect(host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE)
        # 핵심 추가: 업데이트 즉시 락을 해제하고 확정 저장하여 데드락을 방지합니다.
        conn.autocommit = True 
        return conn
    except Exception as e: 
        print(f"❌ DB 연결 오류: {e}")
        sys.exit(1)

def _get_columns(cur, table_name):
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s", (table_name,))
    return [r[0] for r in cur.fetchall()]

from functools import lru_cache
@lru_cache(maxsize=10000)
def _hanja_translate(token: str) -> str:
    if not token or not hanja: return ''
    try: res = hanja.translate(token, 'substitution')
    except Exception: res = ''
    if not res: res = _hanja_char_by_char(token)
    
    kim_exceptions = {"金剛", "金剛山", "金融", "金屬", "金庫", "金鑛"}
    if token.startswith('金') and len(token) >= 2 and res.startswith('금') and not any(token.startswith(x) for x in kim_exceptions):
        res = '김' + res[1:]
    for han, kor in [('李','이'), ('柳','유'), ('林','임'), ('盧','노'), ('羅','나')]:
        if token.startswith(han) and len(res) > 1 and res.startswith(kor): res = kor + res[1:]
    return res

def build_dictionary(conn):
    cur = conn.cursor()
    dic = {}
    priority = {'persName': 3, 'placeName': 2, 'orgName': 1, 'term': 0}
    
    def _add(name, tag):
        name = str(name).strip() if name else ''
        if name and priority.get(tag, 0) >= priority.get(dic.get(name, ''), -1): dic[name] = tag

    for col in _get_columns(cur, 'raw_event_info'):
        if any(k in col.lower() for k in ['관련인물', '인물', '피고인', '사건명', '사건']):
            cur.execute(f'SELECT "{col}" FROM raw_event_info')
            for (val,) in cur.fetchall():
                for p in re.split(r'[;,/]', str(val or '')): _add(p, 'persName' if '인물' in col or '피고' in col else 'term')
    
    for col in _get_columns(cur, 'raw_detail_place'):
        if any(k in col.lower() for k in ['명칭', '장소명', '이름', '이칭', '별칭']):
            cur.execute(f'SELECT "{col}" FROM raw_detail_place')
            for (val,) in cur.fetchall():
                for p in re.split(r'[;,/]', str(val or '')): _add(p, 'placeName')

    extra = {}
    for name, tag in dic.items():
        if re.search(r'[\u4E00-\u9FFF]', name):
            rd = _hanja_translate(name) or _hanja_char_by_char(name)
            if rd and rd not in dic: extra[rd] = tag
    dic.update(extra)
    cur.close()
    return dic, sorted([(k, v) for k, v in dic.items() if len(k) >= 2], key=lambda x: len(x[0]), reverse=True)

def _normalize_person_entity_span(text, start, end):
    s, e = int(start), int(end)
    while s < e and text[s] in " \t\n\r\"'.,;:!?()[]{}<>~`|/\\-_=+*&#%^$@": s += 1
    if s >= e: return None
    m = re.match(r'^([가-힣]+)', text[s:min(len(text), s + 12)])
    if not m: return None
    chunk = m.group(1)
    for L in [4, 3, 2]:
        if len(chunk) > L and any(chunk[L:].startswith(j) for j in ['에게','에서','으로','은','는','이','가','의','을','를','도','과','와','로']):
            return (s, s+L, chunk[:L])
    return (s, s+len(chunk), chunk) if len(chunk) > 1 else None

def _process_chunk(chunk, term_list):
    results = []
    p_pattern = re.compile(r'(<p[^>]*>)(.*?)(</p>)', re.IGNORECASE | re.DOTALL)
    dict_regex = re.compile('|'.join(re.escape(t) for t, _ in term_list)) if term_list else None
    term_dict = dict(term_list)

    for rowid, tei in chunk:
        if not tei:
            results.append(None)
            continue

        def process_p(m):
            open_tag, text, close_tag = m.groups()
            if not text.strip(): return m.group(0)

            shadow_chars = []
            for ch in text:
                if '\u4e00' <= ch <= '\u9fff':
                    cv = _hanja_translate(ch) or _hanja_char_by_char(ch) or ch
                    shadow_chars.append(cv[0] if cv else ch)
                else: shadow_chars.append(ch)
            shadow_text = "".join(shadow_chars).replace('류', '유').replace('리', '이').replace('로', '노').replace('림', '임')

            spans = []
            def is_overlap(s, e): return any(s < e0 and e > s0 for s0, e0, _, _ in spans)

            for m_date in re.finditer(r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일', text):
                s, e = m_date.span()
                spans.append((s, e, 'DATE', f"{int(m_date.group(1)):04d}-{int(m_date.group(2)):02d}-{int(m_date.group(3)):02d}"))

            if dict_regex:
                for m_dict in dict_regex.finditer(shadow_text):
                    s, e = m_dict.span()
                    if not is_overlap(s, e): spans.append((s, e, 'TAG', term_dict.get(m_dict.group(0), 'term')))

            if NER_AVAILABLE:
                try:
                    with ner_lock: ents = ner_pipeline(shadow_text)
                    flat_ents = [e for sub in ents for e in sub] if isinstance(ents, list) and len(ents)>0 and isinstance(ents[0], list) else (ents if isinstance(ents, list) else [ents])
                    for ent in flat_ents:
                        if not ent: continue
                        if 'PER' in str(ent.get('entity_group', ent.get('entity', ''))).upper():
                            norm = _normalize_person_entity_span(shadow_text, ent.get('start'), ent.get('end'))
                            if norm and not is_overlap(norm[0], norm[1]): spans.append((norm[0], norm[1], 'TAG', 'persName'))
                except Exception: pass

            for m_hanja in re.finditer(r'[\u4E00-\u9FFF]+', text):
                s, e = m_hanja.span()
                if not is_overlap(s, e): spans.append((s, e, 'HANJA', None))

            spans.sort(key=lambda x: x[0])
            res, last_idx = [], 0
            
            for s, e, stype, data in spans:
                res.append(text[last_idx:s])
                chunk_text = text[s:e]
                
                def gloss(inner_txt):
                    ret = ""
                    for ch in inner_txt:
                        if '\u4e00' <= ch <= '\u9fff':
                            rd = _hanja_translate(ch) or _hanja_char_by_char(ch)
                            ret += f"{ch}<gloss>{rd}</gloss>" if rd else ch
                        else: ret += ch
                    return ret

                if stype == 'DATE': res.append(f'<date when="{data}">{chunk_text}</date>')
                elif stype == 'TAG': res.append(f'<{data}>{gloss(chunk_text)}</{data.split()[0]}>')
                elif stype == 'HANJA': res.append(gloss(chunk_text))
                
                last_idx = e
                
            res.append(text[last_idx:])
            return f"{open_tag}{''.join(res)}{close_tag}"

        new_tei = p_pattern.sub(process_p, tei.replace('data-col=""', 'data-col="'))
        results.append((rowid, new_tei, 'REFINED') if new_tei != tei else (rowid, None, 'REFINED'))
    return results

def process_table(conn, table_name, term_list, limit=5, event_id=None):
    cur = conn.cursor()
    cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS tei_status text')

    params = []
    if event_id:
        id_col = "아이디" if table_name == "raw_event_info" else "사건id"
        where_clause = f'WHERE tei IS NOT NULL AND "{id_col}" = %s'
        params.append(event_id)
        limit_sql = ""
    else:
        where_clause = "WHERE tei IS NOT NULL AND (tei_status IS NULL OR tei_status != 'REFINED')"
        limit_sql = f' LIMIT {int(limit)}' if limit and int(limit) > 0 else ''

    cur.execute(f'SELECT rowid, tei FROM "{table_name}" {where_clause} ORDER BY rowid' + limit_sql, params)
    rows = cur.fetchall()
    if not rows:
        if event_id: print(f"❌ 경고: {table_name}에서 이벤트 ID '{event_id}'에 해당하는 데이터를 찾을 수 없습니다.")
        return

    print(f"🚀 {table_name}: {len(rows)}개 레코드 단일 스레드 순차 처리 시작 (데드락 방지/자동저장)...")
    updates, status_updates = [], []
    
    # 멀티스레딩 완전 제거, 순차 처리 및 진행률 표시
    chunk_size = 100
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i+chunk_size]
        results = _process_chunk(chunk, term_list)
        
        for res in results:
            if res:
                if res[1]: updates.append((res[1], res[2], res[0]))
                else: status_updates.append((res[2], res[0]))
                
        # 1000건마다 진행률 출력
        processed = min(i + chunk_size, len(rows))
        if processed % 1000 == 0 or processed == len(rows):
            print(f"  → 진행률: {processed} / {len(rows)} ({(processed/len(rows))*100:.1f}%)")

    from psycopg2.extras import execute_batch
    if updates: execute_batch(cur, f'UPDATE "{table_name}" SET tei = %s, tei_status = %s WHERE rowid = %s', updates)
    if status_updates: execute_batch(cur, f'UPDATE "{table_name}" SET tei_status = %s WHERE rowid = %s', status_updates)
    print(f"  ✓ 완료: {len(updates)}건 본문 업데이트, {len(status_updates)}건 상태 업데이트")
    cur.close()

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--table', default='raw_source_info')
    p.add_argument('--limit', type=int, default=5)
    p.add_argument('--all', action='store_true')
    p.add_argument('--event_id', type=str, help='특정 사건 ID 하나만 처리합니다')
    p.add_argument('--skip-neo4j', action='store_true')
    args = p.parse_args()

    conn = _connect()
    dic, term_list = build_dictionary(conn)
    
    if args.all:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema = 'public' AND column_name = 'tei'")
        tables = [r[0] for r in cur.fetchall()]
        cur.close()
    else:
        tables = [args.table]

    for tbl in tables: process_table(conn, tbl, term_list, limit=args.limit, event_id=args.event_id)
    
    if not args.skip_neo4j:
        try: 
            tei_to_neo4j_script = str(Path(__file__).resolve().parent / 'tei_to_neo4j.py')
            subprocess.run([sys.executable, tei_to_neo4j_script], check=True)
        except Exception as e: 
            print(f"⚠️ Neo4j 로드 중 오류 (확인 필요): {e}")
            
    conn.close()

if __name__ == '__main__':
    main()