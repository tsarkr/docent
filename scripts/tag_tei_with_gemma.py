"""Tag TEI <p data-col="출처정보"> content using local Gemma model (gemma4:26b).

Usage:
    python scripts/tag_tei_with_gemma.py --table raw_source_info --limit 5

The script reads from PostgreSQL and updates the `tei` column in-place. It looks for
<p data-col="출처정보">...</p> fragments inside the TEI column and sends the inner
content to the local `gemma4:26b` model (via `ollama.chat`) with an instruction to
add the following TEI tags where appropriate:

- <placeName> : 역사적 지명
- <persName>  : 인명
- <orgName>   : 조직/기관
- <date when="..."> : 날짜 (ISO-ish when 속성 사용 권장)
- <term>      : 특정 지위나 용어
- <gloss>     : 한자/일본어 표현의 병기 (한글 독음/의미)

The original paragraph wrapper and other TEI should be preserved. Rows are updated
with tei_status='REFINED' after successful processing.
"""

import argparse
import os
import re
import sys
import time
import importlib
import signal
from typing import Optional

try:
    import ollama
except Exception:
    ollama = None
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


PROMPT_TEMPLATE = """
당신은 역사 도슨트이자 XML 태깅 도우미입니다. 아래 지시를 엄격히 따르세요.

[작업 규칙]
1. 절대 텍스트를 창작하거나 반복하지 마시오.
2. 주어진 텍스트 안의 명사(인물, 장소, 기구)에만 XML 태그를 씌워서 반환하시오.
3. 마크다운(`xml`)이나 설명 없이 오직 태깅된 결과물만 반환하시오.

[예시 입력]
1919년 3월 1일, 홍길동은 탑골공원에서 만세를 불렀다.

[예시 출력]
<date when="1919-03-01">1919년 3월 1일</date>, <persName>홍길동</persName>은 <placeName>탑골공원</placeName>에서 만세를 불렀다.

아래 추가 규칙을 준수하세요:
- 이미 해당 XML 태그가 존재하면 중복 태깅하지 마시오.
- 날짜는 가능하면 `when` 속성에 ISO(YYYY-MM 또는 YYYY-MM-DD) 형태로 표기하세요.
- 한자/일문 표기에는 `<gloss>`를 추가하고 한글독음(및 가능하면 의미)을 병기하세요.
- 출력은 오직 태깅된 원문 조각(원문에서 태그만 추가된 형태)이어야 하며, 어떠한 설명도 포함하지 마십시오.

사용자 메시지로 전달된 텍스트만 태깅해 그대로 반환하세요. 추가 텍스트를 생성하거나 반복하면 안 됩니다.
명령: 반드시 최종 결과물은 <result> 태그와 </result> 태그 사이에만 작성하시오. 서론, 결론, 마크다운 기호 등 다른 어떤 말도 절대 덧붙이지 마시오.
"""


def tag_paragraph_with_gemma(text, model="gemma4:26b", timeout=60, force_fallback=False):
    # If ollama client is not installed, fall back to returning the original text
    # If ollama client is not installed, try hanja-based fallback so the script
    # still populates the TEI_tagged column with useful glosses for 한자/일문.
    if force_fallback:
        return add_hanja_gloss_to_text(text)

    if ollama is None:
        return add_hanja_gloss_to_text(text)

    # Build chat-style messages: system prompt (instructions) + user content (the exact text to tag)
    messages = [
        {'role': 'system', 'content': PROMPT_TEMPLATE},
        {'role': 'user', 'content': text.strip()}
    ]
    # Use signal-based timeout to avoid infinite blocking LLM calls (works on Unix/macOS main thread)
    def _timeout_handler(signum, frame):
        raise TimeoutError('ollama.generate timeout')

    old_handler = signal.getsignal(signal.SIGALRM)
    try:
        t0 = time.monotonic()
        signal.signal(signal.SIGALRM, _timeout_handler)
        # timeout is seconds; schedule alarm
        if timeout and timeout > 0:
            signal.alarm(int(timeout))
        # 환각 방지를 위해 생성 파라미터를 강제합니다: 온도/탑피 낮춤, 출력 길이 상한 및 반복 패널티 설정
        chat_options = {'temperature': 0.0, 'top_p': 0.1, 'num_predict': 1500, 'repeat_penalty': 1.5}
        # prefer chat() API for clearer system/user separation
        if hasattr(ollama, 'chat'):
            res = ollama.chat(model=model, messages=messages, options=chat_options)
        else:
            # backward compatibility: if chat not available, fall back to generate with prompt concatenation
            prompt = PROMPT_TEMPLATE + "\n---\n" + text.strip()
            res = ollama.generate(model=model, prompt=prompt, options=chat_options)
        # cancel alarm on success
        signal.alarm(0)
        t1 = time.monotonic()
    except TimeoutError as e:
        # restore handler
        signal.signal(signal.SIGALRM, old_handler)
        print(f"경고: LLM 호출 타임아웃({timeout}s) — 폴백으로 원문 기반 처리 사용합니다.")
        return add_hanja_gloss_to_text(text)
    except Exception as e:
        # on failure return original text to avoid dropping content
        # cancel any pending alarm and restore handler
        try:
            signal.alarm(0)
        except Exception:
            pass
        signal.signal(signal.SIGALRM, old_handler)
        print(f"경고: LLM 호출 실패: {e}")
        # attempt hanja-based fallback tagging
        return add_hanja_gloss_to_text(text)
    finally:
        try:
            signal.signal(signal.SIGALRM, old_handler)
        except Exception:
            pass

    # extract textual content from various possible ollama.chat/generate return shapes
    def _extract_text_from_response(r):
        if r is None:
            return ''
        if isinstance(r, str):
            return r
        if isinstance(r, dict):
            # older generate returned {'response': '...'}
            if 'response' in r and isinstance(r['response'], str):
                return r['response']
            # chat-like: choices -> [{ 'message': { 'role': 'assistant', 'content': '...' } }]
            if 'choices' in r and isinstance(r['choices'], (list, tuple)) and r['choices']:
                first = r['choices'][0]
                if isinstance(first, dict):
                    if 'message' in first and isinstance(first['message'], dict) and 'content' in first['message']:
                        return first['message']['content']
                    if 'content' in first and isinstance(first['content'], str):
                        return first['content']
            # some clients return {'content': '...'} or similar
            if 'content' in r and isinstance(r['content'], str):
                return r['content']
            # last resort: stringify
            return str(r)
        # object-like with text attribute
        if hasattr(r, 'text'):
            return str(r.text)
        return str(r)

    out = _extract_text_from_response(res)

    # strip surrounding whitespace and ensure a string
    out_str = out.strip()

    # Post-processing: extract only content inside <result> ... </result>
    result_match = re.search(r'<result>(.*?)</result>', out_str, re.DOTALL)
    if result_match:
        out_str = result_match.group(1)
    else:
        # fallback: remove markdown code fences if present
        out_str = re.sub(r'```(?:xml)?\n?', '', out_str)
        out_str = out_str.replace('```', '')

    out_str = out_str.strip()

    # If the model didn't include any <gloss> tags, try hanja-based fallback
    if '<gloss>' not in out_str:
        try:
            fb = add_hanja_gloss_to_text(out_str)
            return fb
        except Exception:
            return out_str
    return out_str


def _hanja_translate(token: str) -> Optional[str]:
    """Try to translate/convert a hanja token to Korean reading using
    the `hanja` library when available. Returns None if not possible."""
    try:
        hanja = importlib.import_module('hanja')
    except Exception:
        return None

    # try common API patterns for hanja-like libraries
    try:
        if hasattr(hanja, 'translate'):
            # many hanja libs provide translate(text, 'substitution')
            try:
                val = hanja.translate(token, 'substitution')
                if val and isinstance(val, str):
                    return val
            except Exception:
                pass
        if hasattr(hanja, 'hangulize'):
            try:
                val = hanja.hangulize(token)
                if val and isinstance(val, str):
                    return val
            except Exception:
                pass
    except Exception:
        return None

    return None


# small fallback mapping for common hanja -> 한글 음
HANJA_TO_HANGUL = {
    '明': '명', '治': '치', '光': '광', '州': '주', '海': '해', '子': '자', '浦': '포',
    '德': '덕', '沼': '소', '里': '리', '憲': '헌', '兵': '병', '駐': '주', '在': '재', '所': '소',
    '暴': '폭', '民': '민', '來': '래', '襲': '습', '發': '발', '砲': '포', '下': '하', '通': '통',
    '長': '장', '津': '진', '咸': '함', '興': '흥', '京': '경', '畿': '기', '道': '도', '郡': '군',
    '義': '의', '山': '산', '邑': '읍', '警': '경', '察': '찰', '官': '관', '駐': '주', '在': '재',
    '所': '소', '德': '덕', '沼': '소', '里': '리', '維': '유', '鳩': '구', '木': '목', '川': '천',
}


def _hanja_char_by_char(token: str) -> Optional[str]:
    """Best-effort per-character mapping using HANJA_TO_HANGUL.
    Returns concatenated reading if sufficient characters map, else None.
    """
    if not token:
        return None
    readings = []
    mapped_count = 0
    for ch in token:
        if ch in HANJA_TO_HANGUL:
            readings.append(HANJA_TO_HANGUL[ch])
            mapped_count += 1
        else:
            # if it's ASCII or punctuation, keep as-is
            if ord(ch) < 128:
                readings.append(ch)
            else:
                readings.append('')

    # require at least one mapped CJK char to consider successful
    if mapped_count == 0:
        return None

    return ''.join(readings)


def add_hanja_gloss_to_text(text: str) -> str:
    """Add <gloss>한글독음</gloss> immediately after hanja/japanese character runs
    when no gloss exists yet. Returns modified text.
    This is a best-effort fallback and intentionally conservative.
    """
    if not text:
        return text

    # skip if gloss already present anywhere
    if '<gloss>' in text:
        return text

    # simple CJK range matcher
    cjk_re = re.compile(r'([\u4E00-\u9FFF]+)')

    def repl(m):
        tok = m.group(1)
        # try to get reading
        reading = _hanja_translate(tok)
        if not reading:
            # try per-character mapping as a secondary fallback
            reading = _hanja_char_by_char(tok)
        if reading:
            return f"{tok}<gloss>{reading}</gloss>"
        else:
            return f"{tok}<gloss>미상</gloss>"

    return cjk_re.sub(repl, text)


def process_db(table_name, limit=5, model='gemma4:26b', force_fallback=False):
    if not psycopg2:
        print('psycopg2 not available; cannot connect to DB')
        sys.exit(1)

    conn = psycopg2.connect(host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE)
    conn.autocommit = True
    cur = conn.cursor()

    # ensure tei_status column exists
    try:
        cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS tei_status text')
    except Exception as e:
        print(f'⚠️ tei_status 컬럼 생성 실패: {e}')
        cur.close()
        conn.close()
        sys.exit(1)

    limit_sql = f' LIMIT {int(limit)}' if limit and int(limit) > 0 else ''
    cur.execute(
        f'SELECT rowid, tei FROM "{table_name}" '
        'WHERE tei IS NOT NULL AND (tei_status IS NULL OR tei_status != \"REFINED\") '
        'ORDER BY rowid' + limit_sql
    )
    rows = cur.fetchall()

    pattern = re.compile(r'(<p[^>]*data-col="출처정보"[^>]*>)(.*?)(</p>)', re.IGNORECASE | re.DOTALL)

    for i, (rowid, tei) in enumerate(rows, 1):
        tei = tei or ''
        # normalize escaped quotes inside TEI fields
        if 'data-col=""' in tei:
            tei = tei.replace('data-col=""', 'data-col="')
        if not tei:
            continue

        def repl(m):
            open_tag = m.group(1)
            inner = m.group(2)
            close_tag = m.group(3)
            try:
                tagged_inner = tag_paragraph_with_gemma(inner, model=model, force_fallback=force_fallback)
            except Exception as e:
                print(f"경고: LLM 태깅 실패(rowid={rowid}): {e}")
                tagged_inner = inner
            return f"{open_tag}{tagged_inner}{close_tag}"

        new_tei, nsubs = pattern.subn(repl, tei)
        tei_len = len(tei)
        new_tei_len = len(new_tei)

        if i <= 5:
            print(f"[debug] row={i} rowid={rowid} nsubs={nsubs} tei_len={tei_len} new_tei_len={new_tei_len}")

        if tei_len > 0 and new_tei_len > tei_len * 2.0:
            print('⚠️ 환각 감지됨: 텍스트 폭주 — 반환값을 무시하고 원본 TEI로 롤백합니다.')
            new_tei = tei
            nsubs = 0

        if nsubs > 0:
            cur.execute(
                f'UPDATE "{table_name}" SET tei = %s, tei_status = %s WHERE rowid = %s',
                (new_tei, 'REFINED', rowid)
            )
        else:
            cur.execute(
                f'UPDATE "{table_name}" SET tei_status = %s WHERE rowid = %s',
                ('REFINED', rowid)
            )

    cur.close()
    conn.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--table', default='raw_source_info')
    p.add_argument('--limit', type=int, default=5, help='처리할 최대 행 수 (기본: 5)')
    p.add_argument('--model', default='gemma4:26b')
    p.add_argument('--force-fallback', action='store_true', help='LLM 호출을 건너뛰고 hanja fallback을 사용합니다.')
    args = p.parse_args()

    try:
        process_db(
            args.table,
            limit=args.limit,
            model=args.model,
            force_fallback=args.force_fallback,
        )
    except Exception as e:
        print(f"오류: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
