"""Tag TEI <p data-col="출처정보"> content using local Gemma model (gemma4:26b).

Usage:
  python scripts/tag_tei_with_gemma.py \
    --input data/data-1778569591261.csv \
    --output data/data-1778569591261.tagged.csv \
    --col TEI

The script looks for <p data-col="출처정보">...</p> fragments inside the TEI column
and sends the inner content to the local `gemma4:26b` model (via `ollama.generate`) with
an instruction to add the following TEI tags where appropriate:

- <placeName> : 역사적 지명
- <persName>  : 인명
- <orgName>   : 조직/기관
- <date when="..."> : 날짜 (ISO-ish when 속성 사용 권장)
- <term>      : 특정 지위나 용어
- <gloss>     : 한자/일본어 표현의 병기 (한글 독음/의미)

The original paragraph wrapper and other TEI should be preserved. The script writes
a new CSV with an added column `TEI_tagged` (or replaces the original if --replace).
"""

import argparse
import csv
import os
import re
import sys
import time
import importlib
from typing import Optional

try:
    import ollama
except Exception:
    ollama = None


PROMPT_TEMPLATE = """
아래는 TEI 문단의 내용입니다. 당신은 역사 도슨트이자 XML 태깅 도우미로서, 주어진 문단 안에서 다음 역할을 수행하세요.

요구사항(강제):
1) 원문 텍스트의 의미나 어순은 변경하지 마세요. 가능한 한 최소 변경으로 엔티티 주변에 지정된 TEI 태그만 추가합니다.
2) 이미 해당 XML 태그(예: <placeName>, <persName>, <gloss> 등)가 존재하면 중복으로 추가하지 마세요.
3) 태그 목록과 활용법(반드시 적용):
    - <placeName> : 역사적 지명(도시, 마을, 지역 등)을 감싸세요.
    - <persName>  : 개인 이름을 감싸세요.
    - <orgName>   : 조직/기관 이름을 감싸세요.
    - <date when="YYYY-MM-DD">...</date> : 날짜는 가능하면 ISO(YYYY-MM 또는 YYYY-MM-DD) 형태로 `when` 속성에 표기하세요.
    - <term>      : 직위·지위·특정 용어(예: 폭민, 하사)를 감싸세요.
    - <gloss>     : 한자(漢字)와 일본어(仮名/漢字) 표기에 대해 **반드시** 한국어 독음(한글)과 가능하면 의미를 병기하세요. 형식은 아래 예시를 따르세요.

gloss 사용 규칙(반드시 따를 것):
- 원문에 한자/일문이 등장하면, 해당 토큰 뒤에 `<gloss>...</gloss>` 를 추가합니다.
- `<gloss>` 안에는 '한글독음' 또는 '한글독음: 의미' 형태로 작성합니다.
- 원문에 이미 괄호·대괄호 등으로 독음/설명이 주어져 있다면 그 정보를 우선 사용하세요.

예시 (반드시 이 포맷으로 출력):
 - 원문: 光州市內
    출력: `<placeName>光州</placeName><gloss>광주</gloss>市內`
 - 원문: 海州カイシホ[海州는 海子浦의 오기]
    출력: `<placeName>海州カイシホ</placeName><gloss>해주(해자포의 오기)</gloss>`

4) 출력은 오로지 태깅된 XML 단편(원문에서 태그만 추가된 형태)이어야 합니다. 설명 문구나 메타 출력 금지.
5) 불확실한 독음은 최선의 추정 한글 표기를 제공하되, 전혀 알 수 없으면 `<gloss>미상</gloss>`로 표기하세요.

예시 입력 → 출력 예시:
입력: "헌병주재소에서 김영수(하사)는 1919년 3월에 조사했다."
출력: "<orgName>헌병주재소</orgName>에서 <persName>김영수</persName>(<term>하사</term>)는 <date when=\"1919-03\">1919년 3월</date>에 조사했다."

이제 태깅 대상 텍스트를 아래에 넣었습니다. 태그된 단편만 응답하세요.
---
{text}
"""


def tag_paragraph_with_gemma(text, model="gemma4:26b", timeout=60, force_fallback=False):
    # If ollama client is not installed, fall back to returning the original text
    # If ollama client is not installed, try hanja-based fallback so the script
    # still populates the TEI_tagged column with useful glosses for 한자/일문.
    if force_fallback:
        return add_hanja_gloss_to_text(text)

    if ollama is None:
        return add_hanja_gloss_to_text(text)

    prompt = PROMPT_TEMPLATE.format(text=text.strip())
    try:
        t0 = time.monotonic()
        res = ollama.generate(model=model, prompt=prompt)
        t1 = time.monotonic()
    except Exception as e:
        # on failure return original text to avoid dropping content
        print(f"경고: LLM 호출 실패: {e}")
        # attempt hanja-based fallback tagging
        return add_hanja_gloss_to_text(text)

    # app.py 사용 방식과 호환: return value may be dict with 'response'
    out = None
    if isinstance(res, dict) and 'response' in res:
        out = res['response']
    else:
        out = str(res)

    # strip surrounding whitespace and ensure a string
    out_str = out.strip()
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


def process_csv(input_path, output_path, tei_col='TEI', replace=False, model='gemma4:26b', force_fallback=False):
    if not os.path.exists(input_path):
        print(f"입력 파일이 존재하지 않습니다: {input_path}")
        sys.exit(1)

    out_rows = []
    with open(input_path, 'r', encoding='utf-8-sig', newline='') as inf:
        reader = csv.DictReader(inf)
        fieldnames = list(reader.fieldnames or [])
        # find actual TEI column case-insensitively
        real_tei_col = None
        for fn in fieldnames:
            if fn.lower() == tei_col.lower():
                real_tei_col = fn
                break
        if real_tei_col is None:
            print(f"지정된 TEI 컬럼을 찾을 수 없습니다(대소문자 구분 없음): {tei_col}")
            sys.exit(1)

        add_col = 'TEI_tagged'
        if replace:
            out_fieldnames = fieldnames
        else:
            out_fieldnames = fieldnames + [add_col]

        i = 0
        for row in reader:
            i += 1
            tei = row.get(real_tei_col) or ''
            # normalize CSV-escaped quotes inside TEI fields (e.g. data-col=""출처정보""
            # produced by CSV quoting) to a single-quote form for regex matching
            if 'data-col=""' in tei:
                tei = tei.replace('data-col=""', 'data-col="')
            if not tei:
                if not replace:
                    row[add_col] = ''
                out_rows.append(row)
                continue

            # find all <p ... data-col="출처정보"> ... </p>
            def repl(m):
                open_tag = m.group(1)
                inner = m.group(2)
                close_tag = m.group(3)
                try:
                    tagged_inner = tag_paragraph_with_gemma(inner, model=model, force_fallback=force_fallback)
                except Exception as e:
                    print(f"경고: LLM 태깅 실패: {e}")
                    tagged_inner = inner
                return f"{open_tag}{tagged_inner}{close_tag}"

            pattern = re.compile(r'(<p[^>]*data-col="출처정보"[^>]*>)(.*?)(</p>)', re.IGNORECASE | re.DOTALL)
            new_tei, nsubs = pattern.subn(repl, tei)
            # debug: show first few processed rows
            if i <= 5:
                print(f"[debug] row={i} nsubs={nsubs} tei_len={len(tei)} new_tei_len={len(new_tei)}")
            if nsubs == 0:
                # no matching paragraph — leave as-is
                if replace:
                    row[real_tei_col] = tei
                else:
                    row[add_col] = tei
            else:
                if replace:
                    row[real_tei_col] = new_tei
                else:
                    row[add_col] = new_tei

            out_rows.append(row)

    # write output
    with open(output_path, 'w', encoding='utf-8', newline='') as outf:
        writer = csv.DictWriter(outf, fieldnames=out_fieldnames)
        writer.writeheader()
        for r in out_rows:
            writer.writerow(r)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', '-i', required=True)
    p.add_argument('--output', '-o', required=True)
    p.add_argument('--col', default='TEI')
    p.add_argument('--replace', action='store_true', help='원본 TEI 컬럼을 교체합니다.')
    p.add_argument('--model', default='gemma4:26b')
    p.add_argument('--force-fallback', action='store_true', help='LLM 호출을 건너뛰고 hanja fallback을 사용합니다.')
    args = p.parse_args()

    try:
        process_csv(args.input, args.output, tei_col=args.col, replace=args.replace, model=args.model, force_fallback=args.force_fallback)
    except Exception as e:
        print(f"오류: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
