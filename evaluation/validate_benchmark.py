#!/usr/bin/env python3
"""Validate benchmark questions before they are used (mainly the template-generated ones).

Input:  a benchmark JSON (list of questions).
Output (under --out-dir, default evaluation/validation/):
  <name>_validation.xlsx          per-question PASS/FAIL with reasons, the josa fixes,
                                  and a human-review sheet (random 30% of passed items)
  <name>_validation_summary.json  counts by status / source / category / reason
  <name>_validated.json           passed (and not-checked) questions with the josa fixes applied

A question FAILS when
  - a person name is shorter than 2 characters, a romanised fragment ("Sun"),
    or a title / common noun (NON_NAME_TOKENS)
  - the event name and date written in the question do not agree with raw_event_info,
    with the date in the event name itself, or with the period the question states
  - a trap statement is true (the trap person is a recorded participant of that event)
  - a trap statement contradicts itself (the same place on both sides of a comparison)
  - the question duplicates an earlier one
Wrong particles after a name ("김상열가") are not a failure: they are corrected
and every correction is listed.

The text rules here (name checks, particles, generic tokens) are also used by
generate_benchmark_dataset.py and by the trap heuristic in eval_rag_comparison.py.
The original dataset file is never modified.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable

EVAL_DIR = Path(__file__).resolve().parent
ROOT = EVAL_DIR.parent
for _path in (str(ROOT), str(EVAL_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

OUT_DIR = EVAL_DIR / "validation"
STATUS_PASS, STATUS_FAIL, STATUS_SKIP = "PASS", "FAIL", "SKIP"

# ─────────────────────────────────────────────────────────────────────────────
# Text rules (shared)
# ─────────────────────────────────────────────────────────────────────────────
NON_NAME_TOKENS = {'목사', '장로', '전도사', '선교사', '교사', '학생', '면장', '구장', '군수', '순사', '헌병', '의사', '기자', '승려', '농민', '주민'}
PERSON_NAME_RE = re.compile(r"[가-힣]{2,4}")
PROVINCES = {
    "경기도", "강원도", "충청북도", "충청남도", "전라북도", "전라남도", "경상북도", "경상남도",
    "황해도", "평안북도", "평안남도", "함경북도", "함경남도",
    "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "황해", "평북", "평남", "함북", "함남",
}
# Cities and regions that recur across the corpus, written without their unit suffix (not exhaustive).
COMMON_PLACES = {
    "경성", "서울", "한성", "평양", "함흥", "원산", "개성", "인천", "수원", "천안", "공주", "대전", "청주", "전주", "군산",
    "광주", "목포", "대구", "부산", "마산", "진주", "해주", "안악", "의주", "신의주", "선천", "정주", "진남포", "성진",
    "청진", "회령", "춘천", "강릉", "제주", "만주", "간도", "북간도", "연해주", "상하이", "상해", "베이징", "북경",
    "도쿄", "동경", "일본", "조선", "한국", "미국", "중국", "러시아",
}
# Province / county level units: "평안남도", "전주군", "평양부", "성수면". "임시정부" etc. are proper nouns.
ADMIN_UNIT_RE = re.compile(r"[가-힣]{1,4}(?:도|군|면|읍)|[가-힣]{2,3}(?<!정)(?<!독)(?<!본)(?<!지)(?<!간)(?<!학)부")
DATE_TOKEN_RE = re.compile(r"\d+(?:년|월|일)?")
JOSA_PAIRS = [("이", "가"), ("은", "는"), ("을", "를"), ("과", "와")]  # (after a final consonant, after a vowel)
_STRIP_JOSA = ("에서는", "에서", "으로", "까지", "보다", "에게", "이", "가", "은", "는", "을", "를", "의", "에", "와", "과")


def has_batchim(word: str) -> bool | None:
    """True/False for a word ending in a Hangul syllable with/without a final consonant, else None."""
    last = word[-1:]
    if not ("가" <= last <= "힣"):
        return None
    return (ord(last) - 0xAC00) % 28 != 0


def with_josa(word: str, after_consonant: str = "이", after_vowel: str = "가") -> str:
    """Attach the particle that agrees with the word's final sound: 김상열이 / 김구가."""
    batchim = has_batchim(word)
    if batchim is None:
        return word + after_vowel
    return word + (after_consonant if batchim else after_vowel)


def strip_josa(token: str) -> str:
    """Drop one trailing particle so '이동휘가' can be matched as '이동휘'."""
    for josa in _STRIP_JOSA:
        if token.endswith(josa) and len(token) - len(josa) >= 2:
            return token[: -len(josa)]
    return token


def is_date_token(token: str) -> bool:
    return bool(DATE_TOKEN_RE.fullmatch(token))


def is_generic_token(token: str) -> bool:
    """Years, dates, and province/county/city-level place names: too common to identify a claim."""
    return is_date_token(token) or token in PROVINCES or token in COMMON_PLACES or bool(ADMIN_UNIT_RE.fullmatch(token))


def name_problem(name: str) -> str:
    """Reason a string cannot be used as a person name ('' when it is acceptable)."""
    name = name.strip()
    if len(name) < 2:
        return "2글자 미만"
    if re.search(r"[A-Za-z]", name):
        return "로마자 조각"
    if name in NON_NAME_TOKENS:
        return "직책·일반명사"
    if not PERSON_NAME_RE.fullmatch(name):
        return "인명 형식 아님"
    return ""


def is_valid_person_name(name: str) -> bool:
    return not name_problem(name)


def parse_person_list(raw: Any, clean: Callable[[str], str] = lambda s: s) -> list[str]:
    """Person names from a raw_event_info '관련인물' cell.

    Entries are separated by ';'. Roles and romanisations in brackets
    ("김선두(Kim Sun Doo)", "강규찬(… 목사)") are removed before the entry is
    split, so their pieces cannot be mistaken for names.
    """
    persons: list[str] = []
    for entry in re.split(r"[;\n]+", str(raw or "")):
        entry = re.sub(r"\([^)]*\)|\[[^\]]*\]", " ", entry)
        for token in re.split(r"[,\s]+", entry):
            name = clean(token)
            if is_valid_person_name(name) and name not in persons:
                persons.append(name)
    return persons


# ─────────────────────────────────────────────────────────────────────────────
# Reading a question
# ─────────────────────────────────────────────────────────────────────────────
NAME_LIST_RE = re.compile(r"([^\s,]+(?:, [^\s,]+)*) 등(?:이|의|에)")
TRAP_SUBJECT_RE = re.compile(r"^(\S+?)(?:가|이)\s")
ISO_DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
TITLE_DATE_RE = re.compile(r"(\d{1,2})월\s*(\d{1,2})일")
COMPARISON_RE = re.compile(r"^(.+?)(?:가|이)\s+(.+?)보다")
TEXT_FIELDS = ("query", "gold_facts", "ground_truth", "negative_traps")


def _norm_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text)).strip()


def _norm_key(text: str) -> str:
    return re.sub(r"[\s\W_]+", "", str(text))


def _facts(item: dict[str, Any]) -> list[str]:
    return [*item.get("gold_facts", []), *item.get("ground_truth", [])]


def event_refs(query: str) -> list[dict[str, str]]:
    """Events named in the question as 'title', each with the date the question gives for it."""
    titles = re.findall(r"'([^']+)'", query)
    dates = ["-".join(d) for d in ISO_DATE_RE.findall(query)]
    refs = []
    for title in titles:
        attached = re.search(re.escape(f"'{title}'") + r"\((\d{4}-\d{2}-\d{2})\)", query)
        date = attached.group(1) if attached else (dates[0] if len(titles) == 1 and len(dates) == 1 else "")
        refs.append({"title": title, "date": date})
    return refs


def name_candidates(item: dict[str, Any]) -> list[tuple[str, str]]:
    """(name, where it was found) for every string a template question uses as a person name."""
    found: list[tuple[str, str]] = []
    for fact in _facts(item):
        for group in NAME_LIST_RE.findall(fact):
            found += [(name, "gold_facts") for name in group.split(", ")]
    for trap in item.get("negative_traps", []):
        subject = TRAP_SUBJECT_RE.match(trap)
        if subject:
            found.append((subject.group(1), "negative_traps"))
    for entity in item.get("target_entities", []):
        if re.fullmatch(r"[A-Za-z]+", str(entity).strip()):
            found.append((str(entity).strip(), "target_entities"))
    return list(dict.fromkeys(found))


def fix_josa(item: dict[str, Any], words: list[str]) -> tuple[dict[str, Any], list[dict[str, str]]]:
    """Correct particles that disagree with the final sound of a known name or event title."""
    fixed = json.loads(json.dumps(item, ensure_ascii=False))
    changes: list[dict[str, str]] = []
    rules = []
    for word in sorted(set(words), key=len, reverse=True):
        batchim = has_batchim(word)
        if batchim is None:
            continue
        for after_consonant, after_vowel in JOSA_PAIRS:
            wrong, right = (after_vowel, after_consonant) if batchim else (after_consonant, after_vowel)
            rules.append((re.compile(r"(?<![가-힣])" + re.escape(word + wrong) + r"(?=[\s,.?]|$)"), word + wrong, word + right))

    def repair(text: str, field: str) -> str:
        for pattern, before, after in rules:
            text, n = pattern.subn(after, text)
            if n:
                changes.append({"field": field, "before": before, "after": after})
        return text

    for field in TEXT_FIELDS:
        value = fixed.get(field)
        if isinstance(value, str):
            fixed[field] = repair(value, field)
        elif isinstance(value, list):
            fixed[field] = [repair(v, field) if isinstance(v, str) else v for v in value]
    return fixed, changes


# ─────────────────────────────────────────────────────────────────────────────
# raw_event_info
# ─────────────────────────────────────────────────────────────────────────────
def load_event_index() -> dict[str, list[dict[str, Any]]]:
    """title -> [{date, place, persons}] from raw_event_info (read-only)."""
    from scripts.config import get_pg_connection
    from scripts.hanja_utils import translate_hanja_name

    def hangul(text: str) -> str:
        return re.sub(r"[一-鿿]+", lambda m: translate_hanja_name(m.group(0)), text)

    def clean(token: str) -> str:
        return re.sub(r"[^\w가-힣]", "", hangul(token)).strip()

    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute('SELECT "사건명", "시위_시작일자", "시위_행정구역명", "관련인물" FROM raw_event_info WHERE "사건명" IS NOT NULL')
    index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for title, start, place, persons in cur.fetchall():
        index[_norm_space(title)].append({
            "date": str(start) if start else "",
            "place": _norm_space(hangul(str(place or ""))),
            "persons": parse_person_list(persons, clean),
        })
    cur.close()
    conn.close()
    return dict(index)


# ─────────────────────────────────────────────────────────────────────────────
# Checks
# ─────────────────────────────────────────────────────────────────────────────
def check_names(item: dict[str, Any]) -> list[str]:
    reasons = []
    for name, where in name_candidates(item):
        problem = name_problem(name)
        if problem:
            reasons.append(f"인명 오류({problem}): '{name}' [{where}]")
    return reasons


def check_event_dates(item: dict[str, Any], events: dict[str, list[dict[str, Any]]] | None) -> list[str]:
    reasons = []
    query = item.get("query", "")
    stated_period = re.search(r"1919년\s*(\d{1,2})월", query)
    for ref in event_refs(query):
        title, date = ref["title"], ref["date"]
        if events is not None:
            rows = events.get(_norm_space(title))
            if not rows:
                reasons.append(f"사건명이 raw_event_info에 없음: '{title}'")
                continue
            if date and date not in {r["date"] for r in rows}:
                recorded = ", ".join(sorted({r["date"] or "날짜 없음" for r in rows}))
                reasons.append(f"질문의 날짜({date})가 raw_event_info({recorded})와 다름: '{title}'")
        if not date:
            continue
        month, day = int(date[5:7]), int(date[8:10])
        in_title = [(int(m), int(d)) for m, d in TITLE_DATE_RE.findall(title)]
        if in_title:
            low, high = min(in_title), max(in_title)
            slack = 1 if "전후" in title else 0
            if not ((low[0], low[1] - slack) <= (month, day) <= (high[0], high[1] + slack)):
                reasons.append(f"사건명의 날짜({low[0]}월 {low[1]}일)와 질문의 날짜({date})가 다름: '{title}'")
        if stated_period and int(stated_period.group(1)) != month:
            reasons.append(f"질문이 밝힌 시기(1919년 {stated_period.group(1)}월)와 사건 날짜({date})가 다름: '{title}'")
    return reasons


def _participants(item: dict[str, Any], ref: dict[str, str], events: dict[str, list[dict[str, Any]]] | None) -> set[str]:
    """Recorded participants of an event: raw_event_info when available, plus the question's own gold facts."""
    people: set[str] = set()
    for row in (events or {}).get(_norm_space(ref["title"]), []):
        people.update(row["persons"])
    for fact in _facts(item):
        if ref["title"] in fact or (ref["date"] and ref["date"] in fact):
            for group in NAME_LIST_RE.findall(fact):
                people.update(group.split(", "))
    return people


def check_traps(item: dict[str, Any], events: dict[str, list[dict[str, Any]]] | None) -> list[str]:
    reasons = []
    refs = event_refs(item.get("query", ""))
    for trap in item.get("negative_traps", []):
        comparison = COMPARISON_RE.match(trap)
        if comparison:
            left, right = (_norm_key(re.sub(r"\s*(?:시위|사건)$", "", side)) for side in comparison.groups())
            if left and left == right:
                reasons.append(f"함정 자기모순(비교 양쪽이 같은 지명): '{trap}'")
                continue
        subject = TRAP_SUBJECT_RE.match(trap)
        if not subject:
            continue
        person = subject.group(1)
        targets = [r for r in refs if r["title"] in trap or (r["date"] and r["date"] in trap)]
        if not targets and events is not None:
            targets = [r for r in refs
                       if any(row["place"] and row["place"] in _norm_space(trap) for row in events.get(_norm_space(r["title"]), []))]
        if not targets and len(refs) == 1:
            targets = refs
        for ref in targets:
            if person in _participants(item, ref, events):
                reasons.append(f"함정이 사실일 수 있음('{person}'이(가) '{ref['title']}' 참여자 목록에 있음): '{trap}'")
                break
    return reasons


def duplicate_key(item: dict[str, Any]) -> tuple[Any, ...] | None:
    """Same category, same set of events, same leading subject = the same question asked again."""
    query = item.get("query", "")
    titles = frozenset(_norm_space(r["title"]) for r in event_refs(query))
    if not titles:
        return None
    return (item.get("category", ""), titles, query.split("'")[0].split()[0] if query.split("'")[0].split() else "")


def validate_items(
    items: list[dict[str, Any]],
    events: dict[str, list[dict[str, Any]]] | None,
    sources: set[str],
) -> list[dict[str, Any]]:
    """One result per question: status, reasons, josa fixes, and the corrected question."""
    results = []
    seen_queries: dict[str, str] = {}
    seen_keys: dict[tuple[Any, ...], str] = {}
    for item in items:
        source = item.get("source") or "unknown"
        reasons: list[str] = []
        query_key = _norm_key(item.get("query", ""))
        key = duplicate_key(item)
        if query_key in seen_queries:
            reasons.append(f"질문 중복: {seen_queries[query_key]}와 같은 질문")
        elif key is not None and key in seen_keys:
            reasons.append(f"질문 중복: {seen_keys[key]}와 같은 사건 조합")
        seen_queries.setdefault(query_key, item.get("id", ""))
        if key is not None:
            seen_keys.setdefault(key, item.get("id", ""))

        checked = source in sources or source == "unknown"
        fixed, fixes = item, []
        if checked:
            reasons += check_names(item)
            reasons += check_event_dates(item, events)
            reasons += check_traps(item, events)
            words = [name for name, _ in name_candidates(item)] + [r["title"] for r in event_refs(item.get("query", ""))]
            words += [e for e in item.get("target_entities", []) if is_valid_person_name(str(e)) and not is_generic_token(str(e))]
            fixed, fixes = fix_josa(item, words)

        status = STATUS_FAIL if reasons else (STATUS_PASS if checked else STATUS_SKIP)
        results.append({"item": item, "fixed": fixed, "source": source, "status": status, "reasons": reasons, "fixes": fixes})
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Output
# ─────────────────────────────────────────────────────────────────────────────
INSTRUCTIONS = [
    "벤치마크 문항 자동 검증 결과",
    "",
    "[results] 문항별 판정. PASS=규칙 통과, FAIL=reasons의 사유로 탈락, SKIP=검증 대상 출처가 아님(중복만 확인).",
    "[fixes] 자동으로 고친 조사(이/가, 은/는, 을/를, 과/와). 수정된 문장은 *_validated.json 에 반영되어 있습니다.",
    "[human_review] 통과 문항 중 무작위 표본입니다. 질문·정답·함정이 사료와 맞는지 확인하고 verdict에 O 또는 X를 기입해 주십시오.",
    "  - 자동 검증은 형식과 raw_event_info 일치만 확인합니다. 정답 사실(gold_facts)의 역사적 정확성은 사람이 확인해야 합니다.",
]


def validate_dataset(
    dataset_path: Path,
    out_dir: Path = OUT_DIR,
    sources: set[str] | None = None,
    seed: int = 42,
    review_fraction: float = 0.3,
    use_db: bool = True,
) -> dict[str, Any]:
    from annotation_io import add_sheet, new_workbook

    items = json.loads(dataset_path.read_text(encoding="utf-8"))
    if not isinstance(items, list):
        raise ValueError(f"{dataset_path}: 최상위 구조는 문항 리스트여야 합니다.")

    events, db_error = None, ""
    if use_db:
        try:
            events = load_event_index()
        except Exception as exc:  # the offline rules still run without the database
            db_error = str(exc)[:200]
            print(f"⚠️ raw_event_info를 읽지 못해 DB 대조 규칙을 건너뜁니다: {db_error}")

    results = validate_items(items, events, sources or {"template"})
    passed = [r for r in results if r["status"] == STATUS_PASS]
    rng = random.Random(seed)
    n_review = min(len(passed), math.ceil(len(passed) * review_fraction))
    review_ids = set(rng.sample(sorted(r["item"]["id"] for r in passed), n_review))

    rows = [{
        "id": r["item"].get("id", ""), "source": r["source"], "category": r["item"].get("category", ""),
        "status": r["status"], "reasons": "\n".join(r["reasons"]),
        "josa_fixes": "\n".join(f"{f['before']} → {f['after']}" for f in r["fixes"]),
        "query": r["fixed"].get("query", ""),
    } for r in results]
    fix_rows = [{"id": r["item"].get("id", ""), **f} for r in results for f in r["fixes"]]
    review_rows = [{
        "id": r["item"]["id"], "source": r["source"], "category": r["item"].get("category", ""),
        "query": r["fixed"].get("query", ""),
        "gold_facts": "\n".join(r["fixed"].get("gold_facts", [])),
        "negative_traps": "\n".join(r["fixed"].get("negative_traps", [])),
    } for r in passed if r["item"]["id"] in review_ids]

    out_dir.mkdir(parents=True, exist_ok=True)
    stem = dataset_path.stem
    xlsx_path = out_dir / f"{stem}_validation.xlsx"
    summary_path = out_dir / f"{stem}_validation_summary.json"
    validated_path = out_dir / f"{stem}_validated.json"

    wb = new_workbook(INSTRUCTIONS)
    add_sheet(wb, "results", ["id", "source", "category", "status", "reasons", "josa_fixes", "query"], rows,
              widths={"id": 12, "category": 26, "status": 8, "reasons": 80, "josa_fixes": 40, "query": 80})
    add_sheet(wb, "fixes", ["id", "field", "before", "after"], fix_rows, widths={"id": 12, "before": 40, "after": 40})
    add_sheet(wb, "human_review", ["id", "source", "category", "query", "gold_facts", "negative_traps", "verdict", "note"], review_rows,
              widths={"id": 12, "category": 26, "query": 70, "gold_facts": 80, "negative_traps": 70, "note": 30},
              choices={"verdict": ["O", "X"]})
    wb.save(xlsx_path)

    kept = [r["fixed"] for r in results if r["status"] != STATUS_FAIL]
    validated_path.write_text(json.dumps(kept, ensure_ascii=False, indent=2), encoding="utf-8")

    def tally(field: Callable[[dict[str, Any]], str]) -> dict[str, dict[str, int]]:
        table: dict[str, Counter] = defaultdict(Counter)
        for r in results:
            table[field(r)][r["status"]] += 1
        return {k: dict(v) for k, v in sorted(table.items())}

    summary = {
        "dataset": dataset_path.name,
        "n_items": len(items),
        "checked_sources": sorted(sources or {"template"}),
        "status": dict(Counter(r["status"] for r in results)),
        "by_source": tally(lambda r: r["source"]),
        "by_category": tally(lambda r: r["item"].get("category", "")),
        "fail_reasons": dict(Counter(reason.split(":")[0].split("(")[0] for r in results for reason in r["reasons"]).most_common()),
        "failed_ids": {r["item"].get("id", ""): r["reasons"] for r in results if r["status"] == STATUS_FAIL},
        "josa_fixes": len(fix_rows),
        "db_checks": events is not None,
        "db_error": db_error,
        "review": {"seed": seed, "fraction": review_fraction, "n": n_review, "ids": sorted(review_ids)},
        "outputs": {"xlsx": str(xlsx_path), "validated_dataset": str(validated_path)},
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary["outputs"]["summary"] = str(summary_path)
    return summary


def print_summary(summary: dict[str, Any]) -> None:
    status = summary["status"]
    print(f"🔎 {summary['dataset']}: {summary['n_items']}문항 — "
          f"통과 {status.get(STATUS_PASS, 0)}, 탈락 {status.get(STATUS_FAIL, 0)}, 검증 제외 {status.get(STATUS_SKIP, 0)} "
          f"(검증 대상 출처: {', '.join(summary['checked_sources'])})")
    for source, counts in summary["by_source"].items():
        print(f"   {source:<12} " + ", ".join(f"{k} {v}" for k, v in counts.items()))
    for reason, n in summary["fail_reasons"].items():
        print(f"   ✗ {reason}: {n}건")
    print(f"   조사 자동 수정 {summary['josa_fixes']}건, 사람 검토 표본 {summary['review']['n']}문항 (seed={summary['review']['seed']})")
    if not summary["db_checks"]:
        print("   ⚠️ raw_event_info 대조(사건명·날짜·참여자)는 수행하지 않았습니다.")
    for label, path in summary["outputs"].items():
        print(f"💾 {label}: {path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate benchmark questions and write a review workbook.")
    parser.add_argument("dataset", type=Path, help="Benchmark JSON to validate")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR, help="Where the workbook and summary are written (default: evaluation/validation)")
    parser.add_argument("--sources", default="template",
                        help="Comma-separated sources to validate (default: template). Other sources are only checked for duplicates. "
                             "Questions without a source field are always validated.")
    parser.add_argument("--seed", type=int, default=42, help="Seed for the human-review sample (default: 42)")
    parser.add_argument("--review-fraction", type=float, default=0.3, help="Share of passed questions sampled for human review (default: 0.3)")
    parser.add_argument("--no-db", action="store_true", help="Skip the raw_event_info checks (event name, date, participants)")
    args = parser.parse_args()

    summary = validate_dataset(args.dataset, args.out_dir, {s.strip() for s in args.sources.split(",") if s.strip()},
                               args.seed, args.review_fraction, use_db=not args.no_db)
    print_summary(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
