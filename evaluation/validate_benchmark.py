#!/usr/bin/env python3
"""Validate benchmark questions before they are used (the template-generated ones).

  .venv3.14/bin/python evaluation/validate_benchmark.py evaluation/dataset/benchmark_v2.json

Writes <name>_validation.xlsx (PASS/FAIL per question with reasons, particle
fixes, and a human-review sheet with a seeded random 30% of the passed
questions) and <name>_validation_summary.json to --out-dir. The input file is
never modified. Only `template` questions (and questions without a `source`)
are checked; other sources are only checked for duplicates.

A question FAILS when
  - a person name is shorter than 2 characters, a romanised fragment ("Sun"),
    or a title / common noun (NON_NAME_TOKENS)
  - the event name and date in the question disagree with raw_event_info, with
    the date in the event name, or with the period the question states
  - the trap person is a recorded participant of that event (the trap may be true)
  - a trap compares a place with itself
  - it duplicates an earlier question
Wrong particles after a name ("김상열가") are corrected and listed, not failed.

The name and token rules at the top are also used by
generate_benchmark_dataset.py and by the trap heuristic in eval_rag_comparison.py.
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
for _path in (str(EVAL_DIR.parent), str(EVAL_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

OUT_DIR = EVAL_DIR / "validation"

# ─────────────────────────────────────────────────────────────────────────────
# Name and token rules (shared)
# ─────────────────────────────────────────────────────────────────────────────
NON_NAME_TOKENS = {'목사', '장로', '전도사', '선교사', '교사', '학생', '면장', '구장', '군수', '순사', '헌병', '의사', '기자', '승려', '농민', '주민'}
DATE_TOKEN_RE = re.compile(r"\d+(?:년|월|일)?")
# Places too common to identify a claim: provinces, recurring cities (not exhaustive),
# and anything with a province/county unit suffix ("전주군", "평양부"; not "임시정부").
GENERIC_PLACES = {
    "경기도", "강원도", "충청북도", "충청남도", "전라북도", "전라남도", "경상북도", "경상남도",
    "황해도", "평안북도", "평안남도", "함경북도", "함경남도",
    "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "황해", "평북", "평남", "함북", "함남",
    "경성", "서울", "한성", "평양", "함흥", "원산", "개성", "인천", "수원", "천안", "공주", "대전", "청주", "전주", "군산",
    "광주", "목포", "대구", "부산", "마산", "진주", "해주", "안악", "의주", "신의주", "선천", "정주", "진남포", "성진",
    "청진", "회령", "춘천", "강릉", "제주", "만주", "간도", "북간도", "연해주", "상하이", "상해", "베이징", "북경",
    "도쿄", "동경", "일본", "조선", "한국", "미국", "중국", "러시아",
}
ADMIN_UNIT_RE = re.compile(r"[가-힣]{1,4}(?:도|군|면|읍)|[가-힣]{2,3}(?<!정)(?<!독)(?<!본)(?<!지)(?<!간)(?<!학)부")
JOSA_PAIRS = [("이", "가"), ("은", "는"), ("을", "를"), ("과", "와")]  # (after a final consonant, after a vowel)


def with_josa(word: str, after_consonant: str = "이", after_vowel: str = "가") -> str:
    """Attach the particle that agrees with the word's final sound: 김상열이 / 김구가."""
    last = word[-1:]
    has_batchim = "가" <= last <= "힣" and (ord(last) - 0xAC00) % 28 != 0
    return word + (after_consonant if has_batchim else after_vowel)


def strip_josa(token: str) -> str:
    """Drop one trailing particle so '이동휘가' can be matched as '이동휘'."""
    for josa in ("에서는", "에서", "으로", "까지", "보다", "에게", "이", "가", "은", "는", "을", "를", "의", "에", "와", "과"):
        if token.endswith(josa) and len(token) - len(josa) >= 2:
            return token[: -len(josa)]
    return token


def is_generic_token(token: str) -> bool:
    """Years, dates, and province/county/city-level place names."""
    return bool(DATE_TOKEN_RE.fullmatch(token)) or token in GENERIC_PLACES or bool(ADMIN_UNIT_RE.fullmatch(token))


def name_problem(name: str) -> str:
    """Reason a string cannot be used as a person name ('' when it is acceptable)."""
    if len(name) < 2:
        return "2글자 미만"
    if re.search(r"[A-Za-z]", name):
        return "로마자 조각"
    if name in NON_NAME_TOKENS:
        return "직책·일반명사"
    if not re.fullmatch(r"[가-힣]{2,4}", name):
        return "인명 형식 아님"
    return ""


def parse_person_list(raw: Any, clean: Callable[[str], str] = lambda s: s) -> list[str]:
    """Person names from a raw_event_info '관련인물' cell.

    Entries are separated by ';'. Roles and romanisations in brackets
    ("김선두(Kim Sun Doo)", "강규찬(… 목사)") are removed before the entry is
    split, so their pieces cannot be mistaken for names.
    """
    persons: list[str] = []
    for entry in re.split(r"[;\n]+", str(raw or "")):
        for token in re.split(r"[,\s]+", re.sub(r"\([^)]*\)|\[[^\]]*\]", " ", entry)):
            name = clean(token)
            if not name_problem(name) and name not in persons:
                persons.append(name)
    return persons


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────
NAME_LIST_RE = re.compile(r"([^\s,]+(?:, [^\s,]+)*) 등(?:이|의|에)")   # "A, B, C 등이"
TRAP_SUBJECT_RE = re.compile(r"^(\S+?)(?:가|이)\s")                    # "A가 ..."
INSTRUCTIONS = [
    "벤치마크 문항 자동 검증 결과",
    "",
    "[results] 문항별 판정. PASS=규칙 통과, FAIL=reasons의 사유로 탈락, SKIP=검증 대상 출처가 아님(중복만 확인).",
    "          query 열에는 조사(이/가, 은/는, 을/를, 과/와)를 자동으로 고친 문장이 들어 있고, 고친 내역은 josa_fixes 열에 있습니다.",
    "[human_review] 통과 문항 중 무작위 표본입니다. 질문·정답·함정이 사료와 맞는지 확인하고 verdict에 O 또는 X를 기입해 주십시오.",
    "  - 자동 검증은 형식과 raw_event_info 일치만 확인합니다. 정답 사실(gold_facts)의 역사적 정확성은 사람이 확인해야 합니다.",
]


def _norm(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text)).strip()


def load_event_index() -> dict[str, list[dict[str, Any]]]:
    """title -> [{date, place, persons}] from raw_event_info (read-only)."""
    from scripts.config import get_pg_connection
    from scripts.hanja_utils import translate_hanja_name

    def hangul(text: str) -> str:
        return re.sub(r"[一-鿿]+", lambda m: translate_hanja_name(m.group(0)), text)

    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute('SELECT "사건명", "시위_시작일자", "시위_행정구역명", "관련인물" FROM raw_event_info WHERE "사건명" IS NOT NULL')
    index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for title, start, place, persons in cur.fetchall():
        index[_norm(title)].append({
            "date": str(start) if start else "",
            "place": _norm(hangul(str(place or ""))),
            "persons": parse_person_list(persons, lambda token: re.sub(r"[^\w가-힣]", "", hangul(token))),
        })
    cur.close()
    conn.close()
    return dict(index)


def check_item(item: dict[str, Any], events: dict[str, list[dict[str, Any]]] | None) -> tuple[list[str], dict[str, Any], list[str]]:
    """Apply every rule to one question. Returns (fail reasons, question with particles fixed, fixes)."""
    query = item.get("query", "")
    facts = [*item.get("gold_facts", []), *item.get("ground_truth", [])]
    traps = item.get("negative_traps", [])
    reasons: list[str] = []

    # Events named in the question as 'title', each with the date the question gives for it.
    titles = re.findall(r"'([^']+)'", query)
    dates = re.findall(r"\d{4}-\d{2}-\d{2}", query)
    refs: list[tuple[str, str]] = []
    for title in titles:
        attached = re.search(re.escape(f"'{title}'") + r"\((\d{4}-\d{2}-\d{2})\)", query)
        refs.append((title, attached.group(1) if attached else (dates[0] if len(titles) == 1 and len(dates) == 1 else "")))

    # 1. Person names: lists in the gold facts, trap subjects, romanised target entities.
    names = [(name, "gold_facts") for fact in facts for group in NAME_LIST_RE.findall(fact) for name in group.split(", ")]
    names += [(m.group(1), "negative_traps") for m in map(TRAP_SUBJECT_RE.match, traps) if m]
    names += [(str(e).strip(), "target_entities") for e in item.get("target_entities", []) if re.fullmatch(r"[A-Za-z]+", str(e).strip())]
    for name, where in dict.fromkeys(names):
        if name_problem(name):
            reasons.append(f"인명 오류({name_problem(name)}): '{name}' [{where}]")

    # 2. Event name and date.
    period = re.search(r"1919년\s*(\d{1,2})월", query)
    for title, date in refs:
        rows = (events or {}).get(_norm(title), [])
        if events is not None and not rows:
            reasons.append(f"사건명이 raw_event_info에 없음: '{title}'")
            continue
        if rows and date and date not in {r["date"] for r in rows}:
            recorded = ", ".join(sorted({r["date"] or "날짜 없음" for r in rows}))
            reasons.append(f"질문의 날짜({date})가 raw_event_info({recorded})와 다름: '{title}'")
        if not date:
            continue
        month_day = (int(date[5:7]), int(date[8:10]))
        in_title = [(int(m), int(d)) for m, d in re.findall(r"(\d{1,2})월\s*(\d{1,2})일", title)]
        if in_title:
            low, high = min(in_title), max(in_title)
            slack = 1 if "전후" in title else 0
            if not ((low[0], low[1] - slack) <= month_day <= (high[0], high[1] + slack)):
                reasons.append(f"사건명의 날짜({low[0]}월 {low[1]}일)와 질문의 날짜({date})가 다름: '{title}'")
        if period and int(period.group(1)) != month_day[0]:
            reasons.append(f"질문이 밝힌 시기(1919년 {period.group(1)}월)와 사건 날짜({date})가 다름: '{title}'")

    # 3. Traps: self-contradiction, or the trap person is a recorded participant of the event it names.
    for trap in traps:
        comparison = re.match(r"^(.+?)(?:가|이)\s+(.+?)보다", trap)
        if comparison:
            left, right = (re.sub(r"\W+", "", re.sub(r"\s*(?:시위|사건)$", "", side)) for side in comparison.groups())
            if left and left == right:
                reasons.append(f"함정 자기모순(비교 양쪽이 같은 지명): '{trap}'")
                continue
        subject = TRAP_SUBJECT_RE.match(trap)
        if not subject:
            continue
        # The event the trap is about: named by title or date, else by place, else the question's only event.
        targets = [(t, d) for t, d in refs if t in trap or (d and d in trap)]
        if not targets:
            targets = [(t, d) for t, d in refs
                       if any(row["place"] and row["place"] in _norm(trap) for row in (events or {}).get(_norm(t), []))]
        if not targets and len(refs) == 1:
            targets = refs
        for title, date in targets:
            participants = {p for row in (events or {}).get(_norm(title), []) for p in row["persons"]}
            for fact in facts:
                if title in fact or (date and date in fact):
                    participants.update(name for group in NAME_LIST_RE.findall(fact) for name in group.split(", "))
            if subject.group(1) in participants:
                reasons.append(f"함정이 사실일 수 있음('{subject.group(1)}'이(가) '{title}' 참여자 목록에 있음): '{trap}'")
                break

    # 4. Particles that disagree with the final sound of a name or event title (fixed, not failed).
    words = {name for name, _ in names} | set(titles)
    words |= {str(e) for e in item.get("target_entities", []) if not name_problem(str(e)) and not is_generic_token(str(e))}
    fixed = json.loads(json.dumps(item, ensure_ascii=False))
    fixes: list[str] = []

    def repair(text: str) -> str:
        for word in sorted((w for w in words if "가" <= w[-1:] <= "힣"), key=len, reverse=True):
            for pair in JOSA_PAIRS:
                right = with_josa(word, *pair)
                wrong = word + (pair[1] if right.endswith(pair[0]) else pair[0])
                text, n = re.subn(r"(?<![가-힣])" + re.escape(wrong) + r"(?=[\s,.?]|$)", right, text)
                if n and f"{wrong} → {right}" not in fixes:
                    fixes.append(f"{wrong} → {right}")
        return text

    for field in ("query", "gold_facts", "ground_truth", "negative_traps"):
        if isinstance(fixed.get(field), str):
            fixed[field] = repair(fixed[field])
        elif isinstance(fixed.get(field), list):
            fixed[field] = [repair(v) for v in fixed[field]]
    return reasons, fixed, fixes


def validate_dataset(dataset_path: Path, out_dir: Path = OUT_DIR, seed: int = 42, save_passed: Path | None = None) -> dict[str, Any]:
    """Validate a benchmark file, write the workbook and summary, print the result."""
    from annotation_io import add_sheet, new_workbook

    items = json.loads(dataset_path.read_text(encoding="utf-8"))
    try:
        events = load_event_index()
    except Exception as exc:  # the rules that do not need the database still run
        events = None
        print(f"⚠️ raw_event_info를 읽지 못해 DB 대조(사건명·날짜·참여자)를 건너뜁니다: {str(exc)[:200]}")

    rows, kept = [], []
    seen: dict[str, str] = {}
    for item in items:
        source = item.get("source") or "unknown"
        query = item.get("query", "")
        titles = sorted({_norm(t) for t in re.findall(r"'([^']+)'", query)})
        # Same wording, or same category + same events + same leading subject = the same question again.
        keys = [re.sub(r"\W+", "", query)]
        if titles:
            keys.append(str((item.get("category", ""), titles, query.split("'")[0].split()[:1])))
        duplicate_of = next((seen[k] for k in keys if k in seen), "")
        for k in keys:
            seen.setdefault(k, item.get("id", ""))

        reasons = [f"질문 중복: {duplicate_of}와 같은 질문·사건 조합"] if duplicate_of else []
        checked = source in ("template", "unknown")
        fixed, fixes = item, []
        if checked:
            found, fixed, fixes = check_item(item, events)
            reasons += found
        status = "FAIL" if reasons else ("PASS" if checked else "SKIP")
        if status != "FAIL":
            kept.append(fixed)
        rows.append({"id": item.get("id", ""), "source": source, "category": item.get("category", ""), "status": status,
                     "reasons": "\n".join(reasons), "josa_fixes": "\n".join(fixes), "query": fixed.get("query", ""),
                     "gold_facts": "\n".join(fixed.get("gold_facts", [])), "negative_traps": "\n".join(fixed.get("negative_traps", []))})

    passed = [r for r in rows if r["status"] == "PASS"]
    review = random.Random(seed).sample(passed, math.ceil(len(passed) * 0.3))

    out_dir.mkdir(parents=True, exist_ok=True)
    xlsx_path = out_dir / f"{dataset_path.stem}_validation.xlsx"
    wb = new_workbook(INSTRUCTIONS)
    add_sheet(wb, "results", ["id", "source", "category", "status", "reasons", "josa_fixes", "query"], rows,
              widths={"id": 12, "category": 26, "status": 8, "reasons": 80, "josa_fixes": 40, "query": 80})
    add_sheet(wb, "human_review", ["id", "source", "category", "query", "gold_facts", "negative_traps", "verdict", "note"], review,
              widths={"id": 12, "category": 26, "query": 70, "gold_facts": 80, "negative_traps": 70, "note": 30},
              choices={"verdict": ["O", "X"]})
    wb.save(xlsx_path)

    by_source: dict[str, Counter] = defaultdict(Counter)
    for r in rows:
        by_source[r["source"]][r["status"]] += 1
    summary = {
        "dataset": dataset_path.name,
        "n_items": len(items),
        "status": dict(Counter(r["status"] for r in rows)),
        "by_source": {k: dict(v) for k, v in by_source.items()},
        "fail_reasons": dict(Counter(re.split(r"[:(]", line)[0] for r in rows for line in r["reasons"].split("\n") if line).most_common()),
        "failed": {r["id"]: r["reasons"].split("\n") for r in rows if r["status"] == "FAIL"},
        "josa_fixes": sum(len(r["josa_fixes"].split("\n")) for r in rows if r["josa_fixes"]),
        "db_checks": events is not None,
        "review_sample": {"seed": seed, "fraction": 0.3, "ids": sorted(r["id"] for r in review)},
    }
    summary_path = out_dir / f"{dataset_path.stem}_validation_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if save_passed:
        save_passed.write_text(json.dumps(kept, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"🔎 {dataset_path.name}: {len(items)}문항 — " + ", ".join(f"{k} {v}" for k, v in summary["status"].items()))
    for reason, n in summary["fail_reasons"].items():
        print(f"   ✗ {reason}: {n}건")
    print(f"   조사 자동 수정 {summary['josa_fixes']}건, 사람 검토 표본 {len(review)}문항 (seed={seed})")
    print(f"💾 {xlsx_path}\n💾 {summary_path}" + (f"\n💾 {save_passed} ({len(kept)}문항)" if save_passed else ""))
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate benchmark questions and write a review workbook.")
    parser.add_argument("dataset", type=Path, help="Benchmark JSON to validate")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR, help="Where the workbook and summary are written (default: evaluation/validation)")
    parser.add_argument("--seed", type=int, default=42, help="Seed for the human-review sample (default: 42)")
    parser.add_argument("--save-passed", type=Path, default=None,
                        help="Also write the questions that did not fail (particles fixed) to this JSON, for use with --dataset")
    args = parser.parse_args()
    validate_dataset(args.dataset, args.out_dir, args.seed, args.save_passed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
