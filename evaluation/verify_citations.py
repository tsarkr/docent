#!/usr/bin/env python3
"""Check the citations in generated answers against the evidence each system received.

An answer may cite three kinds of things:

  record id       raw_bib_123 / raw_bibliography-123. Valid only if that record
                  is part of the evidence passed to the generator.
  internal label  relation types (P70_documents, P106i...), block tags
                  (GRAPH_TOPOLOGY, GRAPH_3, S1, "블록 3"), "Graph DB", ...
                  These are never valid: a reader cannot follow them to a source.
  named source    a newspaper issue, event name, or document title written out.
                  Valid if the same string occurs in the evidence.

``verify_answer`` classifies every citation, and ``clean_answer`` removes the
invalid ones (record ids that are not in the evidence and internal labels)
while leaving the sentence they were attached to untouched. Named sources that
cannot be found are reported but not removed, because the string match is
approximate.

Usage (works on any ragas_dataset.json written by eval_rag_comparison.py, so
it can be applied to earlier runs without re-generating answers):

  .venv3.14/bin/python evaluation/verify_citations.py \
      --input evaluation/results/ragas_dataset.json \
      --out-dir evaluation/results
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

ID_RE = re.compile(r"raw_bib(?:liography)?[_\-\s]?(\d+)", re.IGNORECASE)
CONTEXT_ID_RE = re.compile(r"raw_bib(?:liography)?[_\-](\d+)|raw_bibliography row (\d+)", re.IGNORECASE)

# Things that look like citations but point at the system's own structure.
LABEL_RE = re.compile(
    r"""^(
        P\d+[a-z]?(?:_[A-Za-z0-9_]+)?        # CIDOC-CRM properties: P70_documents, P106i, P106i_02
      | E\d+(?:_[A-Za-z_]+)?                 # CIDOC-CRM classes
      | <?/?GRAPH_TOPOLOGY>? | GRAPH_\d+      # context block tags
      | <?/?SOURCE_EVIDENCE>?
      | S\d+ | g_\d+                         # item labels
      | 블록\s*\d+
      | graph\s*db(?:\s*컨텍스트)?
      | 그래프\s*db | 지식\s*그래프(?:\s*(?:문맥|컨텍스트|톱놀로지|토폴로지))?
      | 사료\s*및\s*지식\s*그래프
      | 액티비티 | activity
      | P14_carried_out_by | P7_took_place_at | P9_consists_of | ACTIVATED_AT | DEFINED_AS
    )$""",
    re.IGNORECASE | re.VERBOSE,
)

# Label fragments inside a longer citation ("사료 ID: P106i", "GRAPH_TOPOLOGY 구간 1").
LABEL_FRAGMENT_RE = re.compile(
    r"\bP\d+[a-z]?(?:_[A-Za-z0-9_]+)?\b|GRAPH_|SOURCE_EVIDENCE|그래프\s*(?:톱|토)폴로지|블록\s*\d|사료\s*ID\s*:",
    re.IGNORECASE,
)


def is_label(part: str) -> bool:
    return bool(LABEL_RE.match(part) or LABEL_FRAGMENT_RE.search(part))


GROUP_RE = re.compile(r"\(([^()\n]{1,200})\)|`([^`\n]{1,120})`")
SOURCE_CUE_RE = re.compile(r"\d|보$|보\s|신문|일보|판결|조서|문서|보고|사건|시위|배포|선언|회의|기록")


def _norm(text: str) -> str:
    return re.sub(r"[\s`\[\]『』「」《》<>\"'·.,:;~\-–—]", "", text).lower()


def context_ids(contexts: list[str]) -> set[str]:
    ids: set[str] = set()
    for ctx in contexts:
        for a, b in CONTEXT_ID_RE.findall(ctx):
            ids.add(a or b)
    return ids


def _split(group: str) -> list[str]:
    return [p.strip(" `[]") for p in re.split(r"[,;，、]|\s/\s", group) if p.strip(" `[]")]


def verify_answer(answer: str, contexts: list[str]) -> list[dict[str, Any]]:
    """Return one entry per citation: text, kind, valid."""
    ids = context_ids(contexts)
    norm_ctx = _norm("\n".join(contexts))
    found: list[dict[str, Any]] = []
    for m in GROUP_RE.finditer(answer or ""):
        group = m.group(1) or m.group(2)
        parts = _split(group)
        has_pointer = any(ID_RE.search(p) or is_label(p) for p in parts)
        for part in parts:
            id_match = ID_RE.search(part)
            if id_match:
                found.append({"text": part, "kind": "record_id", "valid": id_match.group(1) in ids,
                              "span": m.span()})
            elif is_label(part):
                found.append({"text": part, "kind": "internal_label", "valid": False, "span": m.span()})
            elif (has_pointer or m.group(2)) and len(part) >= 4 or (len(part) >= 8 and SOURCE_CUE_RE.search(part)):
                found.append({"text": part, "kind": "named_source",
                              "valid": bool(_norm(part)) and _norm(part) in norm_ctx, "span": m.span()})
    return found


def clean_answer(answer: str, contexts: list[str]) -> str:
    """Remove record ids not in the evidence and internal labels; keep everything else."""
    ids = context_ids(contexts)

    def keep(part: str) -> bool:
        id_match = ID_RE.search(part)
        if id_match:
            return id_match.group(1) in ids
        return not is_label(part)

    def fix(m: re.Match) -> str:
        group = m.group(1) if m.group(1) is not None else m.group(2)
        parts = _split(group)
        if not any(ID_RE.search(p) or is_label(p) for p in parts):
            return m.group(0)  # not a citation group
        kept = [p for p in parts if keep(p)]
        if not kept:
            return ""
        return f"({', '.join(kept)})" if m.group(1) is not None else f"`{', '.join(kept)}`"

    cleaned = GROUP_RE.sub(fix, answer or "")
    return re.sub(r"[ \t]+([.,。])", r"\1", re.sub(r"\(\s*\)", "", cleaned))


def summarize(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_model: dict[str, dict[str, Any]] = defaultdict(lambda: defaultdict(int))
    for rec in records:
        s = by_model[rec["model"]]
        s["answers"] += 1
        cites = rec["citations"]
        if not any(c["kind"] in ("record_id", "internal_label") for c in cites):
            s["answers_without_pointer"] += 1
        if any(not c["valid"] and c["kind"] in ("record_id", "internal_label") for c in cites):
            s["answers_with_invalid_pointer"] += 1
        for c in cites:
            s[f"{c['kind']}_total"] += 1
            s[f"{c['kind']}_valid"] += int(c["valid"])
    out = {}
    for model, s in by_model.items():
        pointers = s["record_id_total"] + s["internal_label_total"]
        out[model] = {
            **dict(s),
            "pointer_validity": round(s["record_id_valid"] / pointers, 4) if pointers else None,
            "named_source_found_rate": round(s["named_source_valid"] / s["named_source_total"], 4)
            if s["named_source_total"] else None,
        }
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path, default=Path(__file__).resolve().parent / "results" / "ragas_dataset.json")
    parser.add_argument("--out-dir", type=Path, default=Path(__file__).resolve().parent / "results")
    parser.add_argument("--write-clean", action="store_true",
                        help="Also write ragas_dataset_cleaned.json with invalid citations removed")
    args = parser.parse_args()

    data = json.loads(args.input.read_text(encoding="utf-8"))
    records = []
    for item in data:
        cites = verify_answer(item.get("response", ""), item.get("retrieved_contexts", []))
        records.append({"query_id": item.get("query_id"), "model": item.get("model"),
                        "category": item.get("category", ""), "citations": cites})

    args.out_dir.mkdir(parents=True, exist_ok=True)
    with (args.out_dir / "citation_verification.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["query_id", "model", "category", "kind", "valid", "text"])
        for rec in records:
            for c in rec["citations"]:
                w.writerow([rec["query_id"], rec["model"], rec["category"], c["kind"], c["valid"], c["text"]])
    summary = summarize(records)
    (args.out_dir / "citation_verification_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.write_clean:
        cleaned = [{**item, "response": clean_answer(item.get("response", ""), item.get("retrieved_contexts", []))}
                   for item in data]
        (args.out_dir / "ragas_dataset_cleaned.json").write_text(
            json.dumps(cleaned, ensure_ascii=False, indent=2), encoding="utf-8")

    for model, s in summary.items():
        print(f"{model}")
        print(f"  record ids: {s.get('record_id_valid', 0)}/{s.get('record_id_total', 0)} valid, "
              f"internal labels: {s.get('internal_label_total', 0)}, pointer validity: {s['pointer_validity']}")
        print(f"  named sources found in evidence: {s.get('named_source_valid', 0)}/{s.get('named_source_total', 0)}")
        print(f"  answers with an invalid pointer: {s.get('answers_with_invalid_pointer', 0)}/{s['answers']}, "
              f"without any pointer: {s.get('answers_without_pointer', 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
