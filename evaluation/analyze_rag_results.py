#!/usr/bin/env python3
"""Per-category results and paired significance tests for the RAG benchmark.

Reads the per-query CSV written by ``eval_rag_comparison.py`` and writes:
- rag_category_results.csv   (model x category x metric: n, mean, 95% bootstrap CI)
- rag_significance.csv       (every model pair: paired permutation test + Holm)
- table_rag_by_category.tex  (paper table for the category breakdown)

No LLM or database access is needed, so it can be re-run on existing results.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from stats_utils import bootstrap_ci, holm_adjust, paired_permutation_test

RESULTS_DIR = Path(__file__).resolve().parent / "results"
METRICS = ["faithfulness", "hallucination_rate", "entity_hit_rate_at_k", "entity_mrr"]
OVERALL = "Overall"


def load_records(csv_path: Path) -> list[dict[str, Any]]:
    with csv_path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _is_failed(record: dict[str, Any]) -> bool:
    return str(record.get("generation_failed", "")).strip().lower() in {"1", "true"}


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def analyze(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    """Return (category rows, significance rows, bookkeeping counts)."""
    models = list(dict.fromkeys(r["model"] for r in records))
    # A question only enters the comparison if every model produced an answer
    # for it; otherwise an API failure would be scored as a model failure.
    failed_ids = {r["query_id"] for r in records if _is_failed(r)}
    seen: dict[str, set[str]] = defaultdict(set)
    for r in records:
        seen[r["query_id"]].add(r["model"])
    incomplete_ids = {qid for qid, ms in seen.items() if len(ms) < len(models)}
    excluded = failed_ids | incomplete_ids

    # scores[metric][model][query_id] = value
    scores: dict[str, dict[str, dict[str, float]]] = {m: defaultdict(dict) for m in METRICS}
    category_of: dict[str, str] = {}
    for r in records:
        if r["query_id"] in excluded:
            continue
        category_of[r["query_id"]] = r["category"]
        for metric in METRICS:
            value = _to_float(r.get(metric))
            if value is not None:
                scores[metric][r["model"]][r["query_id"]] = value

    scopes = [OVERALL] + sorted(set(category_of.values()))
    category_rows: list[dict[str, Any]] = []
    significance_rows: list[dict[str, Any]] = []

    for metric in METRICS:
        for scope in scopes:
            ids = sorted(q for q, c in category_of.items() if scope == OVERALL or c == scope)
            for model in models:
                values = [scores[metric][model][q] for q in ids if q in scores[metric][model]]
                mean, lo, hi = bootstrap_ci(values)
                category_rows.append({
                    "metric": metric, "category": scope, "model": model, "n": len(values),
                    "mean": round(mean, 4), "ci95_low": round(lo, 4), "ci95_high": round(hi, 4),
                })
            for model_a, model_b in itertools.combinations(models, 2):
                paired = [q for q in ids if q in scores[metric][model_a] and q in scores[metric][model_b]]
                a = [scores[metric][model_a][q] for q in paired]
                b = [scores[metric][model_b][q] for q in paired]
                diffs = [x - y for x, y in zip(a, b)]
                mean_diff, lo, hi = bootstrap_ci(diffs)
                significance_rows.append({
                    "metric": metric, "category": scope, "model_a": model_a, "model_b": model_b,
                    "n_pairs": len(paired), "mean_diff_a_minus_b": round(mean_diff, 4),
                    "ci95_low": round(lo, 4), "ci95_high": round(hi, 4),
                    "p_permutation": paired_permutation_test(a, b),
                })

    # Holm correction within each (metric, scope) family of model pairs.
    families: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in significance_rows:
        families[(row["metric"], row["category"])].append(row)
    for rows in families.values():
        for row, adjusted in zip(rows, holm_adjust([r["p_permutation"] for r in rows])):
            row["p_holm"] = round(adjusted, 5)
            row["p_permutation"] = round(row["p_permutation"], 5)
            row["significant_0.05"] = bool(adjusted == adjusted and adjusted < 0.05)

    counts = {
        "questions_total": len(seen),
        "questions_used": len(category_of),
        "questions_excluded_generation_failed": len(failed_ids),
        "questions_excluded_incomplete": len(incomplete_ids - failed_ids),
    }
    return category_rows, significance_rows, counts


def _write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    if not rows:
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def export_category_latex(category_rows: list[dict[str, Any]], out_path: Path, metric: str = "faithfulness") -> None:
    rows = [r for r in category_rows if r["metric"] == metric]
    models = list(dict.fromkeys(r["model"] for r in rows))
    scopes = list(dict.fromkeys(r["category"] for r in rows))
    cell = {(r["category"], r["model"]): r for r in rows}
    lines = [
        r"\begin{table*}[htbp]",
        r"\centering",
        rf"\caption{{{metric.replace('_', ' ').title()} by question category (mean, 95\% bootstrap CI)}}",
        r"\label{tab:rag_by_category}",
        r"\begin{tabular}{l|c|" + "c" * len(models) + "}",
        r"\hline",
        r"\textbf{Category} & \textbf{N} & " + " & ".join(rf"\textbf{{{m}}}" for m in models) + r" \\",
        r"\hline",
    ]
    for scope in scopes:
        n = cell[(scope, models[0])]["n"]
        values = " & ".join(
            f"{cell[(scope, m)]['mean']:.1f} [{cell[(scope, m)]['ci95_low']:.1f}, {cell[(scope, m)]['ci95_high']:.1f}]"
            for m in models
        )
        lines.append(f"{scope.replace('&', r'\&')} & {n} & {values} \\\\")
    lines += [r"\hline", r"\end{tabular}", r"\end{table*}", ""]
    out_path.write_text("\n".join(lines), encoding="utf-8")


def run(csv_path: Path, out_dir: Path = RESULTS_DIR) -> dict[str, int]:
    category_rows, significance_rows, counts = analyze(load_records(csv_path))
    _write_csv(category_rows, out_dir / "rag_category_results.csv")
    _write_csv(significance_rows, out_dir / "rag_significance.csv")
    export_category_latex(category_rows, out_dir / "table_rag_by_category.tex")
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Category breakdown and paired significance tests for RAG results.")
    parser.add_argument("--input", type=Path, default=RESULTS_DIR / "rag_evaluation_results.csv")
    parser.add_argument("--out-dir", type=Path, default=RESULTS_DIR)
    args = parser.parse_args()

    counts = run(args.input, args.out_dir)
    print(f"📊 분석 대상 질문: {counts['questions_used']}/{counts['questions_total']}건")
    if counts["questions_excluded_generation_failed"]:
        print(f"⚠️ 생성 실패로 제외된 질문: {counts['questions_excluded_generation_failed']}건")
    if counts["questions_excluded_incomplete"]:
        print(f"⚠️ 일부 모델 결과가 없어 제외된 질문: {counts['questions_excluded_incomplete']}건")
    print(f"💾 {args.out_dir / 'rag_category_results.csv'}")
    print(f"💾 {args.out_dir / 'rag_significance.csv'}")
    print(f"📄 {args.out_dir / 'table_rag_by_category.tex'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
