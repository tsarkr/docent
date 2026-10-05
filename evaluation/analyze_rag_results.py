#!/usr/bin/env python3
"""Per-category results and paired significance tests for the RAG benchmark.

Reads the per-query CSV written by ``eval_rag_comparison.py`` and writes:
- rag_category_results.csv   (model x category x metric: n, mean, 95% bootstrap CI)
- rag_significance.csv       (every model pair: paired permutation test + Holm)
- table_rag_by_category.tex  (paper table for the category breakdown)
- rag_source_results.csv / rag_source_significance.csv
                             (the same two tables broken down by question source:
                              curated, template, third_party; written only when
                              the CSV has a ``source`` column)

No LLM or database access is needed, so it can be re-run on existing results.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from stats_utils import bootstrap_ci, holm_adjust, paired_permutation_test

RESULTS_DIR = Path(__file__).resolve().parent / "results"
METRICS = ["faithfulness", "hallucination_rate", "entity_hit_rate_at_k", "entity_mrr"]
OVERALL = "Overall"
SOURCE_ORDER = ["curated", "template", "third_party"]


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


def analyze(
    records: list[dict[str, Any]], group_field: str = "category",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    """Return (per-group rows, significance rows, bookkeeping counts).

    ``group_field`` is the CSV column the questions are broken down by:
    "category" (default) or "source". Overall is always included.
    """
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
        category_of[r["query_id"]] = (r.get(group_field) or "").strip() or "unknown"
        for metric in METRICS:
            value = _to_float(r.get(metric))
            if value is not None:
                scores[metric][r["model"]][r["query_id"]] = value

    groups = set(category_of.values())
    scopes = [OVERALL] + [s for s in SOURCE_ORDER if s in groups] + sorted(groups - set(SOURCE_ORDER))
    category_rows: list[dict[str, Any]] = []
    significance_rows: list[dict[str, Any]] = []

    for metric in METRICS:
        for scope in scopes:
            ids = sorted(q for q, c in category_of.items() if scope == OVERALL or c == scope)
            for model in models:
                values = [scores[metric][model][q] for q in ids if q in scores[metric][model]]
                mean, lo, hi = bootstrap_ci(values)
                category_rows.append({
                    "metric": metric, group_field: scope, "model": model, "n": len(values),
                    "mean": round(mean, 4), "ci95_low": round(lo, 4), "ci95_high": round(hi, 4),
                })
            for model_a, model_b in itertools.combinations(models, 2):
                paired = [q for q in ids if q in scores[metric][model_a] and q in scores[metric][model_b]]
                a = [scores[metric][model_a][q] for q in paired]
                b = [scores[metric][model_b][q] for q in paired]
                diffs = [x - y for x, y in zip(a, b)]
                mean_diff, lo, hi = bootstrap_ci(diffs)
                significance_rows.append({
                    "metric": metric, group_field: scope, "model_a": model_a, "model_b": model_b,
                    "n_pairs": len(paired), "mean_diff_a_minus_b": round(mean_diff, 4),
                    "ci95_low": round(lo, 4), "ci95_high": round(hi, 4),
                    "p_permutation": paired_permutation_test(a, b),
                })

    # Holm correction within each (metric, scope) family of model pairs.
    families: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in significance_rows:
        families[(row["metric"], row[group_field])].append(row)
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


def run(csv_path: Path, out_dir: Path = RESULTS_DIR, dataset: Path | None = None, verbose: bool = False) -> dict[str, Any]:
    records = load_records(csv_path)
    if dataset is not None:
        # Results written before the `source` column existed: take it from the benchmark file.
        source_of = {item["id"]: item.get("source", "") for item in json.loads(dataset.read_text(encoding="utf-8"))}
        for record in records:
            record["source"] = record.get("source") or source_of.get(record["query_id"], "")
    out_dir.mkdir(parents=True, exist_ok=True)

    category_rows, significance_rows, counts = analyze(records)
    _write_csv(category_rows, out_dir / "rag_category_results.csv")
    _write_csv(significance_rows, out_dir / "rag_significance.csv")
    export_category_latex(category_rows, out_dir / "table_rag_by_category.tex")

    # Same breakdown by question source (curated / template / third_party), when the run recorded it.
    counts["has_source"] = any((r.get("source") or "").strip() for r in records)
    if counts["has_source"]:
        source_rows, source_significance, _ = analyze(records, group_field="source")
        _write_csv(source_rows, out_dir / "rag_source_results.csv")
        _write_csv(source_significance, out_dir / "rag_source_significance.csv")
        if verbose:
            for metric in METRICS:
                print(f"\n■ {metric} — source별 평균 [95% CI]")
                for r in (r for r in source_rows if r["metric"] == metric):
                    print(f"  {r['source']:<12} {r['model']:<40} n={r['n']:<3} {r['mean']:8.2f} [{r['ci95_low']:.2f}, {r['ci95_high']:.2f}]")
                print("  쌍대 유의성 검정 (sign-flip permutation, Holm 보정; * = p<0.05)")
                for r in (r for r in source_significance if r["metric"] == metric):
                    print(f"  {r['source']:<12} {r['model_a']} vs {r['model_b']}: Δ={r['mean_diff_a_minus_b']:.2f} "
                          f"(n={r['n_pairs']}), p_holm={r['p_holm']}{' *' if r['significant_0.05'] else ''}")
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Category breakdown and paired significance tests for RAG results.")
    parser.add_argument("--input", type=Path, default=RESULTS_DIR / "rag_evaluation_results.csv")
    parser.add_argument("--out-dir", type=Path, default=RESULTS_DIR)
    parser.add_argument("--dataset", type=Path, default=None,
                        help="Benchmark JSON with a `source` field, used when the CSV has no source column")
    args = parser.parse_args()

    counts = run(args.input, args.out_dir, args.dataset, verbose=True)
    print(f"\n📊 분석 대상 질문: {counts['questions_used']}/{counts['questions_total']}건")
    if counts["questions_excluded_generation_failed"]:
        print(f"⚠️ 생성 실패로 제외된 질문: {counts['questions_excluded_generation_failed']}건")
    if counts["questions_excluded_incomplete"]:
        print(f"⚠️ 일부 모델 결과가 없어 제외된 질문: {counts['questions_excluded_incomplete']}건")
    print(f"💾 {args.out_dir / 'rag_category_results.csv'}")
    print(f"💾 {args.out_dir / 'rag_significance.csv'}")
    print(f"📄 {args.out_dir / 'table_rag_by_category.tex'}")
    if counts["has_source"]:
        print(f"💾 {args.out_dir / 'rag_source_results.csv'}")
        print(f"💾 {args.out_dir / 'rag_source_significance.csv'}")
    else:
        print("ℹ️ CSV에 source 열이 없어 출처별 분석은 생략했습니다 (--dataset 으로 source가 있는 벤치마크 파일을 지정하세요).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
