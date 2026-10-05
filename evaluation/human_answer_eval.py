#!/usr/bin/env python3
"""Blind human evaluation of final answers (factual accuracy, citation accuracy, usefulness).

  export  Read the answers saved by eval_rag_comparison.py (ragas_dataset.json)
          and write one Excel workbook per rater. Model names are replaced by
          shuffled labels; the mapping goes to blinding_key.json, which must
          not be shared with the raters.
  score   Combine completed workbooks with the key and report per-model means
          with bootstrap CIs, paired significance tests, citation accuracy,
          and inter-rater agreement.

Citation accuracy is what backs a provenance claim: for every sentence that
cites a source record, the rater checks the record itself and marks whether it
supports the sentence.
"""

from __future__ import annotations

import argparse
import itertools
import json
import random
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from annotation_io import add_sheet, new_workbook, read_sheet
from stats_utils import bootstrap_ci, cohen_kappa, holm_adjust, paired_permutation_test

from scripts.config import get_pg_connection

EVAL_DIR = Path(__file__).resolve().parent
OUT_DIR = EVAL_DIR / "human_eval"
RATING_COLUMNS = ["factual_accuracy", "usefulness"]
SCALE = ["1", "2", "3", "4", "5"]
SUPPORT_VALUES = {"O": 1.0, "△": 0.5, "X": 0.0}
CITATION_PATTERN = re.compile(r"raw_bib_(\d+)")

INSTRUCTIONS = [
    "최종 답변 전문가 평가 시트 (블라인드)",
    "",
    "[answers] 시트: 같은 질문에 대한 여러 시스템의 답변이 무작위 순서로 섞여 있습니다. 어느 시스템인지는 표시하지 않습니다.",
    "  - factual_accuracy (1~5): 답변의 역사적 사실이 정확한가. 1=핵심 내용이 틀림, 3=부분적으로 정확, 5=오류 없음.",
    "    '자료에서 확인되지 않음'처럼 답을 유보한 경우, 유보가 타당하면 높게, 실제로는 답할 수 있는 질문이면 낮게 채점합니다.",
    "  - usefulness (1~5): 질문한 연구자·관람객에게 실제로 도움이 되는가. 1=쓸모없음, 5=그대로 활용 가능.",
    "[citations] 시트: 답변 문장(claim)과 그 문장이 인용한 사료 원문(source_text)입니다.",
    "  - supports: O=사료가 문장을 뒷받침함, △=일부만 뒷받침함, X=뒷받침하지 않거나 무관함.",
    "다른 평가자와 상의하지 말고 독립적으로 채점해 주십시오. item_id와 cite_id는 수정하지 마십시오.",
]


def _is_failed(record: dict[str, Any]) -> bool:
    answer = str(record.get("response", "")).strip()
    return bool(record.get("generation_failed")) or not answer or bool(re.match(r"\[[A-Za-z ]*Error", answer))


def _source_texts(rowids: set[int]) -> dict[int, str]:
    if not rowids:
        return {}
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("SELECT rowid, tei FROM raw_bibliography WHERE rowid = ANY(%s)", (sorted(rowids),))
    out = {}
    for rowid, tei in cur.fetchall():
        body = re.search(r"<body>(.*?)</body>", tei or "", flags=re.DOTALL)
        out[int(rowid)] = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", body.group(1) if body else tei or "")).strip()
    cur.close()
    conn.close()
    return out


def cmd_export(args: argparse.Namespace) -> int:
    records = json.loads(args.input.read_text(encoding="utf-8"))
    by_question: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        by_question[record["query_id"]].append(record)

    n_models = len({r["model"] for r in records})
    usable = {q: rs for q, rs in by_question.items() if len(rs) == n_models and not any(_is_failed(r) for r in rs)}
    skipped = len(by_question) - len(usable)

    rng = random.Random(args.seed)
    question_ids = sorted(usable)
    if args.n_questions and args.n_questions < len(question_ids):
        # Sample evenly across categories so every category is rated.
        by_category: dict[str, list[str]] = defaultdict(list)
        for q in question_ids:
            by_category[usable[q][0].get("category", "")].append(q)
        for ids in by_category.values():
            rng.shuffle(ids)
        picked = [q for group in itertools.zip_longest(*by_category.values()) for q in group if q]
        question_ids = sorted(picked[:args.n_questions])

    answer_rows, citation_rows, key = [], [], {}
    for query_id in question_ids:
        answers = list(usable[query_id])
        rng.shuffle(answers)
        for label, record in zip("ABCDEFGH", answers):
            item_id = f"{query_id}-{label}"
            key[item_id] = {"query_id": query_id, "model": record["model"], "category": record.get("category", "")}
            answer_rows.append({"item_id": item_id, "query_id": query_id, "question": record["user_input"],
                                "answer_label": f"답변 {label}", "answer": record["response"]})
            for sentence in re.split(r"(?<=[.!?。])\s+|\n+", record["response"]):
                for rowid in dict.fromkeys(CITATION_PATTERN.findall(sentence)):
                    citation_rows.append({"cite_id": f"{item_id}#{len(citation_rows) + 1}", "item_id": item_id,
                                          "claim": sentence.strip(), "cited_source": f"raw_bib_{rowid}", "_rowid": int(rowid)})

    sources = _source_texts({row["_rowid"] for row in citation_rows})
    for row in citation_rows:
        row["source_text"] = sources.get(row.pop("_rowid"), "[해당 사료 없음: 존재하지 않는 ID 인용]")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    for rater in args.raters:
        wb = new_workbook(INSTRUCTIONS)
        add_sheet(wb, "answers", ["item_id", "query_id", "question", "answer_label", "answer", *RATING_COLUMNS, "note"], answer_rows,
                  widths={"item_id": 14, "question": 50, "answer": 100, "note": 30},
                  choices={c: SCALE for c in RATING_COLUMNS})
        add_sheet(wb, "citations", ["cite_id", "item_id", "claim", "cited_source", "source_text", "supports", "note"], citation_rows,
                  widths={"cite_id": 16, "item_id": 14, "claim": 60, "source_text": 100, "note": 30},
                  choices={"supports": list(SUPPORT_VALUES)})
        path = args.out_dir / f"rating_{rater}.xlsx"
        wb.save(path)
        print(f"📝 {path}")
    key_path = args.out_dir / "blinding_key.json"
    key_path.write_text(json.dumps(key, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"🔑 {key_path}  (평가자에게 전달하지 마세요)")
    print(f"✅ 질문 {len(question_ids)}건 × 모델 {n_models}개 = 답변 {len(answer_rows)}개, 인용 검증 {len(citation_rows)}건")
    if skipped:
        print(f"⚠️ 생성 실패 답변이 있어 제외한 질문: {skipped}건")
    cited_models = {key[row["item_id"]]["model"] for row in citation_rows}
    uncited = sorted({v["model"] for v in key.values()} - cited_models)
    if uncited:
        print(f"ℹ️ 사료 ID를 인용하지 않는 모델(인용 정확성 산출 불가): {uncited}")
    return 0


def _fmt(value: float) -> str:
    return "  n/a" if value != value else f"{value:5.2f}"


def cmd_score(args: argparse.Namespace) -> int:
    key = json.loads(args.key.read_text(encoding="utf-8"))
    raters = {path.stem: path for path in args.files}

    # ratings[metric][rater][item_id]
    ratings: dict[str, dict[str, dict[str, float]]] = {m: defaultdict(dict) for m in RATING_COLUMNS}
    supports: dict[str, dict[str, str]] = defaultdict(dict)
    cite_item: dict[str, str] = {}
    for rater, path in raters.items():
        for row in read_sheet(path, "answers"):
            for metric in RATING_COLUMNS:
                if row[metric]:
                    ratings[metric][rater][row["item_id"]] = float(row[metric])
        for row in read_sheet(path, "citations"):
            cite_item[row["cite_id"]] = row["item_id"]
            if row["supports"] in SUPPORT_VALUES:
                supports[rater][row["cite_id"]] = row["supports"]

    models = list(dict.fromkeys(v["model"] for v in key.values()))
    report: dict[str, Any] = {"n_raters": len(raters), "models": {}, "significance": [], "agreement": {}}

    # Per-question score of a model = mean over raters.
    per_question: dict[str, dict[str, dict[str, float]]] = {m: defaultdict(dict) for m in RATING_COLUMNS}
    for metric in RATING_COLUMNS:
        collected: dict[tuple[str, str], list[float]] = defaultdict(list)
        for rater_scores in ratings[metric].values():
            for item_id, score in rater_scores.items():
                collected[(key[item_id]["model"], key[item_id]["query_id"])].append(score)
        for (model, query_id), values in collected.items():
            per_question[metric][model][query_id] = sum(values) / len(values)

    print(f"평가자 {len(raters)}명\n{'model':<42} {'metric':<18} {'n':>4} {'mean':>6}  95% CI")
    for model in models:
        report["models"][model] = {}
        for metric in RATING_COLUMNS:
            values = list(per_question[metric][model].values())
            mean, lo, hi = bootstrap_ci(values)
            report["models"][model][metric] = {"n": len(values), "mean": mean, "ci95": [lo, hi]}
            print(f"{model:<42} {metric:<18} {len(values):>4} {_fmt(mean)}  [{_fmt(lo)}, {_fmt(hi)}]")

        strict, lenient = [], []
        for rater_supports in supports.values():
            for cite_id, value in rater_supports.items():
                if key[cite_item[cite_id]]["model"] == model:
                    strict.append(1.0 if value == "O" else 0.0)
                    lenient.append(SUPPORT_VALUES[value])
        report["models"][model]["citation_accuracy"] = {
            "n_judgements": len(strict),
            "strict": sum(strict) / len(strict) if strict else None,
            "lenient": sum(lenient) / len(lenient) if lenient else None,
        }
        if strict:
            print(f"{model:<42} {'citation_accuracy':<18} {len(strict):>4} {_fmt(sum(strict) / len(strict))}  (△ 0.5점 포함 시 {_fmt(sum(lenient) / len(lenient))})")
        else:
            print(f"{model:<42} {'citation_accuracy':<18} {0:>4}   n/a  (인용 없음)")

    print("\n쌍대 유의성 검정 (sign-flip permutation, Holm 보정)")
    for metric in RATING_COLUMNS:
        rows = []
        for model_a, model_b in itertools.combinations(models, 2):
            shared = sorted(set(per_question[metric][model_a]) & set(per_question[metric][model_b]))
            a = [per_question[metric][model_a][q] for q in shared]
            b = [per_question[metric][model_b][q] for q in shared]
            diff, lo, hi = bootstrap_ci([x - y for x, y in zip(a, b)])
            rows.append({"metric": metric, "model_a": model_a, "model_b": model_b, "n_pairs": len(shared),
                         "mean_diff_a_minus_b": diff, "ci95": [lo, hi], "p_permutation": paired_permutation_test(a, b)})
        for row, adjusted in zip(rows, holm_adjust([r["p_permutation"] for r in rows])):
            row["p_holm"] = adjusted
            print(f"  {metric:<18} {row['model_a']} vs {row['model_b']}: Δ={_fmt(row['mean_diff_a_minus_b'])} (n={row['n_pairs']}), p_holm={adjusted:.4f}")
        report["significance"].extend(rows)

    for rater_a, rater_b in itertools.combinations(raters, 2):
        entry: dict[str, Any] = {}
        for metric in RATING_COLUMNS:
            shared = sorted(set(ratings[metric][rater_a]) & set(ratings[metric][rater_b]))
            entry[metric] = {"n": len(shared), "weighted_kappa": cohen_kappa(
                [int(ratings[metric][rater_a][i]) for i in shared], [int(ratings[metric][rater_b][i]) for i in shared], weights="quadratic")}
        shared = sorted(set(supports[rater_a]) & set(supports[rater_b]))
        entry["citation_supports"] = {"n": len(shared), "kappa": cohen_kappa(
            [supports[rater_a][i] for i in shared], [supports[rater_b][i] for i in shared])}
        report["agreement"][f"{rater_a} vs {rater_b}"] = entry
        print(f"\n🤝 {rater_a} vs {rater_b}: " + ", ".join(
            f"{name} κ={_fmt(v.get('weighted_kappa', v.get('kappa')))} (n={v['n']})" for name, v in entry.items()))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    # NaN is not valid JSON; store it as null.
    text = json.dumps(report, ensure_ascii=False, indent=2)
    args.output.write_text(re.sub(r"\bNaN\b", "null", text), encoding="utf-8")
    print(f"\n💾 {args.output}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Blind human evaluation of final answers.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_export = sub.add_parser("export", help="Write blinded rating workbooks")
    p_export.add_argument("--input", type=Path, default=EVAL_DIR / "results" / "ragas_dataset.json")
    p_export.add_argument("--n-questions", type=int, default=0, help="Rate only N questions, sampled across categories (0 = all)")
    p_export.add_argument("--seed", type=int, default=42)
    p_export.add_argument("--raters", nargs="+", default=["A", "B"], help="One workbook per rater (default: A B)")
    p_export.add_argument("--out-dir", type=Path, default=OUT_DIR)
    p_export.set_defaults(func=cmd_export)

    p_score = sub.add_parser("score", help="Score completed rating workbooks")
    p_score.add_argument("files", nargs="+", type=Path)
    p_score.add_argument("--key", type=Path, default=OUT_DIR / "blinding_key.json")
    p_score.add_argument("--output", type=Path, default=EVAL_DIR / "results" / "human_eval_results.json")
    p_score.set_defaults(func=cmd_score)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
