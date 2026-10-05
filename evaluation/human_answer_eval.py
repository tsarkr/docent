#!/usr/bin/env python3
"""Blind human evaluation of final answers (accuracy, completeness, unsupported claims, usefulness, citations).

  export  Read the answers saved by eval_rag_comparison.py (ragas_dataset.json)
          and write one Excel workbook per rater. Model names are replaced by
          shuffled labels; the mapping goes to blinding_key.json, which is
          written to a separate folder (human_eval_key/) so that the rater
          folder can be shared as a whole.
  score   Combine completed workbooks with the key and report per-model means
          with bootstrap CIs, paired significance tests, citation accuracy,
          inter-rater agreement, and the correlation between ratings and
          answer length (length-bias check).

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
import numpy as np

from stats_utils import bootstrap_ci, cohen_kappa, holm_adjust, paired_permutation_test

from scripts.config import get_pg_connection

EVAL_DIR = Path(__file__).resolve().parent
OUT_DIR = EVAL_DIR / "human_eval"
KEY_DIR = EVAL_DIR / "human_eval_key"  # never inside the folder handed to raters
SCALE = ["1", "2", "3", "4", "5"]
NA = "해당없음"
YES, NO = "Y", "N"
# Columns the rater fills in, with the values offered in the dropdown (None = free integer).
RATING_CHOICES: dict[str, list[str] | None] = {
    "declined": [YES, NO],
    "decline_justified": [YES, NO, NA],
    "accuracy": [*SCALE, NA],
    "completeness": SCALE,
    "unsupported_claims": None,
    "usefulness": SCALE,
}
RATING_COLUMNS = list(RATING_CHOICES)
SCALE_METRICS = ["accuracy", "completeness", "usefulness"]          # 1-5, higher is better
NUMERIC_METRICS = [*SCALE_METRICS, "unsupported_claims", "declined"]  # compared between systems
LENGTH_BIAS_METRICS = ["accuracy", "completeness"]
SUPPORT_VALUES = {"O": 1.0, "△": 0.5, "X": 0.0}
CITATION_PATTERN = re.compile(r"raw_bib_(\d+)")

INSTRUCTIONS = [
    "최종 답변 전문가 평가 시트 (블라인드)",
    "",
    "[answers] 시트: 같은 질문에 대한 여러 시스템의 답변이 무작위 순서로 섞여 있습니다. 어느 시스템인지는 표시하지 않습니다.",
    "",
    "※ 답변의 길이로 판단하지 마십시오. 긴 답변이 더 정확하거나 더 완전한 것은 아닙니다.",
    "   짧아도 질문에 필요한 내용을 틀림없이 담았으면 높은 점수를, 길어도 틀린 내용이나 근거 없는 서술이 있으면 그만큼 낮은 점수를 주십시오.",
    "   소제목·서론·결론 같은 형식이나 문장의 유려함도 점수에 반영하지 않습니다.",
    "",
    "1) declined (Y/N): 답변이 질문에 답하기를 거절(유보)했는가.",
    "   예) Y: \"자료에서 확인되지 않음\", \"제공된 사료만으로는 답할 수 없습니다\"가 답변의 결론인 경우.",
    "   예) N: 일부 내용에 '확인되지 않는다'는 단서를 달았더라도 질문에 대한 답을 실제로 제시한 경우.",
    "2) decline_justified (Y/N/해당없음): 거절이 타당한가. declined=N이면 '해당없음'.",
    "   예) Y: 이동휘가 아우내 장터 시위를 지휘했는지 묻는 질문처럼 전제가 사실이 아니어서, 확인되지 않는다고 답한 경우.",
    "   예) N: 사료에 답이 분명히 있는 질문(예: 특정 시위의 날짜와 주도 인물)인데도 확인되지 않는다고 답한 경우.",
    "3) accuracy (1~5): 답변에 틀린 내용이 있는가. 1=핵심 내용이 틀림, 3=핵심은 맞으나 세부 오류 있음, 5=틀린 내용 없음.",
    "   답변 전체가 거절뿐이면 '해당없음'을 고릅니다 (거절의 타당성은 2번에서 평가).",
    "   예) 5: 서술한 인물·날짜·장소가 모두 맞음. 내용이 적더라도 틀린 것이 없으면 5.",
    "   예) 2: 시위 날짜는 맞지만 주도 인물을 다른 지역 사건의 인물로 잘못 서술함.",
    "4) completeness (1~5): 질문에 답하는 데 필요한 내용을 담았는가. 1=필요한 내용이 거의 없음, 3=절반 정도, 5=빠진 것 없음.",
    "   예) 5: '누가, 언제, 어디서'를 물은 질문에 세 가지를 모두 답함 (두세 문장이어도 5).",
    "   예) 2: 긴 배경 설명은 있으나 질문이 물은 인물의 역할은 언급하지 않음.",
    "   타당한 거절(decline_justified=Y)은 거절 이유를 밝혔으면 5, 부당한 거절은 1로 채점합니다.",
    "5) unsupported_claims (0 이상의 정수): 사료로 뒷받침되지 않는 주장의 개수. 인용 표시가 붙어 있는지와 무관하게 셉니다.",
    "   예) 인용 없이 \"이 시위에는 약 3,000명이 참여했다\"고 썼고 사료에서 확인되지 않으면 1개.",
    "   예) (raw_bib_123)을 인용했지만 그 사료에 해당 내용이 없으면 이것도 1개. 사료로 확인되는 주장은 인용이 없어도 세지 않습니다.",
    "6) usefulness (1~5): 질문한 연구자·관람객에게 실제로 도움이 되는가. 1=쓸모없음, 5=그대로 활용 가능.",
    "   예) 5: 답과 근거 사료를 바로 확인해 인용할 수 있음.",
    "   예) 2: 틀린 내용은 없으나 질문과 관련이 적은 일반적 서술이 대부분임.",
    "",
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
    if args.dataset:
        # Answers saved before `source` was recorded: take it from the benchmark file.
        source_of = {item["id"]: item.get("source", "") for item in json.loads(args.dataset.read_text(encoding="utf-8"))}
        for record in records:
            record["source"] = record.get("source") or source_of.get(record["query_id"], "")
    n_models = len({r["model"] for r in records})
    wanted = {s.strip() for s in args.source.split(",") if s.strip()}
    if wanted:
        if not any(r.get("source") for r in records):
            print("❌ 답변 파일에 source 정보가 없습니다. --dataset 으로 source가 있는 벤치마크 파일을 함께 지정하세요.", file=sys.stderr)
            return 1
        records = [r for r in records if r.get("source") in wanted]
        if not records:
            print(f"❌ source가 {sorted(wanted)} 인 문항이 없습니다.", file=sys.stderr)
            return 1
    by_question: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        by_question[record["query_id"]].append(record)

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
            key[item_id] = {"query_id": query_id, "model": record["model"], "category": record.get("category", ""),
                            "source": record.get("source", ""), "answer_chars": len(record["response"])}
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
                  choices={c: values for c, values in RATING_CHOICES.items() if values})
        add_sheet(wb, "citations", ["cite_id", "item_id", "claim", "cited_source", "source_text", "supports", "note"], citation_rows,
                  widths={"cite_id": 16, "item_id": 14, "claim": 60, "source_text": 100, "note": 30},
                  choices={"supports": list(SUPPORT_VALUES)})
        path = args.out_dir / f"rating_{rater}.xlsx"
        wb.save(path)
        print(f"📝 {path}")
    if args.key_dir.resolve() == args.out_dir.resolve():
        print("❌ --key-dir 는 평가자 시트 폴더(--out-dir)와 달라야 합니다.", file=sys.stderr)
        return 1
    args.key_dir.mkdir(parents=True, exist_ok=True)
    key_path = args.key_dir / "blinding_key.json"
    key_path.write_text(json.dumps(key, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"🔑 {key_path}  (평가자에게 전달하지 마세요. 평가자에게는 {args.out_dir} 폴더만 전달합니다)")
    if wanted:
        print(f"🔎 source 필터: {sorted(wanted)}")
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
    key_path = args.key or next((p for p in (KEY_DIR / "blinding_key.json", OUT_DIR / "blinding_key.json") if p.exists()),
                                KEY_DIR / "blinding_key.json")
    key = json.loads(key_path.read_text(encoding="utf-8"))
    raters = {path.stem: path for path in args.files}

    # ratings[metric][rater][item_id]
    ratings: dict[str, dict[str, dict[str, float]]] = {m: defaultdict(dict) for m in RATING_COLUMNS}
    supports: dict[str, dict[str, str]] = defaultdict(dict)
    cite_item: dict[str, str] = {}
    answer_chars: dict[str, int] = {item_id: v["answer_chars"] for item_id, v in key.items() if "answer_chars" in v}
    invalid_cells = 0
    for rater, path in raters.items():
        for row in read_sheet(path, "answers"):
            answer_chars.setdefault(row["item_id"], len(row.get("answer", "")))
            for metric in RATING_COLUMNS:
                cell = row.get(metric, "").upper()
                if metric in ("declined", "decline_justified"):
                    value = {YES: 1.0, NO: 0.0}.get(cell)
                else:
                    low, high = (0, 10**6) if metric == "unsupported_claims" else (1, 5)
                    value = float(cell) if re.fullmatch(r"\d+(\.0+)?", cell) and low <= float(cell) <= high else None
                if value is not None:
                    ratings[metric][rater][row["item_id"]] = value
                elif cell and cell != NA:
                    invalid_cells += 1
        for row in read_sheet(path, "citations"):
            cite_item[row["cite_id"]] = row["item_id"]
            if row["supports"] in SUPPORT_VALUES:
                supports[rater][row["cite_id"]] = row["supports"]

    models = list(dict.fromkeys(v["model"] for v in key.values()))
    report: dict[str, Any] = {"n_raters": len(raters), "n_items": len(key), "n_questions": len({v["query_id"] for v in key.values()}),
                              "models": {}, "significance": [], "agreement": {}, "length_bias": {}}

    # Per-question score of a model = mean over raters.
    per_question: dict[str, dict[str, dict[str, float]]] = {m: defaultdict(dict) for m in RATING_COLUMNS}
    for metric in RATING_COLUMNS:
        collected: dict[tuple[str, str], list[float]] = defaultdict(list)
        for rater_scores in ratings[metric].values():
            for item_id, score in rater_scores.items():
                collected[(key[item_id]["model"], key[item_id]["query_id"])].append(score)
        for (model, query_id), values in collected.items():
            per_question[metric][model][query_id] = sum(values) / len(values)

    print(f"평가자 {len(raters)}명, 질문 {report['n_questions']}건, 답변 {len(key)}개")
    print("declined·decline_justified는 비율(0~1), unsupported_claims는 답변당 개수, 나머지는 1~5점 평균입니다.")
    print(f"{'model':<42} {'metric':<18} {'n':>4} {'mean':>6}  95% CI")
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
    for metric in NUMERIC_METRICS:
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
            # Ordinal scales and counts: quadratic-weighted kappa. Y/N columns: plain kappa.
            weights = None if metric in ("declined", "decline_justified") else "quadratic"
            entry[metric] = {"n": len(shared), "weighted_kappa" if weights else "kappa": cohen_kappa(
                [int(ratings[metric][rater_a][i]) for i in shared], [int(ratings[metric][rater_b][i]) for i in shared], weights=weights)}
        shared = sorted(set(supports[rater_a]) & set(supports[rater_b]))
        entry["citation_supports"] = {"n": len(shared), "kappa": cohen_kappa(
            [supports[rater_a][i] for i in shared], [supports[rater_b][i] for i in shared])}
        report["agreement"][f"{rater_a} vs {rater_b}"] = entry
        print(f"\n🤝 {rater_a} vs {rater_b}: " + ", ".join(
            f"{name} κ={_fmt(v.get('weighted_kappa', v.get('kappa')))} (n={v['n']})" for name, v in entry.items()))

    # Length bias: do longer answers get higher ratings? (item score = mean over raters)
    print("\n📏 길이 편향 점검: 답변 길이(글자 수)와 점수의 상관계수")
    for metric in LENGTH_BIAS_METRICS:
        by_item: dict[str, list[float]] = defaultdict(list)
        for rater_scores in ratings[metric].values():
            for item_id, score in rater_scores.items():
                by_item[item_id].append(score)
        items = sorted(i for i in by_item if i in answer_chars)
        lengths = [float(answer_chars[i]) for i in items]
        scores = [sum(by_item[i]) / len(by_item[i]) for i in items]
        defined = len(items) >= 3 and len(set(lengths)) > 1 and len(set(scores)) > 1
        pearson = float(np.corrcoef(lengths, scores)[0, 1]) if defined else float("nan")
        report["length_bias"][metric] = {"n": len(items), "pearson_r": pearson}
        print(f"  {metric:<14} Pearson r={_fmt(pearson)} (n={len(items)})")
    print("  (|r|이 크면 점수가 길이에 좌우되고 있다는 신호입니다. 시스템별 평균 길이가 다르면 해석에 유의하세요.)")
    if invalid_cells:
        print(f"\n⚠️ 허용되지 않는 값이 들어 있어 무시한 칸: {invalid_cells}개")
        report["invalid_cells"] = invalid_cells

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
    p_export.add_argument("--out-dir", type=Path, default=OUT_DIR, help="Folder for the rater workbooks (default: evaluation/human_eval)")
    p_export.add_argument("--key-dir", type=Path, default=KEY_DIR,
                          help="Folder for blinding_key.json; must differ from --out-dir (default: evaluation/human_eval_key)")
    p_export.add_argument("--source", default="",
                          help="Rate only questions from these sources, e.g. curated,third_party (default: all)")
    p_export.add_argument("--dataset", type=Path, default=None,
                          help="Benchmark JSON with a `source` field, for answer files that do not record the source")
    p_export.set_defaults(func=cmd_export)

    p_score = sub.add_parser("score", help="Score completed rating workbooks")
    p_score.add_argument("files", nargs="+", type=Path)
    p_score.add_argument("--key", type=Path, default=None,
                         help="blinding_key.json (default: evaluation/human_eval_key/, then the legacy evaluation/human_eval/)")
    p_score.add_argument("--output", type=Path, default=EVAL_DIR / "results" / "human_eval_results.json")
    p_score.set_defaults(func=cmd_score)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
