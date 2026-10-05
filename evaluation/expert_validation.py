#!/usr/bin/env python3
"""Expert (historian) validation of entity and relation extraction.

The automatic NER/RE benchmark scores the pipeline against its own TEI tags,
so it cannot measure real precision or recall. This script produces an
independent, human-judged estimate instead:

  export  Draw a seeded random sample of source records and write one Excel
          workbook per annotator listing everything the pipeline extracted.
          The annotator marks each item O/X and adds what the pipeline missed.
  score   Read the completed workbooks and report precision / recall / F1 with
          bootstrap confidence intervals, plus Cohen's kappa between annotators.
          The report states the sample size and repeats precision / recall
          per source table (raw_event_info, raw_source_info, ...).

Records are sampled from all rows that have TEI, including rows where the
pipeline extracted nothing; otherwise recall would be overestimated.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from annotation_io import add_sheet, new_workbook, read_sheet
from stats_utils import cohen_kappa, prf

from scripts.config import get_pg_connection

OUT_DIR = Path(__file__).resolve().parent / "expert_validation"
# table -> column holding the event id of the row (None: no event link, entities only)
SAMPLE_TABLES = {
    "raw_event_info": "아이디",
    "raw_source_info": "사건아이디",
    "raw_bibliography": None,
    "raw_detail_place": None,
}
TYPES = ["Person", "Place", "P14_carried_out_by", "P7_took_place_at"]
ENTITY_TYPES = {"Person", "Place"}
VERDICT_OK, VERDICT_WRONG = "O", "X"

INSTRUCTIONS = [
    "개체·관계 추출 전문가 검증 시트",
    "",
    "1. [records] 시트에서 사료 원문(text)을 읽습니다.",
    "2. [extracted] 시트는 파이프라인이 해당 사료에서 추출한 인명·지명·관계입니다. 각 행의 verdict에 O(맞음) 또는 X(틀림)를 기입합니다.",
    "   - Person: 실제 사람 이름이면 O. 기관·직책·지명·일반명사이거나 이름의 경계가 틀리면 X.",
    "   - Place: 실제 지명이면 O.",
    "   - P14_carried_out_by: 그 인물이 해당 사건의 행위자·참여자로 사료에 기록되어 있으면 O. 단순 언급(수신자, 재판관 등)이면 X.",
    "   - P7_took_place_at: 그 사건이 해당 장소에서 일어난 것으로 사료에 기록되어 있으면 O.",
    "   - X인 경우 corrected에 올바른 값을 적어 주시면 오류 유형 분석에 사용합니다 (선택).",
    "3. [missed] 시트에는 사료에 있는데 파이프라인이 놓친 인명·지명·관계를 한 행에 하나씩 추가합니다 (record_id, type, value).",
    "4. 다른 검증자와 상의하지 말고 독립적으로 판정해 주십시오. item_id와 record_id는 수정하지 마십시오.",
]


def _surface(inner: str) -> str:
    """Readable form of a tagged span, e.g. '金容圭 (김용규)'."""
    foreign = "".join(re.findall(r"<foreign[^>]*>([^<]+)</foreign>", inner)).strip()
    gloss = "".join(re.findall(r"<gloss>([^<]+)</gloss>", inner)).strip()
    if foreign and gloss:
        return f"{foreign} ({gloss})"
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", inner)).strip()


def _body_text(tei: str) -> str:
    body = re.search(r"<body>(.*?)</body>", tei, flags=re.DOTALL)
    # Drop glosses so the annotator reads the source as written.
    text = re.sub(r"<gloss>[^<]*</gloss>", "", body.group(1) if body else tei)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text)).strip()


def sample_records(n: int, seed: int) -> list[dict[str, Any]]:
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute('SELECT "아이디", "사건명" FROM raw_event_info')
    event_titles = {str(i): str(t or "").strip() for i, t in cur.fetchall()}

    per_table = -(-n // len(SAMPLE_TABLES))
    records = []
    for table, event_col in SAMPLE_TABLES.items():
        event_expr = f'"{event_col}"' if event_col else "NULL"
        cur.execute(
            f'SELECT rowid, tei, {event_expr} FROM "{table}" WHERE tei IS NOT NULL '
            f"ORDER BY md5(rowid::text || %s) LIMIT %s",
            (str(seed), per_table),
        )
        for rowid, tei, event_id in cur.fetchall():
            event_id = str(event_id).strip() if event_id else ""
            records.append({
                "record_id": f"{table}:{rowid}",
                "table": table,
                "event_id": event_id,
                "event_title": event_titles.get(event_id, ""),
                "tei": tei,
            })
    cur.close()
    conn.close()
    return records[:n]


def extract_items(record: dict[str, Any]) -> list[dict[str, str]]:
    """Entities tagged in the TEI and the event relations the graph builder derives from them."""
    tei = record["tei"]
    persons = list(dict.fromkeys(_surface(m) for m in re.findall(r"<persName[^>]*>(.*?)</persName>", tei, flags=re.DOTALL)))
    places = list(dict.fromkeys(_surface(m) for m in re.findall(r"<placeName[^>]*>(.*?)</placeName>", tei, flags=re.DOTALL)))
    items = [{"type": "Person", "value": p} for p in persons if p]
    items += [{"type": "Place", "value": p} for p in places if p]
    if record["event_id"]:
        event = record["event_title"] or record["event_id"]
        items += [{"type": "P14_carried_out_by", "value": f"[{event}] → {p}"} for p in persons if p]
        items += [{"type": "P7_took_place_at", "value": f"[{event}] → {p}"} for p in places if p]
    return items


def cmd_export(args: argparse.Namespace) -> int:
    records = sample_records(args.n, args.seed)
    record_rows, item_rows = [], []
    for record in records:
        record_rows.append({
            "record_id": record["record_id"], "table": record["table"],
            "event_title": record["event_title"], "text": _body_text(record["tei"]),
        })
        for n, item in enumerate(extract_items(record), 1):
            item_rows.append({
                "item_id": f"{record['record_id']}/{n}", "record_id": record["record_id"],
                "kind": "entity" if item["type"] in ENTITY_TYPES else "relation", **item,
            })

    args.out_dir.mkdir(parents=True, exist_ok=True)
    for annotator in args.annotators:
        wb = new_workbook(INSTRUCTIONS)
        add_sheet(wb, "records", ["record_id", "table", "event_title", "text"], record_rows,
                  widths={"record_id": 26, "event_title": 34, "text": 110})
        add_sheet(wb, "extracted", ["item_id", "record_id", "kind", "type", "value", "verdict", "corrected", "note"], item_rows,
                  widths={"item_id": 28, "record_id": 26, "type": 22, "value": 46, "corrected": 30, "note": 30},
                  choices={"verdict": [VERDICT_OK, VERDICT_WRONG]})
        add_sheet(wb, "missed", ["record_id", "type", "value", "note"], [],
                  widths={"record_id": 26, "type": 22, "value": 46, "note": 30}, choices={"type": TYPES})
        path = args.out_dir / f"annotation_{annotator}.xlsx"
        wb.save(path)
        print(f"📝 {path}")

    manifest = {
        "seed": args.seed, "n_records": len(records), "n_items": len(item_rows),
        "records_without_extraction": sum(1 for r in records if not extract_items(r)),
        "record_ids": [r["record_id"] for r in records],
    }
    (args.out_dir / "sample_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ 사료 {len(records)}건, 추출 항목 {len(item_rows)}개 (추출 0건인 사료 {manifest['records_without_extraction']}건 포함)")
    return 0


def _load_annotation(path: Path) -> dict[str, Any]:
    verdicts = {}
    unjudged = 0
    counts: dict[str, dict[str, dict[str, int]]] = defaultdict(lambda: defaultdict(lambda: {"tp": 0, "fp": 0, "fn": 0}))
    for row in read_sheet(path, "extracted"):
        verdict = row["verdict"].upper()
        if verdict not in (VERDICT_OK, VERDICT_WRONG):
            unjudged += 1
            continue
        verdicts[row["item_id"]] = verdict
        counts[row["type"]][row["record_id"]]["tp" if verdict == VERDICT_OK else "fp"] += 1
    for row in read_sheet(path, "missed"):
        if row["type"] not in TYPES:
            raise ValueError(f"{path} [missed]: 알 수 없는 type '{row['type']}' (record {row['record_id']})")
        counts[row["type"]][row["record_id"]]["fn"] += 1
    records = [r["record_id"] for r in read_sheet(path, "records")]
    return {"verdicts": verdicts, "counts": counts, "records": records, "unjudged": unjudged}


def _score_type(per_record: dict[str, dict[str, int]], records: list[str], n_boot: int = 5000) -> dict[str, Any]:
    """Micro P/R/F1 with a bootstrap CI that resamples source records."""
    table = np.asarray([[per_record[r]["tp"], per_record[r]["fp"], per_record[r]["fn"]] if r in per_record else [0, 0, 0]
                        for r in records])
    tp, fp, fn = (int(x) for x in table.sum(axis=0))
    precision, recall, f1 = prf(tp, fp, fn)
    rng = np.random.default_rng(0)
    sums = table[rng.integers(0, len(records), size=(n_boot, len(records)))].sum(axis=1).astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        boot_p = sums[:, 0] / (sums[:, 0] + sums[:, 1])
        boot_r = sums[:, 0] / (sums[:, 0] + sums[:, 2])

    def interval(samples: np.ndarray) -> list[float | None]:
        samples = samples[~np.isnan(samples)]
        return [round(float(q), 4) for q in np.quantile(samples, [0.025, 0.975])] if samples.size else [None, None]

    def clean(value: float) -> float | None:
        return None if value != value else round(value, 4)

    return {"tp": tp, "fp": fp, "fn": fn, "precision": clean(precision), "precision_ci95": interval(boot_p),
            "recall": clean(recall), "recall_ci95": interval(boot_r), "f1": clean(f1)}


def _table_of(record_id: str) -> str:
    return record_id.rsplit(":", 1)[0]


def cmd_score(args: argparse.Namespace) -> int:
    annotations = {path.stem: _load_annotation(path) for path in args.files}
    report: dict[str, Any] = {"annotators": {}, "agreement": {}}
    fmt = lambda v: "   n/a" if v is None else f"{v * 100:6.1f}"
    for name, ann in annotations.items():
        merged = {"Entity (all)": defaultdict(lambda: {"tp": 0, "fp": 0, "fn": 0}),
                  "Relation (all)": defaultdict(lambda: {"tp": 0, "fp": 0, "fn": 0})}
        for type_name in TYPES:
            group = "Entity (all)" if type_name in ENTITY_TYPES else "Relation (all)"
            for record_id, c in ann["counts"][type_name].items():
                for key in c:
                    merged[group][record_id][key] += c[key]
        per_type = {**{t: ann["counts"][t] for t in TYPES}, **merged}
        scores = {t: _score_type(per_record, ann["records"]) for t, per_record in per_type.items()}

        # The same scores restricted to the records of one source table.
        tables = list(dict.fromkeys(_table_of(r) for r in ann["records"]))
        by_table = {}
        for table in tables:
            table_records = [r for r in ann["records"] if _table_of(r) == table]
            by_table[table] = {"n_records": len(table_records),
                               "scores": {t: _score_type(per_record, table_records) for t, per_record in per_type.items()}}

        report["annotators"][name] = {
            "n_records": len(ann["records"]),
            "n_records_by_table": {t: by_table[t]["n_records"] for t in tables},
            "n_items_judged": len(ann["verdicts"]),
            "n_items_missed_added": sum(c["fn"] for per_record in ann["counts"].values() for c in per_record.values()),
            "unjudged_items": ann["unjudged"],
            "scores": scores,
            "by_table": by_table,
        }

        print(f"\n■ {name} — 표본: 사료 {len(ann['records'])}건 ("
              + ", ".join(f"{t} {by_table[t]['n_records']}" for t in tables)
              + f"), 판정 항목 {len(ann['verdicts'])}개, 미판정 {ann['unjudged']}개")
        print(f"  {'type':<22} {'P':>7} {'R':>7} {'F1':>7}   TP/FP/FN")
        for type_name, s in scores.items():
            print(f"  {type_name:<22} {fmt(s['precision'])} {fmt(s['recall'])} {fmt(s['f1'])}   {s['tp']}/{s['fp']}/{s['fn']}")
        print(f"  사료 유형(table)별 — {'table':<18} {'n':>4}  {'Entity P':>8} {'Entity R':>8}  {'Rel. P':>7} {'Rel. R':>7}")
        for table in tables:
            e, r = by_table[table]["scores"]["Entity (all)"], by_table[table]["scores"]["Relation (all)"]
            print(f"                      {table:<18} {by_table[table]['n_records']:>4}  "
                  f"{fmt(e['precision']):>8} {fmt(e['recall']):>8}  {fmt(r['precision']):>7} {fmt(r['recall']):>7}")
        if ann["unjudged"]:
            print(f"  ⚠️ verdict가 비어 있는 {ann['unjudged']}개 항목은 계산에서 제외했습니다.")

    names = list(annotations)
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            a, b = annotations[names[i]]["verdicts"], annotations[names[j]]["verdicts"]
            shared = sorted(set(a) & set(b))
            kappa = cohen_kappa([a[k] for k in shared], [b[k] for k in shared])
            raw = sum(a[k] == b[k] for k in shared) / len(shared) if shared else float("nan")
            report["agreement"][f"{names[i]} vs {names[j]}"] = {
                "n_items": len(shared), "raw_agreement": None if raw != raw else round(raw, 4),
                "cohen_kappa": None if kappa != kappa else round(kappa, 4),
            }
            print(f"\n🤝 {names[i]} vs {names[j]}: 공통 {len(shared)}개, 단순 일치율 {raw:.3f}, Cohen's κ {kappa:.3f}")
            print("   (missed 시트는 자유 기입이라 κ에 포함되지 않습니다. 재현율 차이로 확인하세요.)")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n💾 {args.output}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Expert validation of entity/relation extraction.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_export = sub.add_parser("export", help="Sample records and write annotation workbooks")
    p_export.add_argument("--n", type=int, default=150, help="Number of source records (default: 150)")
    p_export.add_argument("--seed", type=int, default=42)
    p_export.add_argument("--annotators", nargs="+", default=["A", "B"], help="One workbook per annotator (default: A B)")
    p_export.add_argument("--out-dir", type=Path, default=OUT_DIR)
    p_export.set_defaults(func=cmd_export)

    p_score = sub.add_parser("score", help="Score completed annotation workbooks")
    p_score.add_argument("files", nargs="+", type=Path)
    p_score.add_argument("--output", type=Path, default=Path(__file__).resolve().parent / "results" / "expert_validation_results.json")
    p_score.set_defaults(func=cmd_score)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
