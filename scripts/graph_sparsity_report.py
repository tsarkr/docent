#!/usr/bin/env python3
"""Explain how sparse the Event layer of the graph is, and why.

Unit of an Event node: one row of ``raw_event_info`` (one protest/incident,
keyed by its 사건 id). Those nodes carry ``id``. Any :Event node without
``id`` is an artifact of the old CIDOC import, which labelled every ``ex:``
resource (per-row wrappers, E7_Activity, E39_Actor) as an Event without
connecting it to anything.

The same import also produced isolated :Place artifacts; the report lists
isolated nodes for every label.

Default run is read-only and writes evaluation/results/graph_sparsity_report.json.
``--cleanup`` lists the artifact nodes that can be removed; it deletes them only
together with ``--apply``.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.config import get_neo4j_driver, get_pg_connection

REPORT_PATH = ROOT / "evaluation" / "results" / "graph_sparsity_report.json"

# Event-labelled nodes that are not events: no 사건 id, minted from a CIDOC ex: resource.
ARTIFACT_WHERE = "e.id IS NULL AND e.uid STARTS WITH 'ex:'"


def build_report(session) -> dict:
    def rows(cypher: str) -> list[dict]:
        return [r.data() for r in session.run(cypher)]

    total = rows("MATCH (e:Event) RETURN count(e) AS n")[0]["n"]
    isolated = rows("MATCH (e:Event) WHERE NOT (e)--() RETURN count(e) AS n")[0]["n"]

    origins = rows(f"""
        MATCH (e:Event)
        WITH e, CASE
            WHEN e.id IS NOT NULL THEN 'event (raw_event_info 사건 1건)'
            WHEN e.uid STARTS WITH 'ex:activity_' THEN 'artifact: CIDOC E7_Activity (인명 태그 1개당 1노드)'
            WHEN e.uid STARTS WITH 'ex:actor_' THEN 'artifact: CIDOC E39_Actor (인물인데 Event로 분류)'
            WHEN {ARTIFACT_WHERE} THEN 'artifact: CIDOC 행 단위 래퍼 (ex:<table>_<rowid>)'
            ELSE 'other' END AS origin
        RETURN origin, count(e) AS nodes, sum(CASE WHEN NOT (e)--() THEN 1 ELSE 0 END) AS isolated
        ORDER BY nodes DESC
    """)

    degrees = [r["deg"] for r in rows("MATCH (e:Event) WHERE e.id IS NOT NULL RETURN COUNT {(e)--()} AS deg")]
    real = {"nodes": len(degrees)}
    if degrees:
        ordered = sorted(degrees)
        real.update({
            "isolated": sum(1 for d in degrees if d == 0),
            "degree_min": ordered[0],
            "degree_median": statistics.median(ordered),
            "degree_mean": round(statistics.mean(ordered), 2),
            "degree_p90": ordered[int(0.9 * (len(ordered) - 1))],
            "degree_max": ordered[-1],
            "without_person": rows("MATCH (e:Event) WHERE e.id IS NOT NULL AND NOT (e)-[:P14_carried_out_by]->(:Person) RETURN count(e) AS n")[0]["n"],
            "without_place": rows("MATCH (e:Event) WHERE e.id IS NOT NULL AND NOT (e)-[:P7_took_place_at]->(:Place) RETURN count(e) AS n")[0]["n"],
        })
        real["relations"] = rows("MATCH (e:Event)-[r]-() WHERE e.id IS NOT NULL RETURN type(r) AS type, count(r) AS n ORDER BY n DESC")

    by_label = rows("""
        MATCH (n)
        WITH coalesce(labels(n)[0], '(no label)') AS label, COUNT {(n)--()} AS deg
        RETURN label, count(*) AS nodes, sum(CASE WHEN deg = 0 THEN 1 ELSE 0 END) AS isolated
        ORDER BY nodes DESC
    """)

    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute('SELECT count(*), count(DISTINCT "아이디") FROM raw_event_info')
    pg_rows, pg_ids = cur.fetchone()
    cur.close()
    conn.close()

    return {
        "event_unit": "raw_event_info 1행 = 사건 1건 (Event.id = 사건 아이디)",
        "event_nodes_total": total,
        "event_nodes_isolated": isolated,
        "isolated_ratio": round(isolated / total, 4) if total else None,
        "by_origin": origins,
        "all_labels": by_label,
        "real_events": real,
        "source": {"raw_event_info_rows": pg_rows, "raw_event_info_distinct_ids": pg_ids},
    }


def print_report(report: dict) -> None:
    total = report["event_nodes_total"]
    print(f"Event 단위: {report['event_unit']}")
    print(f"Event 노드 {total:,}개 중 고립 {report['event_nodes_isolated']:,}개 ({(report['isolated_ratio'] or 0) * 100:.1f}%)\n")
    print(f"{'출처':<52} {'노드':>8} {'고립':>8}")
    for row in report["by_origin"]:
        print(f"{row['origin']:<52} {row['nodes']:>8,} {row['isolated']:>8,}")
    print(f"\n{'레이블':<20} {'노드':>8} {'고립':>8}")
    for row in report["all_labels"]:
        print(f"{row['label']:<20} {row['nodes']:>8,} {row['isolated']:>8,}")
    real = report["real_events"]
    if real.get("nodes"):
        print(f"\n실제 사건 {real['nodes']:,}개 (원본 raw_event_info {report['source']['raw_event_info_rows']:,}행, "
              f"고유 아이디 {report['source']['raw_event_info_distinct_ids']:,}개)")
        print(f"  고립 {real['isolated']:,}개, 인물 미연결 {real['without_person']:,}개, 장소 미연결 {real['without_place']:,}개")
        print(f"  차수: 최소 {real['degree_min']}, 중앙값 {real['degree_median']}, 평균 {real['degree_mean']}, "
              f"p90 {real['degree_p90']}, 최대 {real['degree_max']}")


def cleanup(session, apply: bool) -> None:
    # Restricted to isolated nodes so nothing connected is ever removed.
    target = f"MATCH (e:Event|Place) WHERE {ARTIFACT_WHERE} AND NOT (e)--()"
    count = session.run(f"{target} RETURN count(e) AS n").single()["n"]
    if not apply:
        print(f"\n[dry-run] 삭제 대상 고립 artifact 노드: {count:,}개. 실제로 지우려면 --cleanup --apply 를 사용하세요.")
        return
    deleted = 0
    while True:
        n = session.run(f"{target} WITH e LIMIT 20000 DELETE e RETURN count(*) AS n").single()["n"]
        if not n:
            break
        deleted += n
        print(f"  삭제 진행: {deleted:,}/{count:,}")
    print(f"\n🧹 고립 artifact 노드 {deleted:,}개를 삭제했습니다.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Report (and optionally clean) Event-layer sparsity in Neo4j.")
    parser.add_argument("--output", type=Path, default=REPORT_PATH)
    parser.add_argument("--cleanup", action="store_true", help="List isolated CIDOC artifact nodes mislabelled as Event/Place")
    parser.add_argument("--apply", action="store_true", help="With --cleanup: actually delete them (irreversible)")
    args = parser.parse_args()
    if args.apply and not args.cleanup:
        parser.error("--apply 는 --cleanup 과 함께 사용해야 합니다.")

    driver = get_neo4j_driver()
    try:
        with driver.session() as session:
            report = build_report(session)
            print_report(report)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"\n💾 {args.output}")
            if args.cleanup:
                cleanup(session, args.apply)
    finally:
        driver.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
