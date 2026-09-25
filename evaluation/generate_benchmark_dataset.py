#!/usr/bin/env python3
"""Automated Benchmark Dataset Generator for Historical Knowledge Graph & RAG.

Generates a representative, academically grounded quantitative evaluation benchmark:
1. Multi-hop Chaining (40%): Causal & sequential relations across linked events/documents
2. Spatial-Temporal Diffusion (30%): Regional movement of 3.1 movement across provinces
3. Zero-Correlation Trap (30%): Hallucination & cross-attribution traps

Sources:
- PostgreSQL raw_event_info (2,616 events) & raw_source_info
- Neo4j Knowledge Graph topology
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.config import get_neo4j_driver, get_pg_connection
from scripts.hanja_utils import translate_hanja_name

DATASET_PATH = Path(__file__).resolve().parent / "dataset" / "multihop_benchmark_dataset.json"


def clean_name(raw: str) -> str:
    """Clean person or place name, translating Hanja if present."""
    c = re.sub(r'\(.*?\)', '', raw)
    c = re.sub(r'\[.*?\]', '', c)
    c = re.sub(r'[\u4e00-\u9fff]+', lambda m: translate_hanja_name(m.group(0)), c)
    c = re.sub(r'[^\w가-힣]', '', c).strip()
    return c


def load_events_from_pg() -> list[dict[str, Any]]:
    conn = get_pg_connection()
    cur = conn.cursor()
    query = """
        SELECT "아이디", "사건명", "시위_시작일자", "시위_종료일자", "시위_행정구역명", "관련인물", "관련시위_아이디"
        FROM raw_event_info
        WHERE "사건명" IS NOT NULL AND "시위_행정구역명" IS NOT NULL
        ORDER BY "시위_시작일자" ASC NULLS LAST, "아이디" ASC
    """
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    events = []
    for r in rows:
        ev_id, title, s_date, e_date, place, persons_raw, rel_ids = r
        persons = []
        if persons_raw:
            raw_tokens = re.split(r'[;, \t\n]+', str(persons_raw))
            for tok in raw_tokens:
                c = clean_name(tok)
                if len(c) in (2, 3, 4) and c not in persons:
                    persons.append(c)

        clean_place = re.sub(r'[\u4e00-\u9fff]+', lambda m: translate_hanja_name(m.group(0)), str(place))
        clean_place = re.sub(r'\s+', ' ', clean_place).strip()

        events.append({
            "id": str(ev_id),
            "title": str(title).strip(),
            "start_date": str(s_date) if s_date else "",
            "end_date": str(e_date) if e_date else "",
            "place": clean_place,
            "persons": persons,
            "related_event_ids": [x.strip() for x in str(rel_ids).split(';') if x.strip()] if rel_ids else []
        })
    return events


def generate_benchmark_queries(target_count: int = 60) -> list[dict[str, Any]]:
    events = load_events_from_pg()
    ev_by_id = {e["id"]: e for e in events}

    # 1. Load existing curated high-quality queries first (13 queries)
    existing = []
    if DATASET_PATH.exists():
        try:
            existing = json.loads(DATASET_PATH.read_text(encoding="utf-8"))
        except Exception:
            existing = []

    # Preserve the existing curated queries up to 13
    curated = existing[:13]
    generated = list(curated)
    seen_queries = {item["query"] for item in generated}

    # Count how many of each category we need
    needed_total = target_count - len(generated)
    target_mh = int(needed_total * 0.40)
    target_st = int(needed_total * 0.30)
    target_zc = needed_total - target_mh - target_st

    print(f"📊 [데이터셋 생성] 기존 엄선 질의: {len(curated)}건, 추가 생성 목표: {needed_total}건 (MH:{target_mh}, ST:{target_st}, ZC:{target_zc})")

    # -------------------------------------------------------------------------
    # Category 1: Multi-hop Chaining (연계 사건 간 인과 및 주동자 추론)
    # -------------------------------------------------------------------------
    mh_count = 0
    for ev in events:
        if mh_count >= target_mh:
            break
        if not ev["related_event_ids"] or not ev["persons"]:
            continue

        for r_id in ev["related_event_ids"]:
            target_ev = ev_by_id.get(r_id)
            if not target_ev or not target_ev["persons"] or target_ev["id"] == ev["id"]:
                continue

            # Multi-hop between ev and target_ev
            p1_str = ", ".join(ev["persons"][:3])
            p2_str = ", ".join(target_ev["persons"][:3])
            q_id = f"Q_MH_{len(generated) + 1:02d}"
            query_text = (
                f"'{ev['title']}'(발생지: {ev['place']})과 이에 연계된 '{target_ev['title']}'(발생지: {target_ev['place']})에서 "
                f"각각 활동한 주요 인물들과 그들의 구체적인 역할은 무엇인가?"
            )
            if query_text in seen_queries:
                continue

            item = {
                "id": q_id,
                "category": "Multi-hop Chaining",
                "query": query_text,
                "hops": 2,
                "target_entities": list(set([ev["title"][:15], target_ev["title"][:15], ev["place"].split()[-1], target_ev["place"].split()[-1]] + ev["persons"][:2] + target_ev["persons"][:2])),
                "gold_facts": [
                    f"{ev['title']}에서는 {p1_str} 등이 참여하여 활동을 전개함",
                    f"이와 연계된 {target_ev['title']}에서는 {p2_str} 등이 주도적 역할을 수행함",
                    f"두 사건은 {ev['place']}와 {target_ev['place']} 간의 연계망을 통해 순차적으로 발생함"
                ],
                "negative_traps": [
                    f"{ev['persons'][0]}가 {target_ev['place']}의 {target_ev['title']} 현장을 직접 지휘했다",
                    f"{target_ev['persons'][0]}가 {ev['place']} 사건의 총책임자로 체포되었다"
                ]
            }
            generated.append(item)
            seen_queries.add(query_text)
            mh_count += 1
            break

    # -------------------------------------------------------------------------
    # Category 2: Spatial-Temporal Diffusion (시공간 확산 질의)
    # -------------------------------------------------------------------------
    st_count = 0
    # Group events by broad region / province
    regions = {}
    for ev in events:
        if not ev["persons"] or not ev["start_date"]:
            continue
        prov = ev["place"].split()[0] if ev["place"] else "기타"
        if prov not in regions:
            regions[prov] = []
        regions[prov].append(ev)

    for prov, p_events in regions.items():
        if st_count >= target_st:
            break
        if len(p_events) < 2:
            continue

        e1 = p_events[0]
        e2 = p_events[-1]
        if e1["start_date"] == e2["start_date"] or not e1["persons"] or not e2["persons"]:
            continue

        q_id = f"Q_ST_{len(generated) + 1:02d}"
        query_text = (
            f"1919년 3월 {prov} 지역에서 '{e1['title']}'({e1['start_date']})부터 '{e2['title']}'({e2['start_date']})로 "
            f"만세 운동이 확산되어 나간 경로와 각 시위의 주동 세력은 누구인가?"
        )
        if query_text in seen_queries:
            continue

        p1 = ", ".join(e1["persons"][:2])
        p2 = ", ".join(e2["persons"][:2])
        item = {
            "id": q_id,
            "category": "Spatial-Temporal Diffusion",
            "query": query_text,
            "hops": 2,
            "target_entities": list(set([prov, e1["place"].split()[-1], e2["place"].split()[-1]] + e1["persons"][:2] + e2["persons"][:2])),
            "gold_facts": [
                f"{e1['start_date']} {e1['place']}에서 {p1} 등의 주도로 초기 시위가 발발함",
                f"이후 {e2['start_date']}에 이르러 {e2['place']}에서 {p2} 등에 의해 시위가 확산·계승됨",
                f"{prov} 내에서 초기 거점에서 점차 인근 읍·면 지역으로 시위가 파급됨"
            ],
            "negative_traps": [
                f"{e1['persons'][0]}가 {e2['start_date']} {e2['place']} 시위까지 직접 이동하여 주동했다",
                f"{e2['place']} 시위가 {e1['place']}보다 먼저 발생한 발원지이다"
            ]
        }
        generated.append(item)
        seen_queries.add(query_text)
        st_count += 1

    # -------------------------------------------------------------------------
    # Category 3: Zero-Correlation Trap (교차 귀속 환각 유도 질의)
    # -------------------------------------------------------------------------
    zc_count = 0
    famous_figures = [
        ("김구", "황해도", "상하이 임시정부"),
        ("안창호", "미국 체류", "임시정부 내무총장"),
        ("유관순", "충남 천안 아우내", "공주 영명학교 및 이화학당"),
        ("한용운", "경성 태화관", "백담사 및 만해사상"),
        ("이동휘", "러시아 연해주", "한인사회당"),
        ("신채호", "베이징", "신대한 동맹"),
        ("조만식", "평양 기독교계", "조선물산장려회"),
        ("여운형", "상하이 신한청년당", "파리강화회의 대표 파견")
    ]

    for ev in events:
        if zc_count >= target_zc:
            break
        if not ev["persons"] or not ev["place"]:
            continue

        # Pair a famous figure with a completely unrelated regional event
        fig_name, fig_loc, fig_role = famous_figures[zc_count % len(famous_figures)]
        if fig_name in ev["persons"] or fig_loc in ev["place"]:
            continue

        q_id = f"Q_ZC_{len(generated) + 1:02d}"
        query_text = (
            f"{fig_name}가 {ev['start_date'] if ev['start_date'] else '1919년 3월'} {ev['place']}에서 일어난 "
            f"'{ev['title']}' 현장에서 군중을 직접 인솔하며 만세 시위를 지휘했는가?"
        )
        if query_text in seen_queries:
            continue

        real_leaders = ", ".join(ev["persons"][:3])
        item = {
            "id": q_id,
            "category": "Zero-Correlation Trap",
            "query": query_text,
            "hops": 0,
            "target_entities": [fig_name, ev["place"].split()[-1], ev["title"][:12]] + ev["persons"][:2],
            "gold_facts": [
                f"{fig_name}는 해당 시기 {fig_loc} 등에서 독자적인 활동({fig_role})을 수행 중이었음",
                f"{ev['place']}의 {ev['title']}은 현지 인물인 {real_leaders} 등에 의해 독자적으로 주도됨",
                f"{fig_name}가 {ev['place']} 시위 현장에 직접 나타나 군중을 지휘했다는 것은 사료적 근거가 없는 허위 사실임"
            ],
            "negative_traps": [
                f"{fig_name}가 {ev['place']} 시위대 선두에서 태극기를 흔들다 현장에서 일제 순사에게 연행되었다",
                f"{fig_name}가 {ev['title']}의 격문을 직접 작성하여 배포하였다"
            ]
        }
        generated.append(item)
        seen_queries.add(query_text)
        zc_count += 1

    print(f"✅ 총 {len(generated)}건의 정량 평가 벤치마크 데이터셋 구축 완료!")
    return generated


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate comprehensive quantitative RAG benchmark dataset.")
    parser.add_argument("--target-count", type=int, default=60, help="Target total benchmark dataset size (default: 60)")
    parser.add_argument("--out", type=Path, default=DATASET_PATH, help="Output dataset path")
    args = parser.parse_args()

    dataset = generate_benchmark_queries(target_count=args.target_count)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(dataset, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"💾 데이터셋 파일 저장 완료: {args.out} ({len(dataset)} items)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
