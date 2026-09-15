#!/usr/bin/env python3
"""Store precomputed vectors on Neo4j nodes without changing source properties."""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import numpy as np
from neo4j import GraphDatabase


ROOT = Path(__file__).resolve().parents[1]


def load_config() -> dict[str, Any]:
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib
    with (ROOT / ".streamlit" / "secrets.toml").open("rb") as file:
        return tomllib.load(file)


def setting(config: dict[str, Any], name: str, default: str = "") -> str:
    return str(os.getenv(name) or config.get(name) or default)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="외부 임베딩을 Neo4j 노드에 저장")
    parser.add_argument("--vector-dir", default=str(ROOT / "vector_store"))
    parser.add_argument("--property", default="embedding")
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def metadata_rows(path: Path):
    with path.open(encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            record = json.loads(line)
            raw_id = str(record.get("id", ""))
            if not raw_id.startswith("neo4j:"):
                raise ValueError(f"Neo4j 내부 ID 형식이 아닙니다: line {line_number}")
            yield raw_id.removeprefix("neo4j:")


def main() -> int:
    args = parse_args()
    if args.batch_size < 1:
        raise ValueError("--batch-size는 1 이상이어야 합니다.")
    if not args.property.isidentifier():
        raise ValueError("--property는 영문 식별자 형태여야 합니다.")

    vector_dir = Path(args.vector_dir)
    manifest = json.loads((vector_dir / "manifest.json").read_text(encoding="utf-8"))
    vectors = np.load(vector_dir / manifest["vectors"], mmap_mode="r")
    node_ids = metadata_rows(vector_dir / manifest["metadata"])
    if vectors.ndim != 2 or vectors.shape[0] != manifest["count"]:
        raise ValueError("manifest와 embeddings.npy의 크기가 일치하지 않습니다.")

    config = load_config()
    uri = setting(config, "NEO4J_URI", "bolt://localhost:7687")
    user = setting(config, "NEO4J_USER", "neo4j")
    password = setting(config, "NEO4J_PASSWORD")
    if not password:
        raise RuntimeError("NEO4J_PASSWORD를 환경변수 또는 secrets.toml에 설정하세요.")

    driver = GraphDatabase.driver(uri, auth=(user, password))
    total_updated = 0
    try:
        driver.verify_connectivity()
        with driver.session() as session:
            version = session.run(
                "CALL dbms.components() YIELD versions RETURN versions[0] AS version"
            ).single()["version"]
            if args.dry_run:
                print(f"확인 완료: Neo4j {version}, 벡터 {vectors.shape}, 저장 속성 {args.property}")
                return 0

            while True:
                rows = []
                for _ in range(args.batch_size):
                    try:
                        node_id = next(node_ids)
                    except StopIteration:
                        break
                    row_index = total_updated + len(rows)
                    rows.append({"element_id": node_id, "vector": vectors[row_index].tolist()})
                if not rows:
                    break

                result = session.run(
                    f"""
                    UNWIND $rows AS row
                    MATCH (n)
                    WHERE elementId(n) = row.element_id
                    SET n.`{args.property}` = row.vector
                    RETURN count(n) AS updated
                    """,
                    rows=rows,
                ).single()
                updated = int(result["updated"])
                total_updated += len(rows)
                if updated != len(rows):
                    raise RuntimeError(
                        f"노드 매칭 개수 불일치: 요청 {len(rows)}개, 반영 {updated}개"
                    )
                print(f"적용: {total_updated}/{vectors.shape[0]}개 노드")
    finally:
        driver.close()

    print(f"Neo4j 저장 완료: {total_updated}개 노드, 속성={args.property}")
    print("기존 노드 속성과 관계는 변경하지 않았습니다.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"실패: {exc}", file=sys.stderr)
        raise SystemExit(1)