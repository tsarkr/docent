#!/usr/bin/env python3
from __future__ import annotations
"""Read Neo4j graph data and write embeddings to a separate local store.

This script never creates, updates, or deletes Neo4j data. It only executes
read queries and writes the generated vector files under the output directory.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def load_secrets() -> dict[str, Any]:
    secret_path = ROOT / ".streamlit" / "secrets.toml"
    if not secret_path.exists():
        return {}
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib
    try:
        with secret_path.open("rb") as secret_file:
            return tomllib.load(secret_file)
    except Exception as exc:
        print(f"경고: secrets.toml을 읽지 못했습니다: {exc}", file=sys.stderr)
        return {}


SECRETS = load_secrets()


def setting(name: str, default: str = "") -> str:
    return str(os.getenv(name) or SECRETS.get(name) or default)


def json_value(value: Any) -> Any:
    """Convert Neo4j values into JSON-safe values without changing the source."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    return str(value)


def node_key(node: Any) -> str:
    identity = getattr(node, "element_id", None) or getattr(node, "id", None)
    if identity is not None:
        return f"neo4j:{identity}"
    properties = dict(node.items())
    for key in ("uid", "id", "명칭", "name", "title", "제목", "사건명"):
        if properties.get(key) not in (None, ""):
            return f"{','.join(sorted(node.labels))}:{properties[key]}"
    fallback_id = getattr(node, "element_id", None) or getattr(node, "id", "unknown")
    return f"{','.join(sorted(node.labels))}:{fallback_id}"


def node_text(node: Any) -> str:
    properties = dict(node.items())
    labels = ", ".join(sorted(node.labels))
    node_type = properties.get("type") or next(
        (label for label in ("인물", "장소", "사건", "기관", "사료") if label in node.labels),
        labels,
    )
    name = (
        properties.get("name")
        or properties.get("명칭")
        or properties.get("title")
        or properties.get("제목")
        or properties.get("사건명")
        or properties.get("term_id")
        or ""
    )
    fields = [f"[유형: {node_type}] 이름: {name}"]
    for key, value in properties.items():
        if key in {"embedding", "name", "명칭", "title", "제목", "사건명", "type"}:
            continue
        if value in (None, "", [], {}):
            continue
        fields.append(f"{key}: {json_value(value)}")
    return f"레이블: {labels}\n" + "\n".join(fields)


def count_nodes(driver: Any) -> int:
    with driver.session() as session:
        return int(
            session.run(
                "MATCH (n) WHERE NOT n:Thesaurus OR EXISTS((n)--()) "
                "RETURN count(n) AS count"
            ).single()["count"]
        )


def read_graph_batch(session: Any, last_node_id: int, batch_size: int) -> list[dict[str, Any]]:
    query = """
    MATCH (n)
    WHERE id(n) > $last_node_id
    WITH n ORDER BY id(n) LIMIT $batch_size
    WHERE NOT n:Thesaurus OR EXISTS((n)--())
    OPTIONAL MATCH (n)-[r]-(target)
    WITH n, id(n) AS node_id,
         collect(CASE WHEN r IS NULL THEN NULL ELSE {
             relationship: type(r),
             relationship_properties: properties(r),
             target: target
         } END) AS relationships
    RETURN n, node_id, relationships
    ORDER BY node_id
    """
    records = []
    for result in session.run(query, last_node_id=last_node_id, batch_size=batch_size):
        node = result["n"]
        key = node_key(node)
        record = {
            "id": key,
            "labels": sorted(node.labels),
            "properties": json_value(dict(node.items())),
            "text": node_text(node),
            "relationships": [],
        }
        for relation in result["relationships"]:
            if not relation:
                continue
            target = relation["target"]
            target_key = node_key(target)
            relationship = {
                "type": relation["relationship"],
                "properties": json_value(relation["relationship_properties"] or {}),
                "target": target_key,
            }
            record["relationships"].append(relationship)
            record["text"] += (
                f"\n관계: {relation['relationship']} -> "
                f"{node_text(target).replace(chr(10), ' | ')}"
            )
        record["_neo4j_id"] = int(result["node_id"])
        records.append(record)
    return records


def ollama_embeddings(texts: list[str], model: str, base_url: str, batch_size: int) -> Any:
    import numpy as np
    import requests

    vectors = []
    endpoint = f"{base_url.rstrip('/')}/api/embed"
    fallback_endpoint = f"{base_url.rstrip('/')}/api/embeddings"
    for start in range(0, len(texts), batch_size):
        batch = texts[start:start + batch_size]
        response = requests.post(
            endpoint,
            json={"model": model, "input": batch},
            timeout=300,
        )
        if response.status_code == 404:
            for text in batch:
                response = requests.post(
                    fallback_endpoint,
                    json={"model": model, "prompt": text},
                    timeout=300,
                )
                response.raise_for_status()
                vectors.append(response.json()["embedding"])
            continue
        response.raise_for_status()
        payload = response.json()
        vectors.extend(payload.get("embeddings", []))

    if len(vectors) != len(texts):
        raise RuntimeError(f"임베딩 개수 불일치: 문서 {len(texts)}개, 벡터 {len(vectors)}개")
    matrix = np.asarray(vectors, dtype=np.float32)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    return matrix / np.maximum(norms, 1e-12)


def write_store(records: list[dict[str, Any]], vectors: Any, output_dir: Path, model: str) -> None:
    import numpy as np

    output_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = output_dir / "metadata.jsonl"
    vectors_path = output_dir / "embeddings.npy"
    manifest_path = output_dir / "manifest.json"

    with metadata_path.open("w", encoding="utf-8") as metadata_file:
        for record in records:
            metadata_file.write(json.dumps(record, ensure_ascii=False) + "\n")
    np.save(vectors_path, vectors)
    manifest_path.write_text(
        json.dumps(
            {
                "model": model,
                "count": len(records),
                "dimension": int(vectors.shape[1]) if vectors.ndim == 2 else 0,
                "metric": "cosine",
                "metadata": metadata_path.name,
                "vectors": vectors_path.name,
                "neo4j_mutated": False,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Neo4j 읽기 전용 그래프 벡터화")
    parser.add_argument("--output-dir", default=str(ROOT / "vector_store"))
    parser.add_argument("--model", default=setting("OLLAMA_EMBED_MODEL", "nomic-embed-text"))
    parser.add_argument("--ollama-url", default=setting("OLLAMA_HOST", "http://localhost:11434"))
    parser.add_argument("--batch-size", type=int, default=32)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    from neo4j import GraphDatabase
    import numpy as np

    if args.batch_size < 1:
        raise ValueError("--batch-size는 1 이상이어야 합니다.")

    uri = setting("NEO4J_URI", "bolt://localhost:7687")
    user = setting("NEO4J_USER", "neo4j")
    password = setting("NEO4J_PASSWORD")
    if not password:
        raise RuntimeError("NEO4J_PASSWORD를 환경변수 또는 secrets.toml에 설정하세요.")

    driver = GraphDatabase.driver(uri, auth=(user, password))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = output_dir / "metadata.jsonl"
    vectors_path = output_dir / "embeddings.npy"
    temp_metadata_path = output_dir / "metadata.jsonl.tmp"
    temp_vectors_path = output_dir / "embeddings.npy.tmp"
    try:
        driver.verify_connectivity()
        total_nodes = count_nodes(driver)
        if not total_nodes:
            print("Neo4j에서 노드를 찾지 못했습니다.")
            return 0

        written = 0
        last_node_id = -1
        vectors = None
        with temp_metadata_path.open("w", encoding="utf-8") as metadata_file:
            with driver.session() as session:
                while written < total_nodes:
                    records = read_graph_batch(session, last_node_id, args.batch_size)
                    if not records:
                        break
                    batch_vectors = ollama_embeddings(
                        [record["text"] for record in records],
                        args.model,
                        args.ollama_url,
                        args.batch_size,
                    )
                    if vectors is None:
                        vectors = np.lib.format.open_memmap(
                            temp_vectors_path,
                            mode="w+",
                            dtype="float32",
                            shape=(total_nodes, batch_vectors.shape[1]),
                        )
                    end = written + len(records)
                    vectors[written:end] = batch_vectors
                    last_node_id = max(
                        int(record["_neo4j_id"])
                        for record in records
                        if record.get("_neo4j_id") is not None
                    )
                    for record in records:
                        record.pop("_neo4j_id", None)
                        metadata_file.write(json.dumps(record, ensure_ascii=False) + "\n")
                    written = end
                    print(f"진행: {written}/{total_nodes}개 노드")

        if written != total_nodes or vectors is None:
            raise RuntimeError(f"노드 처리 개수 불일치: {written}/{total_nodes}")
        vectors.flush()
        del vectors
        os.replace(temp_metadata_path, metadata_path)
        os.replace(temp_vectors_path, vectors_path)
        dimension = int(batch_vectors.shape[1])
        (output_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "model": args.model,
                    "count": written,
                    "dimension": dimension,
                    "metric": "cosine",
                    "metadata": metadata_path.name,
                    "vectors": vectors_path.name,
                    "neo4j_mutated": False,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    finally:
        driver.close()
    print(f"벡터 저장 완료: {output_dir} ({written} x {dimension})")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit("중단되었습니다.")
    except Exception as exc:
        print(f"실패: {exc}", file=sys.stderr)
        raise SystemExit(1)