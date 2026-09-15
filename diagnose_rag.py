#!/usr/bin/env python3
"""Diagnose Neo4j GraphRAG retrieval and embedding compatibility.

This script is read-only against Neo4j. It checks:
1. Neo4j vector-index metadata and target labels/properties.
2. The Thesaurus node named ``천도구국단`` and its stored vector.
3. Ollama query-vector compatibility and direct cosine similarity.
4. The same ``namesIndex`` full-text retrieval path used by docent.php.
5. Any online vector index that targets Thesaurus.embedding.

The PHP application currently retrieves graph nodes through namesIndex. It does
not call a Neo4j vector index from its graph action, so both retrieval paths
are printed separately when a vector index is available.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parent
DEFAULT_QUERY = "보성사에서 손병희를 명예총재로, 이종일을 단장으로 결성된 천도교 항일비밀결사는?"
TARGET_NAME = "천도구국단"
DEFAULT_OLLAMA_URL = "http://localhost:11434"
DEFAULT_OLLAMA_MODEL = "nomic-embed-text"
VECTOR_DIMENSION = 768
TOP_K = 5


def load_secrets() -> dict[str, Any]:
    path = ROOT / ".streamlit" / "secrets.toml"
    if not path.exists():
        return {}
    try:
        try:
            import tomllib
        except ModuleNotFoundError:
            import tomli as tomllib
        with path.open("rb") as secret_file:
            values = tomllib.load(secret_file)
        return values if isinstance(values, dict) else {}
    except Exception as exc:
        print(f"경고: secrets.toml을 읽지 못했습니다: {exc}", file=sys.stderr)
        return {}


SECRETS = load_secrets()


def setting(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value:
        return value
    secret_value = SECRETS.get(name)
    if secret_value is not None and str(secret_value):
        return str(secret_value)
    return default


def section(title: str) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")


def display(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (list, tuple)):
        return str(list(value))
    return str(value)


def cosine_similarity(left: list[float], right: list[float]) -> float:
    if len(left) != len(right):
        raise ValueError(f"벡터 차원 불일치: query={len(left)}, node={len(right)}")
    left_norm = math.sqrt(sum(float(item) ** 2 for item in left))
    right_norm = math.sqrt(sum(float(item) ** 2 for item in right))
    if left_norm == 0 or right_norm == 0:
        raise ValueError("영벡터에는 코사인 유사도를 계산할 수 없습니다.")
    return sum(float(a) * float(b) for a, b in zip(left, right)) / (left_norm * right_norm)


def ollama_embedding(ollama_url: str, model: str, text: str) -> list[float]:
    base_url = ollama_url.rstrip("/")
    try:
        response = requests.post(
            f"{base_url}/api/embed",
            json={"model": model, "input": [text]},
            timeout=300,
        )
        if response.status_code != 404:
            response.raise_for_status()
            body = response.json()
            embeddings = body.get("embeddings")
            if isinstance(embeddings, list) and len(embeddings) == 1:
                vector = embeddings[0]
                if isinstance(vector, list) and vector:
                    return [float(item) for item in vector]
            raise RuntimeError("Ollama /api/embed 응답에 단일 벡터가 없습니다.")

        response = requests.post(
            f"{base_url}/api/embeddings",
            json={"model": model, "prompt": text},
            timeout=300,
        )
        response.raise_for_status()
        vector = response.json().get("embedding")
        if not isinstance(vector, list) or not vector:
            raise RuntimeError("Ollama /api/embeddings 응답에 벡터가 없습니다.")
        return [float(item) for item in vector]
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Ollama 연결 실패: {ollama_url}. ollama serve와 모델 설치를 확인하세요."
        ) from exc


def show_vector_indexes(session: Any) -> list[dict[str, Any]]:
    section("1. DB 벡터 인덱스 현황")
    query = """
    SHOW VECTOR INDEXES
    YIELD name, state, type, entityType, labelsOrTypes, properties,
          dimensions, similarityFunction
    RETURN name, state, type, entityType, labelsOrTypes, properties,
           dimensions, similarityFunction
    ORDER BY name
    """
    fallback_query = """
    SHOW VECTOR INDEXES
    YIELD name, state, type, entityType, labelsOrTypes, properties, indexConfig
    RETURN name, state, type, entityType, labelsOrTypes, properties, indexConfig
    ORDER BY name
    """
    wildcard_query = """
    SHOW VECTOR INDEXES
    YIELD *
    RETURN *
    """
    try:
        rows = [record.data() for record in session.run(query)]
    except Exception as exc:
        print(f"신규 벡터 인덱스 컬럼 조회 실패, indexConfig 형식으로 재시도: {exc}")
        try:
            rows = [record.data() for record in session.run(fallback_query)]
        except Exception as fallback_exc:
            print(f"구형 벡터 인덱스 컬럼 조회 실패, 전체 컬럼으로 재시도: {fallback_exc}")
            try:
                rows = [record.data() for record in session.run(wildcard_query)]
            except Exception as wildcard_exc:
                print(f"벡터 인덱스 조회 실패: {wildcard_exc}")
                return []

    if not rows:
        print("활성 또는 등록된 벡터 인덱스가 없습니다.")
        return []
    for row in rows:
        config = row.get("indexConfig") or {}
        if row.get("dimensions") is None:
            row["dimensions"] = config.get("vector.dimensions")
        if row.get("similarityFunction") is None:
            row["similarityFunction"] = config.get("vector.similarity_function")
        labels = row.get("labelsOrTypes") or []
        properties = row.get("properties") or []
        print(
            f"- name={display(row.get('name'))}, state={display(row.get('state'))}, "
            f"entityType={display(row.get('entityType'))}, "
            f"labelsOrTypes={display(labels)}, properties={display(properties)}, "
            f"dimensions={display(row.get('dimensions'))}, "
            f"similarityFunction={display(row.get('similarityFunction'))}"
        )
        if "Thesaurus" in labels:
            print("  [확인] Thesaurus 라벨이 이 벡터 인덱스 대상입니다.")
        else:
            print("  [주의] Thesaurus 라벨이 이 벡터 인덱스 대상에 없습니다.")
        if "embedding" not in properties:
            print("  [주의] embedding 속성이 이 벡터 인덱스 대상 속성에 없습니다.")
    return rows


def inspect_target_node(session: Any) -> tuple[list[float] | None, dict[str, Any] | None]:
    section("2. 타깃 Thesaurus 노드 임베딩 검증")
    query = """
    MATCH (t:Thesaurus {name: $name})
    RETURN t.name AS name, size(t.embedding) AS dim,
           t.description AS description, t.embedding AS embedding,
           labels(t) AS labels
    """
    records = list(session.run(query, name=TARGET_NAME))
    if not records:
        print(f"[실패] (t:Thesaurus {{name: '{TARGET_NAME}'}}) 노드를 찾지 못했습니다.")
        return None, None
    if len(records) > 1:
        print(f"[주의] 동일 이름 노드가 {len(records)}개입니다.")
    row = records[0].data()
    vector = row.get("embedding")
    dimension = row.get("dim")
    print(f"name={display(row.get('name'))}")
    print(f"labels={display(row.get('labels'))}")
    print(f"dim={display(dimension)} (기대값: {VECTOR_DIMENSION})")
    print(f"description={display(row.get('description'))}")
    if not isinstance(vector, list) or not vector:
        print("[실패] embedding 속성이 없거나 리스트가 아닙니다.")
        return None, row
    if dimension != VECTOR_DIMENSION:
        print("[주의] 저장 벡터 차원이 768이 아닙니다. 모델/인덱스 설정을 확인하세요.")
    else:
        print("[확인] 저장 벡터 차원은 768입니다.")
    return [float(item) for item in vector], row


def direct_ground_truth(
    node_vector: list[float] | None,
    query: str,
    ollama_url: str,
    model: str,
) -> list[float] | None:
    section("3. 직접 코사인 유사도 Ground Truth 테스트")
    print(f"query={query}")
    print(f"Ollama={ollama_url}, model={model}")
    try:
        query_vector = ollama_embedding(ollama_url, model, query)
        print(f"query_dim={len(query_vector)}")
    except Exception as exc:
        print(f"[실패] 질문 임베딩 생성 실패: {exc}")
        return None
    if node_vector is None:
        print("[중단] 저장 벡터가 없어 코사인 유사도를 계산하지 못했습니다.")
        return query_vector
    try:
        score = cosine_similarity(query_vector, node_vector)
    except ValueError as exc:
        print(f"[실패] 코사인 유사도 계산 실패: {exc}")
        return query_vector
    print(f"cosine(query, {TARGET_NAME})={score:.6f}")
    if len(query_vector) != VECTOR_DIMENSION:
        print("[주의] 질문 벡터 차원이 768이 아닙니다.")
    if score >= 0.75:
        print("[판정] 높은 직접 유사도입니다. 벡터 생성 자체보다 인덱스/검색 경로를 의심하세요.")
    elif score >= 0.50:
        print("[판정] 중간 유사도입니다. 모델, 텍스트 직렬화, 질문 표현 차이를 확인하세요.")
    else:
        print("[판정] 낮은 유사도입니다. 저장 벡터와 질문 벡터의 모델/텍스트 공간 불일치 가능성이 큽니다.")
    return query_vector


def fulltext_results(session: Any, query: str) -> list[dict[str, Any]]:
    """Run the same namesIndex query used by docent.php's graph action."""
    section("4-A. 실제 PHP namesIndex 검색 경로 Top-5")
    metadata_query = """
    SHOW INDEXES
    YIELD name, type, state, labelsOrTypes, properties
    WHERE name = 'namesIndex'
    RETURN name, type, state, labelsOrTypes, properties
    """
    try:
        metadata = session.run(metadata_query).single()
    except Exception as exc:
        metadata = None
        print(f"namesIndex 메타데이터 조회 실패: {exc}")
    if metadata:
        metadata_row = metadata.data()
        labels = metadata_row.get("labelsOrTypes") or []
        print(
            f"index=name={display(metadata_row.get('name'))}, "
            f"type={display(metadata_row.get('type'))}, "
            f"state={display(metadata_row.get('state'))}, "
            f"labelsOrTypes={display(labels)}, "
            f"properties={display(metadata_row.get('properties'))}"
        )
        if "Thesaurus" not in labels:
            print("[원인 후보] namesIndex 대상 라벨에 Thesaurus가 없습니다.")
    else:
        print("namesIndex 메타데이터를 찾지 못했습니다.")
    cypher = """
    CALL db.index.fulltext.queryNodes('namesIndex', $term)
    YIELD node, score
    RETURN node AS n, labels(node) AS labels, score
    ORDER BY score DESC
    LIMIT 5
    """
    try:
        records = list(session.run(cypher, term=query))
    except Exception as exc:
        print(f"namesIndex 검색 실패: {exc}")
        print("PHP도 같은 namesIndex 호출 후 fallback 검색으로 전환할 가능성이 있습니다.")
        return []
    if not records:
        print("검색 결과가 없습니다.")
        return []
    results = []
    for rank, record in enumerate(records, start=1):
        node = record.get("n")
        labels = list(record.get("labels") or [])
        properties = dict(node.items()) if node is not None else {}
        result = {
            "rank": rank,
            "labels": labels,
            "name": properties.get("name") or properties.get("명칭") or properties.get("title"),
            "score": record.get("score"),
        }
        results.append(result)
        print(
            f"{rank}. labels={labels}, name={display(result['name'])}, "
            f"score={display(result['score'])}"
        )
    if not any("Thesaurus" in result["labels"] for result in results):
        print("[주의] PHP namesIndex Top-5에 Thesaurus가 없습니다.")
    return results


def vector_results(
    session: Any,
    indexes: list[dict[str, Any]],
    query_vector: list[float] | None,
) -> None:
    section("4-B. Thesaurus 대상 Neo4j 벡터 검색 Top-5")
    if query_vector is None:
        print("[중단] 질문 벡터가 없어 벡터 검색을 실행하지 못했습니다.")
        return
    candidates = [
        row
        for row in indexes
        if row.get("state") == "ONLINE"
        and "Thesaurus" in (row.get("labelsOrTypes") or [])
        and "embedding" in (row.get("properties") or [])
    ]
    if not candidates:
        print("Thesaurus.embedding을 대상으로 하는 ONLINE 벡터 인덱스가 없습니다.")
        return
    for index in candidates:
        index_name = index.get("name")
        cypher = """
        CALL db.index.vector.queryNodes($index_name, $k, $embedding)
        YIELD node, score
        RETURN node AS n, labels(node) AS labels, score
        ORDER BY score DESC
        LIMIT $k
        """
        print(f"index={index_name}")
        try:
            records = list(
                session.run(
                    cypher,
                    index_name=index_name,
                    k=TOP_K,
                    embedding=query_vector,
                )
            )
        except Exception as exc:
            print(f"벡터 인덱스 검색 실패: {exc}")
            continue
        for rank, record in enumerate(records, start=1):
            node = record.get("n")
            properties = dict(node.items()) if node is not None else {}
            print(
                f"{rank}. labels={list(record.get('labels') or [])}, "
                f"name={display(properties.get('name'))}, "
                f"score={display(record.get('score'))}"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Neo4j GraphRAG 검색 경로 진단")
    parser.add_argument("--query", default=DEFAULT_QUERY, help="진단에 사용할 질문")
    parser.add_argument("--target-name", default=TARGET_NAME, help=argparse.SUPPRESS)
    parser.add_argument("--ollama-url", default=setting("OLLAMA_HOST", DEFAULT_OLLAMA_URL))
    parser.add_argument("--embedding-model", default=setting("OLLAMA_EMBED_MODEL", DEFAULT_OLLAMA_MODEL))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    global TARGET_NAME
    TARGET_NAME = args.target_name
    password = setting("NEO4J_PASSWORD")
    if not password:
        raise RuntimeError("NEO4J_PASSWORD를 환경변수 또는 .streamlit/secrets.toml에 설정하세요.")

    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(
        setting("NEO4J_URI", "bolt://localhost:7687"),
        auth=(setting("NEO4J_USER", "neo4j"), password),
    )
    try:
        driver.verify_connectivity()
        with driver.session() as session:
            indexes = show_vector_indexes(session)
            node_vector, _ = inspect_target_node(session)
            query_vector = direct_ground_truth(
                node_vector, args.query, args.ollama_url, args.embedding_model
            )
            fulltext_results(session, args.query)
            vector_results(session, indexes, query_vector)
    finally:
        driver.close()
    print("\n진단 완료")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("사용자에 의해 중단되었습니다.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"진단 실패: {exc}", file=sys.stderr)
        raise SystemExit(1)
