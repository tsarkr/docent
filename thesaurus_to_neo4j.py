#!/usr/bin/env python3
"""Load the Korean History Thesaurus into Neo4j and optionally add Ollama vectors."""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_CSV = ROOT / "data" / "교육부 국사편찬위원회_한국역사용어시소러스 정보_20211028.csv"
TARGET_PERIODS = {"일제시기", "근대", "근대-현대"}
REQUIRED_COLUMNS = {
    "term_id",
    "term_name",
    "term_ch",
    "term_times",
    "term_lk",
    "term_desc",
}
DEFAULT_EMBEDDING_MODEL = "nomic-embed-text"
DEFAULT_OLLAMA_URL = "http://localhost:11434"
OLLAMA_BATCH_SIZE = 100


def load_secrets() -> dict[str, Any]:
    """Read the repository's Streamlit secrets file when it is available."""
    secret_path = ROOT / ".streamlit" / "secrets.toml"
    if not secret_path.exists():
        return {}
    try:
        try:
            import tomllib
        except ModuleNotFoundError:
            import tomli as tomllib
        with secret_path.open("rb") as secret_file:
            values = tomllib.load(secret_file)
        return values if isinstance(values, dict) else {}
    except Exception as exc:
        print(f"경고: secrets.toml을 읽지 못했습니다: {exc}", file=sys.stderr)
        return {}


SECRETS = load_secrets()


def setting(name: str, default: str = "") -> str:
    """Return an environment value first, then a flat TOML value, then default."""
    environment_value = os.getenv(name)
    if environment_value:
        return environment_value
    secret_value = SECRETS.get(name)
    if secret_value is not None and str(secret_value):
        return str(secret_value)
    return default


def read_thesaurus_csv(csv_path: Path) -> list[dict[str, str]]:
    """Read and normalize only the requested historical periods."""
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {csv_path}")

    rows_by_id: dict[str, dict[str, str]] = {}
    filtered_count = 0
    duplicate_count = 0
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            columns = set(reader.fieldnames or [])
            missing_columns = REQUIRED_COLUMNS - columns
            if missing_columns:
                missing = ", ".join(sorted(missing_columns))
                raise ValueError(f"CSV 필수 컬럼이 없습니다: {missing}")

            for line_number, source_row in enumerate(reader, start=2):
                row = {
                    column: str(source_row.get(column) or "").strip()
                    for column in REQUIRED_COLUMNS
                }
                if row["term_times"] not in TARGET_PERIODS:
                    continue
                filtered_count += 1
                if not row["term_id"]:
                    print(
                        f"경고: {line_number}행은 term_id가 없어 건너뜁니다.",
                        file=sys.stderr,
                    )
                    continue
                if row["term_id"] in rows_by_id:
                    duplicate_count += 1
                rows_by_id[row["term_id"]] = {
                    "term_id": row["term_id"],
                    "name": row["term_name"],
                    "hanja": row["term_ch"],
                    "category": row["term_lk"],
                    "description": row["term_desc"],
                    "period": row["term_times"],
                }
    except UnicodeDecodeError as exc:
        raise ValueError(f"UTF-8 CSV로 읽을 수 없습니다: {csv_path}") from exc
    except csv.Error as exc:
        raise ValueError(f"CSV 형식 오류 ({csv_path}): {exc}") from exc

    if duplicate_count:
        print(f"경고: 중복 term_id {duplicate_count}건은 마지막 행을 사용합니다.")
    print(f"시대 필터 결과: {filtered_count}건, 적재 대상: {len(rows_by_id)}건")
    return list(rows_by_id.values())


SCHEMA_STATEMENTS = (
    "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Thesaurus) REQUIRE t.term_id IS UNIQUE",
    "CREATE INDEX IF NOT EXISTS FOR (t:Thesaurus) ON (t.name)",
    "CREATE VECTOR INDEX samil_docent_vector_idx IF NOT EXISTS FOR (t:Thesaurus) ON (t.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 768, `vector.similarity_function`: 'cosine'}}",
)

UPSERT_QUERY = """
UNWIND $rows AS row
MERGE (t:Thesaurus {term_id: row.term_id})
SET t.name = row.name,
    t.hanja = row.hanja,
    t.category = row.category,
    t.description = row.description,
    t.period = row.period,
    t.type = '시소러스'
WITH t
CALL {
    WITH t
    MATCH (target)
        WHERE (target:Place OR target:Person OR target:장소 OR target:인물)
            AND (coalesce(target.name, '') <> '' OR coalesce(target.명칭, '') <> '')
            AND (
                    target.name = t.name OR target.name = t.hanja OR
                    target.명칭 = t.name OR target.명칭 = t.hanja
            )
    SET target.description = t.description,
        target.hanja = t.hanja
    MERGE (target)-[:DEFINED_AS]->(t)
    RETURN count(target) AS matched_targets
}
RETURN count(t) AS upserted
"""


def ensure_schema(session: Any) -> None:
    for statement in SCHEMA_STATEMENTS:
        session.run(statement).consume()


def load_batches(driver: Any, rows: list[dict[str, str]], batch_size: int) -> int:
    written = 0
    with driver.session() as session:
        ensure_schema(session)
        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            result = session.run(UPSERT_QUERY, rows=batch).single()
            upserted = int(result["upserted"]) if result else 0
            if upserted != len(batch):
                raise RuntimeError(
                    f"Neo4j 적재 개수 불일치: 입력 {len(batch)}건, 처리 {upserted}건"
                )
            written += upserted
            print(f"Neo4j 적재: {written}/{len(rows)}건", flush=True)
    return written


def embedding_text(row: dict[str, Any]) -> str:
    neighbors = row.get("neighbors") or ""
    return (
        f"[유형: 시소러스] 이름: {row.get('name', '')}({row.get('hanja', '')})\n"
        f"분류: {row.get('category', '')}\n설명: {row.get('description', '')}\n"
        f"관련 그래프 문맥: {neighbors}"
    )


def read_embedding_batches(driver: Any, batch_size: int) -> list[list[dict[str, Any]]]:
    query = """
    MATCH (t:Thesaurus)
    WHERE t.embedding IS NULL AND EXISTS((t)--())
    OPTIONAL MATCH (t)-[r]-(neighbor)
    WITH t, collect(
        CASE WHEN neighbor IS NULL THEN NULL ELSE
            type(r) + " -> " + coalesce(neighbor.name, neighbor.명칭, neighbor.title, neighbor.제목, neighbor.사건명, neighbor.term_id, "")
        END
    ) AS neighbors
    RETURN t.term_id AS term_id, t.name AS name, t.hanja AS hanja,
           t.category AS category, t.description AS description,
           reduce(text = "", value IN neighbors |
               text + CASE WHEN value IS NULL THEN "" ELSE value + " | " END
           ) AS neighbors
    ORDER BY t.term_id
    """
    with driver.session() as session:
        rows = [record.data() for record in session.run(query)]
    return [rows[start : start + batch_size] for start in range(0, len(rows), batch_size)]


def request_ollama_embeddings(
    ollama_url: str, model: str, texts: list[str]
) -> list[list[float]]:
    """Call Ollama's batch embedding endpoint for one batch."""
    import requests

    base_url = ollama_url.rstrip("/")
    endpoint = f"{base_url}/api/embed"
    fallback_endpoint = f"{base_url}/api/embeddings"
    try:
        response = requests.post(
            endpoint,
            json={"model": model, "input": texts},
            timeout=300,
        )
        if response.status_code != 404:
            response.raise_for_status()
            body = response.json()
            vectors = body.get("embeddings")
            if not isinstance(vectors, list) or len(vectors) != len(texts):
                raise RuntimeError(
                    f"Ollama 응답 개수 불일치: 요청 {len(texts)}건, "
                    f"응답 {len(vectors or [])}건"
                )
            if any(not isinstance(vector, list) or not vector for vector in vectors):
                raise RuntimeError("Ollama 응답에 유효한 embedding 벡터가 없습니다.")
            return vectors
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Ollama 연결 실패: {ollama_url}. 서버가 실행 중인지 확인하세요."
        ) from exc

    vectors = []
    for text in texts:
        try:
            response = requests.post(
                fallback_endpoint,
                json={"model": model, "prompt": text},
                timeout=300,
            )
            response.raise_for_status()
            vector = response.json().get("embedding")
            if not isinstance(vector, list) or not vector:
                raise RuntimeError("Ollama 응답에 유효한 embedding 벡터가 없습니다.")
            vectors.append(vector)
        except (requests.RequestException, ValueError) as exc:
            raise RuntimeError(f"Ollama 임베딩 요청 실패: {exc}") from exc
    return vectors


STORE_EMBEDDINGS_QUERY = """
UNWIND $rows AS row
MATCH (t:Thesaurus {term_id: row.term_id})
SET t.embedding = row.embedding
RETURN count(t) AS updated
"""


def embed_missing_nodes(driver: Any, ollama_url: str, model: str) -> int:
    batches = read_embedding_batches(driver, OLLAMA_BATCH_SIZE)
    if not batches:
        print("임베딩 대상 Thesaurus 노드가 없습니다.", flush=True)
        return 0

    total = sum(len(batch) for batch in batches)
    print(
        f"임베딩 시작: 대상 {total}건, {len(batches)}개 배치 "
        f"(모델: {model}, 배치당 최대 {OLLAMA_BATCH_SIZE}건)",
        flush=True,
    )
    updated_total = 0
    for batch_index, batch in enumerate(batches, start=1):
        print(
            f"Ollama 요청 중: 배치 {batch_index}/{len(batches)} "
            f"({len(batch)}건)",
            flush=True,
        )
        vectors = request_ollama_embeddings(
            ollama_url, model, [embedding_text(row) for row in batch]
        )
        updates = [
            {"term_id": row["term_id"], "embedding": vector}
            for row, vector in zip(batch, vectors)
        ]
        with driver.session() as session:
            result = session.run(STORE_EMBEDDINGS_QUERY, rows=updates).single()
            updated = int(result["updated"]) if result else 0
        if updated != len(batch):
            raise RuntimeError(
                f"임베딩 저장 개수 불일치: 요청 {len(batch)}건, 저장 {updated}건"
            )
        updated_total += updated
        print(f"Ollama 임베딩 저장: {updated_total}/{total}건", flush=True)
        if batch_index < len(batches):
            time.sleep(1.0)
    return updated_total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="한국역사용어시소러스의 근대·일제강점기 데이터를 Neo4j에 적재합니다."
    )
    parser.add_argument("--csv", default=str(DEFAULT_CSV), help="시소러스 CSV 파일 경로")
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Neo4j 트랜잭션 배치 크기"
    )
    parser.add_argument(
        "--skip-embed", action="store_true", help="Ollama 임베딩 생성을 건너뜁니다."
    )
    parser.add_argument(
        "--ollama-url",
        default=setting("OLLAMA_HOST", DEFAULT_OLLAMA_URL),
        help="Ollama 서버 주소",
    )
    parser.add_argument(
        "--embedding-model",
        default=setting("OLLAMA_EMBED_MODEL", DEFAULT_EMBEDDING_MODEL),
        help="Ollama 임베딩 모델명",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.batch_size < 1:
        raise ValueError("--batch-size는 1 이상이어야 합니다.")

    rows = read_thesaurus_csv(Path(args.csv).expanduser())
    if not rows:
        print("필터링된 데이터가 없어 종료합니다.")
        return 0

    password = setting("NEO4J_PASSWORD")
    if not password:
        raise RuntimeError("NEO4J_PASSWORD를 환경변수 또는 secrets.toml에 설정하세요.")

    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(
        setting("NEO4J_URI", "bolt://localhost:7687"),
        auth=(setting("NEO4J_USER", "neo4j"), password),
    )
    try:
        driver.verify_connectivity()
        load_batches(driver, rows, args.batch_size)
        print("Neo4j 적재 완료.", flush=True)
        if args.skip_embed:
            print("--skip-embed 지정으로 임베딩을 건너뜁니다.", flush=True)
        else:
            print(
                "--skip-embed가 없어 Ollama 임베딩을 계속합니다. "
                "임베딩 모델이 로컬에 없으면 먼저 ollama pull을 실행하세요.",
                flush=True,
            )
            embed_missing_nodes(driver, args.ollama_url, args.embedding_model)
    finally:
        print("Neo4j 연결 종료 중...", flush=True)
        driver.close()
        print("작업이 정상적으로 종료되었습니다.", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("사용자에 의해 중단되었습니다.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"오류: {exc}", file=sys.stderr)
        raise SystemExit(1)