#!/usr/bin/env python3
"""Stronger retrieval baselines for the RAG comparison.

1. Hybrid Vector RAG: BM25 + dense retrieval fused with Reciprocal Rank
   Fusion, then reranked with a cross-encoder.
2. Microsoft GraphRAG: thin adapter around the reference ``graphrag`` CLI
   (corpus export + query). Indexing is run by the user because it is a long,
   paid LLM job.

Both baselines see only the question text. They never read the benchmark's
``target_entities``, which are gold annotations.
"""

from __future__ import annotations

import argparse
import hashlib
import math
import re
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.config import get_pg_connection, setting

CACHE_DIR = ROOT / "vector_store" / "baselines"
ALLOWED_TABLES = {"raw_bibliography", "raw_source_info", "raw_event_info"}
CHUNK_CHARS = 500
CHUNK_OVERLAP = 100
RRF_K = 60


def corpus_tables() -> list[str]:
    """Tables searched by the baselines (default: the same pool Docent cites)."""
    names = [t.strip() for t in setting("BASELINE_CORPUS_TABLES", "raw_bibliography").split(",") if t.strip()]
    unknown = set(names) - ALLOWED_TABLES
    if unknown:
        raise ValueError(f"지원하지 않는 코퍼스 테이블: {sorted(unknown)}")
    return names


def _strip_tei(tei: str) -> str:
    # Keep only the record body; the teiHeader is generated boilerplate.
    body = re.search(r"<body>(.*?)</body>", tei, flags=re.DOTALL)
    tei = body.group(1) if body else tei
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", tei)).strip()


def load_documents(tables: list[str] | None = None) -> list[dict[str, Any]]:
    """Load TEI records as plain-text documents: {id, table, rowid, text}."""
    conn = get_pg_connection()
    cur = conn.cursor()
    docs = []
    for table in tables or corpus_tables():
        cur.execute(f'SELECT rowid, tei FROM "{table}" WHERE tei IS NOT NULL ORDER BY rowid')
        for rowid, tei in cur.fetchall():
            text = _strip_tei(tei)
            if text:
                docs.append({"id": f"{table}:{rowid}", "table": table, "rowid": rowid, "text": text})
    cur.close()
    conn.close()
    return docs


def chunk_documents(docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    chunks = []
    step = CHUNK_CHARS - CHUNK_OVERLAP
    for doc in docs:
        text = doc["text"]
        for n, start in enumerate(range(0, max(len(text) - CHUNK_OVERLAP, 1), step)):
            chunks.append({"id": f"{doc['id']}#{n}", "text": text[start:start + CHUNK_CHARS]})
    return chunks


# ─────────────────────────────────────────────────────────────────────────────
# BM25
# ─────────────────────────────────────────────────────────────────────────────
def tokenize(text: str) -> list[str]:
    """Words plus character bigrams, so Korean matches without a morph analyzer."""
    tokens = []
    for word in re.findall(r"[가-힣一-鿿A-Za-z0-9]+", text.lower()):
        tokens.append(word)
        if len(word) > 2:
            tokens.extend(word[i:i + 2] for i in range(len(word) - 1))
    return tokens


class BM25Index:
    def __init__(self, texts: list[str], k1: float = 1.5, b: float = 0.75):
        self.k1, self.b = k1, b
        self.n_docs = len(texts)
        self.doc_len = np.zeros(self.n_docs, dtype=np.float32)
        postings: dict[str, tuple[list[int], list[int]]] = defaultdict(lambda: ([], []))
        for doc_id, text in enumerate(texts):
            counts = Counter(tokenize(text))
            self.doc_len[doc_id] = sum(counts.values())
            for token, tf in counts.items():
                ids, tfs = postings[token]
                ids.append(doc_id)
                tfs.append(tf)
        self.avg_len = float(self.doc_len.mean()) if self.n_docs else 0.0
        self.postings = {
            token: (np.asarray(ids, dtype=np.int32), np.asarray(tfs, dtype=np.float32))
            for token, (ids, tfs) in postings.items()
        }

    def scores(self, query: str) -> np.ndarray:
        out = np.zeros(self.n_docs, dtype=np.float32)
        for token in set(tokenize(query)):
            if token not in self.postings:
                continue
            ids, tfs = self.postings[token]
            idf = math.log(1 + (self.n_docs - len(ids) + 0.5) / (len(ids) + 0.5))
            norm = self.k1 * (1 - self.b + self.b * self.doc_len[ids] / self.avg_len)
            out[ids] += idf * tfs * (self.k1 + 1) / (tfs + norm)
        return out


# ─────────────────────────────────────────────────────────────────────────────
# Dense retrieval (Ollama embeddings, cached on disk)
# ─────────────────────────────────────────────────────────────────────────────
def _embed(texts: list[str], kind: str) -> np.ndarray:
    from scripts.vectorize_neo4j import ollama_embeddings

    model = setting("BASELINE_EMBED_MODEL", setting("OLLAMA_EMBED_MODEL", "nomic-embed-text"))
    if "nomic" in model:  # nomic models are trained with task prefixes
        texts = [f"search_{kind}: {t}" for t in texts]
    return ollama_embeddings(texts, model, setting("OLLAMA_HOST", "http://localhost:11434"), 32)


def _dense_matrix(chunks: list[dict[str, Any]]) -> np.ndarray:
    model = setting("BASELINE_EMBED_MODEL", setting("OLLAMA_EMBED_MODEL", "nomic-embed-text"))
    digest = hashlib.sha1()
    for chunk in chunks:
        digest.update(chunk["id"].encode())
        digest.update(chunk["text"].encode())
    cache = CACHE_DIR / f"dense_{re.sub(r'[^A-Za-z0-9]+', '_', model)}_{digest.hexdigest()[:12]}.npy"
    if cache.exists():
        return np.load(cache)
    print(f"  🧮 dense 임베딩 생성 중 ({len(chunks)} chunks, model={model}) — 최초 1회만 수행됩니다...", flush=True)
    matrix = _embed([c["text"] for c in chunks], "document")
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    np.save(cache, matrix)
    return matrix


# ─────────────────────────────────────────────────────────────────────────────
# Hybrid retrieval
# ─────────────────────────────────────────────────────────────────────────────
_STATE: dict[str, Any] = {}


def _rerank(query: str, candidates: list[dict[str, Any]], reranker: str) -> list[dict[str, Any]]:
    if reranker == "none":
        return candidates
    if reranker != "cross-encoder":
        raise ValueError(f"알 수 없는 reranker: {reranker}")
    if "cross_encoder" not in _STATE:
        try:
            from sentence_transformers import CrossEncoder
        except ImportError as exc:
            raise RuntimeError(
                "재순위화에는 'sentence-transformers'가 필요합니다. requirements.txt를 설치하거나 "
                "--reranker none 으로 실행하세요."
            ) from exc
        _STATE["cross_encoder"] = CrossEncoder(setting("BASELINE_RERANK_MODEL", "BAAI/bge-reranker-v2-m3"))
    scores = _STATE["cross_encoder"].predict([(query, c["text"]) for c in candidates])
    order = np.argsort(-np.asarray(scores))
    return [candidates[i] for i in order]


def hybrid_retrieve(query: str, top_k: int = 4, candidates: int = 50, reranker: str = "cross-encoder") -> list[dict[str, Any]]:
    """BM25 + dense -> RRF -> rerank. Returns chunks as {id, text}."""
    if "chunks" not in _STATE:
        print("  📚 하이브리드 베이스라인 인덱스 구축 중...", flush=True)
        chunks = chunk_documents(load_documents())
        _STATE["chunks"] = chunks
        _STATE["bm25"] = BM25Index([c["text"] for c in chunks])
        _STATE["dense"] = _dense_matrix(chunks)
    chunks = _STATE["chunks"]

    bm25_rank = np.argsort(-_STATE["bm25"].scores(query))[:candidates]
    dense_rank = np.argsort(-(_STATE["dense"] @ _embed([query], "query")[0]))[:candidates]

    fused: dict[int, float] = defaultdict(float)
    for ranking in (bm25_rank, dense_rank):
        for rank, idx in enumerate(ranking, 1):
            fused[int(idx)] += 1.0 / (RRF_K + rank)
    pool = [chunks[i] for i, _ in sorted(fused.items(), key=lambda kv: -kv[1])[:candidates]]
    return _rerank(query, pool, reranker)[:top_k]


# ─────────────────────────────────────────────────────────────────────────────
# Microsoft GraphRAG (reference implementation, via its CLI)
# ─────────────────────────────────────────────────────────────────────────────
def export_graphrag_corpus(root: Path, max_docs: int = 0) -> int:
    """Write the baseline corpus as text files under <root>/input for `graphrag index`."""
    input_dir = root / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    docs = load_documents()
    if max_docs > 0:
        docs = docs[:max_docs]
    for doc in docs:
        (input_dir / f"{doc['id'].replace(':', '_')}.txt").write_text(doc["text"], encoding="utf-8")
    return len(docs)


def msgraphrag_query(query: str) -> str:
    """Answer a question with an already-built Microsoft GraphRAG index."""
    root = setting("GRAPHRAG_ROOT", "")
    if not root or not (Path(root) / "output").exists():
        raise RuntimeError(
            "Microsoft GraphRAG 인덱스가 없습니다. GRAPHRAG_ROOT를 설정하고 "
            "`graphrag index --root $GRAPHRAG_ROOT`를 먼저 실행하세요."
        )
    cmd = [
        setting("GRAPHRAG_BIN", "graphrag"), "query",
        "--root", root,
        "--method", setting("GRAPHRAG_METHOD", "local"),
        "--query", query,
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
    if res.returncode != 0:
        raise RuntimeError(f"graphrag query 실패 (exit {res.returncode}): {res.stderr.strip()[-300:]}")
    return res.stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description="Baseline utilities (hybrid retrieval check, GraphRAG corpus export).")
    sub = parser.add_subparsers(dest="command", required=True)

    p_search = sub.add_parser("search", help="Run one hybrid retrieval query and print the chunks")
    p_search.add_argument("query")
    p_search.add_argument("--top-k", type=int, default=4)
    p_search.add_argument("--reranker", choices=["cross-encoder", "none"], default="cross-encoder")

    p_export = sub.add_parser("export-graphrag", help="Export the corpus to <root>/input for Microsoft GraphRAG")
    p_export.add_argument("--root", type=Path, required=True)
    p_export.add_argument("--max-docs", type=int, default=0, help="Export only the first N records (0 = all)")
    p_export.add_argument("--dry-run", action="store_true", help="Only report how many files would be written")

    args = parser.parse_args()
    if args.command == "search":
        for chunk in hybrid_retrieve(args.query, top_k=args.top_k, reranker=args.reranker):
            print(f"[{chunk['id']}] {chunk['text'][:200]}")
    else:
        if args.dry_run:
            n = len(load_documents())
            n = min(n, args.max_docs) if args.max_docs > 0 else n
            print(f"[dry-run] {n}개 문서를 {args.root / 'input'} 에 내보낼 예정입니다.")
        else:
            print(f"✅ {export_graphrag_corpus(args.root, args.max_docs)}개 문서를 {args.root / 'input'} 에 저장했습니다.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
