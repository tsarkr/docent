"""Structured Judge evaluation for context precision.

This is deliberately separate from RAGAS' parser-dependent metric. It uses
the same records and a stronger local Judge, but sends a small JSON-only
prompt directly to Ollama so thinking traces cannot corrupt the score.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import re
from pathlib import Path
from typing import Any

import requests


PROMPT = """You are a strict retrieval evaluator.
Judge whether each context is relevant evidence for answering the question and
the reference. Return JSON only, with exactly this shape:
{{"relevant":[0,1,0],"precision":0.3333}}
Use 1 only when the context contains direct evidence for at least one
reference claim. Use 0 for merely related names, metadata, or unsupported
inference. precision is the mean of relevant values, rounded to 4 decimals.
Do not use outside knowledge.

QUESTION:
{question}

REFERENCE:
{reference}

There are exactly {context_count} contexts. Return exactly {context_count} values in relevant.

CONTEXTS (in rank order):
{contexts}
"""


def judge(record: dict[str, Any], model: str, host: str) -> dict[str, Any]:
    contexts = record.get("retrieved_contexts", [])
    if not contexts:
        return {
            "query_id": record.get("query_id"),
            "model": record.get("model"),
            "relevant": [],
            "context_precision_judge": 0.0,
        }
    prompt = PROMPT.format(
        question=record["user_input"],
        reference=record["reference"],
        context_count=len(contexts),
        contexts="\n".join(f"[{i}] {value}" for i, value in enumerate(contexts)),
    )
    parsed: dict[str, Any] | None = None
    for attempt in range(3):
        response = requests.post(
            f"{host.rstrip('/')}/api/chat",
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "format": "json",
                "stream": False,
                "options": {"temperature": 0, "num_predict": 128},
                "think": False,
            },
            timeout=300,
        )
        response.raise_for_status()
        content = response.json()["message"]["content"]
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if match:
            candidate = json.loads(match.group(0))
            if "relevant" in candidate:
                parsed = candidate
                break
    if parsed is None:
        raise RuntimeError(f"Judge returned no relevance vector for {record.get('query_id')}")
    relevant = [int(value) for value in parsed["relevant"]]
    if len(relevant) > len(contexts):
        relevant = relevant[:len(contexts)]
    if len(relevant) != len(contexts) or any(value not in (0, 1) for value in relevant):
        raise RuntimeError(
            f"Invalid relevance vector for {record.get('query_id')}: "
            f"expected {len(contexts)}, got {parsed!r}"
        )
    precision = round(sum(relevant) / len(relevant), 4) if relevant else 0.0
    return {
        "query_id": record.get("query_id"),
        "model": record.get("model"),
        "relevant": relevant,
        "context_precision_judge": precision,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model", default="gemma4:26b")
    parser.add_argument("--host", default="http://localhost:11434")
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()
    records = json.loads(args.input.read_text(encoding="utf-8"))
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(lambda row: judge(row, args.model, args.host), records))
    if len(results) != len(records):
        raise RuntimeError("Judge result count does not match input count")
    args.output.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Judge context precision 저장 완료: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
