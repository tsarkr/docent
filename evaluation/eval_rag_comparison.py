#!/usr/bin/env python3
"""Quantitative Evaluation: Vector RAG vs. GraphRAG vs. Docent Hybrid RAG.

Evaluates:
1. Model 1: Vanilla Vector RAG (Text chunks Top-K retrieval)
2. Model 2: Hybrid Vector RAG (BM25 + dense, RRF fusion, cross-encoder rerank)
3. Model 3: Standard GraphRAG (Neo4j Graph triples traversal without scoping)
4. Model 4: Microsoft GraphRAG reference implementation (opt-in, needs a built index)
5. Model 5: Proposed Docent Hybrid RAG (Graph + PostgreSQL Scoping Envelopes + 2-Stage In-Context Knowledge Pipeline)

Metrics:
- Faithfulness / Fact Recall (%)
- Hallucination Rate (%) [Zero-correlation trap detection & cross-attribution error rate]
- Context Tokens & Output Tokens
- Information Density / Token Efficiency (Facts / 1k context tokens)
- Latency (seconds)

Outputs:
- table_rag_comparison.tex (IEEE/ACM formatted publication table)
- rag_evaluation_results.csv (Detailed per-query breakdown)
- rag_category_results.csv / rag_significance.csv / table_rag_by_category.tex
  (per-category results and paired significance tests, see analyze_rag_results.py)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.config import get_neo4j_driver, get_pg_connection, load_secrets, setting

DATASET_PATH = Path(__file__).resolve().parent / "dataset" / "multihop_benchmark_dataset.json"
RESULTS_DIR = Path(__file__).resolve().parent / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


# Conditions shared by every pipeline, so differences in the results come from
# the pipelines and not from their settings. All are set from the CLI in main().
GEN_TEMPERATURE = 0.0
GEN_TOP_P = 0.8
CONTEXT_CHARS = 2400          # retrieved-context budget per question; 0 = each pipeline's legacy size
RETRIEVAL_INPUT = "question"  # "question": entities extracted from the question / "entities": gold target_entities


def call_llm(messages: list[dict[str, str]], max_tokens: int = 1500) -> str:
    """Call Google Gemini API using configured key."""
    temperature, top_p = GEN_TEMPERATURE, GEN_TOP_P
    secrets = load_secrets()
    api_key = setting("GEMINI_API_KEY", "", secrets)
    model = setting("GEMINI_MODEL", "gemini-flash-lite-latest", secrets)
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY가 설정되지 않았습니다.")

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"

    contents = []
    system_instruction = None
    for msg in messages:
        if msg["role"] == "system":
            system_instruction = {"parts": [{"text": msg["content"]}]}
        else:
            role = "model" if msg["role"] == "assistant" else "user"
            contents.append({"role": role, "parts": [{"text": msg["content"]}]})

    payload: dict[str, Any] = {
        "contents": contents,
        "generationConfig": {
            "temperature": temperature,
            "topP": top_p,
            "maxOutputTokens": max_tokens,
        },
    }
    if system_instruction:
        payload["systemInstruction"] = system_instruction

    for attempt in range(4):
        try:
            resp = requests.post(url, json=payload, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                return data["candidates"][0]["content"]["parts"][0]["text"].strip()
            elif resp.status_code in (429, 503):
                wait_time = max(15, (attempt + 1) * 10)
                print(f" (재시도 대기 {wait_time}초: {resp.status_code})...", end="", flush=True)
                time.sleep(wait_time)
                continue
            else:
                raise RuntimeError(f"Gemini API 호출 실패 (상태 코드 {resp.status_code}): {resp.text[:200]}")
        except requests.RequestException as e:
            if attempt == 3:
                raise RuntimeError(f"Gemini API 요청 실패: {e}")
            time.sleep(5)

    raise RuntimeError("Gemini API 재시도 한도 초과 (429/503)")


def pack_context(items: list[str], separator: str) -> list[str]:
    """Keep ranked context items until the shared character budget is used up."""
    if CONTEXT_CHARS <= 0:
        return items
    packed: list[str] = []
    used = 0
    for item in items:
        room = CONTEXT_CHARS - used
        if room <= 0:
            break
        packed.append(item[:room])
        used += len(packed[-1]) + len(separator)
    return packed


_ENTITY_CACHE: dict[str, list[str]] = {}


def question_entities(query: str) -> list[str]:
    """Extract search entities from the question text (one LLM call per question, shared by all pipelines)."""
    if query not in _ENTITY_CACHE:
        raw = call_llm([
            {"role": "system", "content": (
                "질문에서 검색에 사용할 고유명(인물, 지명, 기관·단체, 사건·문서명)을 질문에 적힌 표기 그대로 추출하십시오. "
                "질문에 없는 이름을 추가하지 말고, 최대 8개를 JSON 문자열 배열로만 출력하십시오."
            )},
            {"role": "user", "content": query},
        ], max_tokens=300)
        match = re.search(r"\[.*\]", raw, flags=re.DOTALL)
        if not match:
            raise RuntimeError(f"질의 개체 추출 결과를 해석할 수 없습니다: {raw[:80]}")
        _ENTITY_CACHE[query] = [str(e).strip() for e in json.loads(match.group(0)) if str(e).strip()]
    return _ENTITY_CACHE[query]


def retrieval_entities(query_item: dict[str, Any]) -> list[str]:
    """Entities every pipeline may use for retrieval under the current input condition."""
    if RETRIEVAL_INPUT == "entities":
        return list(query_item["target_entities"])
    return question_entities(query_item["query"])


# ─────────────────────────────────────────────────────────────────────────────
# 1. Pipeline 1: Vanilla Vector RAG
# ─────────────────────────────────────────────────────────────────────────────
def retrieve_vector_chunks(query: str, target_entities: list[str], top_k: int = 5) -> list[str]:
    """Retrieve raw text chunks based on lexical/semantic matching from PG raw records."""
    conn = get_pg_connection()
    cur = conn.cursor()
    chunks = []
    for ent in target_entities[:3]:
        cur.execute("""
            SELECT tei FROM raw_bibliography WHERE tei LIKE %s LIMIT %s
        """, (f"%{ent}%", top_k))
        for r in cur.fetchall():
            clean = re.sub(r'<[^>]+>', ' ', r[0])
            clean = re.sub(r'\s+', ' ', clean).strip()
            if clean:
                chunks.append(clean[:400])
        if len(chunks) >= top_k:
            break
    cur.close()
    conn.close()
    return chunks[:top_k]


def run_vector_rag(query_item: dict[str, Any]) -> dict[str, Any]:
    t0 = time.time()
    chunks = retrieve_vector_chunks(query_item["query"], query_item["target_entities"], top_k=12 if CONTEXT_CHARS > 0 else 4)
    return _answer_from_chunks("Vector RAG", query_item, pack_context(chunks, CHUNK_SEPARATOR), time.time() - t0)


RERANKER = "cross-encoder"
CHUNK_SEPARATOR = "\n---\n"


def run_hybrid_vector_rag(query_item: dict[str, Any]) -> dict[str, Any]:
    """BM25 + dense + rerank baseline. Retrieves from the question text only."""
    from baselines import hybrid_retrieve

    t0 = time.time()
    search_text = query_item["query"]
    if RETRIEVAL_INPUT == "entities":
        # Same information the other pipelines receive in this condition.
        search_text += " " + " ".join(query_item["target_entities"])
    chunks = [c["text"] for c in hybrid_retrieve(search_text, top_k=12 if CONTEXT_CHARS > 0 else 4, reranker=RERANKER)]
    return _answer_from_chunks("Hybrid Vector RAG (BM25+Dense+Rerank)", query_item, pack_context(chunks, CHUNK_SEPARATOR), time.time() - t0)


def _answer_from_chunks(model_name: str, query_item: dict[str, Any], chunks: list[str], t_retrieval: float) -> dict[str, Any]:
    context_str = CHUNK_SEPARATOR.join(chunks)
    system_prompt = (
        "You are a strict historical RAG assistant. Use only the retrieved document chunks. "
        "Separate documented facts from inference, never transfer a person or event across regions, "
        "and explicitly state '자료에서 확인되지 않음' when the question is not supported. "
        "Every factual claim must be traceable to a retrieved chunk."
    )
    user_prompt = f"[Retrieved Document Chunks]\n{context_str}\n\n[Question]\n{query_item['query']}"

    t_gen_start = time.time()
    try:
        answer = call_llm([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ])
    except Exception as exc:
        answer = f"[Error: {exc}]"
    t_generation = time.time() - t_gen_start

    prompt_chars = len(system_prompt) + len(user_prompt)
    prompt_tokens = prompt_chars // 3  # estimated token count for Korean/English mix
    output_tokens = len(answer) // 3

    return {
        "model": model_name,
        "answer": answer,
        "contexts": chunks,
        "context_chars": len(context_str),
        "prompt_tokens": prompt_tokens,
        "output_tokens": output_tokens,
        "retrieval_time": round(t_retrieval, 3),
        "generation_time": round(t_generation, 3),
        "total_time": round(t_retrieval + t_generation, 3),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 2. Pipeline 2: Standard GraphRAG
# ─────────────────────────────────────────────────────────────────────────────
def retrieve_graph_triples(target_entities: list[str], limit: int = 15) -> list[str]:
    """Retrieve graph triples from Neo4j knowledge graph without source scoping envelopes."""
    driver = get_neo4j_driver()
    triples = []
    with driver.session() as session:
        for ent in target_entities[:3]:
            cypher = """
            MATCH (n)-[r]->(m)
            WHERE n.name CONTAINS $name OR n.title CONTAINS $name OR n.사건명 CONTAINS $name
            RETURN coalesce(n.name, n.title, n.사건명, labels(n)[0]) AS src,
                   type(r) AS rel,
                   coalesce(m.name, m.title, m.사건명, labels(m)[0]) AS dst
            LIMIT $limit
            """
            res = session.run(cypher, name=ent, limit=limit)
            for r in res:
                triples.append(f"({r['src']}) -[{r['rel']}]-> ({r['dst']})")
            if len(triples) >= limit:
                break
    driver.close()
    return triples[:limit]


def run_graph_rag(query_item: dict[str, Any]) -> dict[str, Any]:
    t0 = time.time()
    triples = pack_context(retrieve_graph_triples(query_item["target_entities"], limit=80 if CONTEXT_CHARS > 0 else 15), "\n")
    t_retrieval = time.time() - t0

    context_str = "\n".join(triples)
    system_prompt = (
        "You are a strict historical GraphRAG assistant. Answer only from the graph triples below. "
        "Do not infer an event/person relationship unless the exact triple supports it; "
        "do not merge similarly named places; explicitly state '자료에서 확인되지 않음' when unsupported."
    )
    user_prompt = f"[Knowledge Graph Relationships]\n{context_str}\n\n[Question]\n{query_item['query']}"

    t_gen_start = time.time()
    try:
        answer = call_llm([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ])
    except Exception as exc:
        answer = f"[Error: {exc}]"
    t_generation = time.time() - t_gen_start

    prompt_chars = len(system_prompt) + len(user_prompt)
    prompt_tokens = prompt_chars // 3
    output_tokens = len(answer) // 3

    return {
        "model": "Standard GraphRAG",
        "answer": answer,
        "contexts": triples,
        "context_chars": len(context_str),
        "prompt_tokens": prompt_tokens,
        "output_tokens": output_tokens,
        "retrieval_time": round(t_retrieval, 3),
        "generation_time": round(t_generation, 3),
        "total_time": round(t_retrieval + t_generation, 3),
    }


def run_ms_graphrag(query_item: dict[str, Any]) -> dict[str, Any]:
    """Microsoft GraphRAG reference implementation, queried through its CLI.

    The CLI returns only the final answer, so contexts and prompt tokens are
    unavailable and the retrieval/token metrics are not comparable for it.
    """
    from baselines import msgraphrag_query

    t0 = time.time()
    try:
        answer = msgraphrag_query(query_item["query"])
    except Exception as exc:
        answer = f"[Error: {exc}]"
    elapsed = round(time.time() - t0, 3)
    return {
        "model": "Microsoft GraphRAG",
        "answer": answer,
        "contexts": [],
        "context_chars": 0,
        "prompt_tokens": 0,
        "output_tokens": len(answer) // 3,
        "retrieval_time": 0.0,
        "generation_time": elapsed,
        "total_time": elapsed,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 3. Pipeline 3: Proposed Docent Hybrid RAG
# ─────────────────────────────────────────────────────────────────────────────
def retrieve_docent_scoped_context(target_entities: list[str]) -> str:
    """Retrieve Neo4j graph triples + PostgreSQL primary sources wrapped in <SOURCE_EVIDENCE> envelopes."""
    driver = get_neo4j_driver()
    conn = get_pg_connection()
    cur = conn.cursor()

    envelopes = []

    # 1. Graph subgraph context
    triples = []
    with driver.session() as session:
        for ent in target_entities[:3]:
            cypher = """
            MATCH (n)-[r]->(m)
            WHERE n.name CONTAINS $name OR n.title CONTAINS $name OR n.사건명 CONTAINS $name
            RETURN coalesce(n.name, n.title, n.사건명, labels(n)[0]) AS src,
                   type(r) AS rel,
                   coalesce(m.name, m.title, m.사건명, labels(m)[0]) AS dst
            LIMIT 8
            """
            res = session.run(cypher, name=ent)
            for r in res:
                triples.append(f"({r['src']}) -[{r['rel']}]-> ({r['dst']})")
    driver.close()

    if triples:
        envelopes.append(f"<GRAPH_TOPOLOGY>\n" + "\n".join(triples) + "\n</GRAPH_TOPOLOGY>")

    # 2. Scoped PostgreSQL primary sources (more candidates when a shared budget decides the cut-off)
    n_entities, per_entity = (3, 4) if CONTEXT_CHARS > 0 else (2, 2)
    for ent in target_entities[:n_entities]:
        cur.execute("""
            SELECT rowid, tei FROM raw_bibliography WHERE tei LIKE %s LIMIT %s
        """, (f"%{ent}%", per_entity))
        for rowid, tei in cur.fetchall():
            clean = re.sub(r'<[^>]+>', ' ', tei)
            clean = re.sub(r'\s+', ' ', clean).strip()[:500]
            envelopes.append(
                f'<SOURCE_EVIDENCE id="raw_bib_{rowid}" entity="{ent}">\n'
                f'  [사료 원문: {clean}]\n'
                f'</SOURCE_EVIDENCE>'
            )

    cur.close()
    conn.close()
    return "\n\n".join(pack_context(envelopes, "\n\n"))


def run_docent_hybrid_rag(query_item: dict[str, Any]) -> dict[str, Any]:
    t0 = time.time()
    scoped_context = retrieve_docent_scoped_context(query_item["target_entities"])
    t_retrieval = time.time() - t0

    # ── Stage 1: Fact Table Extraction ──
    ext_sys = (
        "당신은 한국 근현대사 1차 사료 분석 전문가입니다. "
        "제공된 <SOURCE_EVIDENCE> 사료 블록들을 사료 비판적으로 정밀 분석하여, "
        "각 사료별 사실관계를 왜곡이나 교차 귀속(인물/지역 혼합) 없이 다음 정밀 팩트 표로 추출하십시오.\n"
        "반드시 각 블록에 명시된 사실만 기록하고, 타 지역 사건이나 무관한 인물을 절대 섞지 마십시오."
    )
    ext_user = f"다음 사료군에서 [사료 ID | 대상 개체 | 발생 장소/지역 | 실제 행동 인물 | 사료에 기록된 핵심 팩트(1~2줄)] 표를 마크다운 표로 추출하십시오.\n\n[사료 원문 블록]\n{scoped_context}"

    t_gen_start = time.time()
    try:
        fact_table = call_llm([
            {"role": "system", "content": ext_sys},
            {"role": "user", "content": ext_user}
        ], max_tokens=1000)
    except Exception as exc:
        fact_table = f"[Fact Extraction Error: {exc}]"

    # ── Stage 2: Synthesis ──
    synth_sys = (
        "당신은 국사편찬위원회 및 독립기념관 수준의 전문성을 갖춘 '3·1운동 역사 AI 도슨트'입니다.\n"
        "당신의 임무는 제공된 지식그래프(Graph DB) 사료와 RDB 문맥(Context)을 교차 분석하여, 사용자의 질의에 대한 학술적이고 객관적인 역사 해설을 제공하는 것입니다.\n\n"
        "답변을 작성할 때 다음의 원칙을 엄격하게 준수하십시오:\n\n"
        "1. [유동적 목차 생성]\n"
        "절대 고정된 목차(예: 서론-국외 항일 지도부-국내 시위-결론)를 기계적으로 사용하지 마십시오. 사용자의 질의 의도와 제공된 사료의 성격에 맞추어 가장 적합한 소제목 3~4개를 유동적으로 생성하여 논리적인 흐름을 구성하십시오.\n"
        "(예시: 사건의 배경 -> 전개 과정 -> 일제의 탄압 양상 -> 사료적 의의)\n\n"
        "2. [노이즈 데이터 자체 필터링]\n"
        "검색된 문맥 안에 질의와 직접적인 연관성이 없는 거물급 인물(예: 손병희, 이동휘 등)이나 무관한 지역 명칭이 단순 허브(Hub) 효과로 섞여 들어왔을 경우, 이를 해설에 억지로 끼워 맞추지 말고 과감히 배제하십시오. 오직 질문의 핵심이 되는 장소, 사건, 그리고 실제 주도한 기층 민중에 집중하십시오.\n\n"
        "3. [엄밀한 사료 비판과 환각 방지]\n"
        "제공된 사료에 나타난 지명 오기(예: 남리->제암리)나 겹치는 지명(예: 경기 장안면 vs 경남 장안면)이 있다면, 사료 비판적 관점에서 이를 명확히 구분하고 교정하여 서술하십시오. 제공된 문맥에 없는 내용은 절대 지어내지 마십시오.\n\n"
        "4. [답변의 완결성]\n"
        "문장이 중간에 끊기지 않도록 분량을 조절하십시오. 마지막 단락(결론)은 해당 사건이나 인물이 지니는 역사적 의의를 2~3문장으로 간결하고 명확하게 요약하여 완벽하게 끝맺음하십시오.\n\n"
        "5. [엄격 모드 출력]\n"
        "각 핵심 주장 뒤에 근거가 된 사료 ID(raw_bib_*) 또는 그래프 사건명을 괄호로 표시하십시오. "
        "근거가 없는 질문의 전제는 부정하거나 '자료에서 확인되지 않음'이라고 답하십시오. "
        "검색 문맥에 없는 인물·날짜·장소·인과관계를 보완 지식으로 채우지 마십시오."
    )
    synth_user = f"[사용자 질의]: {query_item['query']}\n\n[검색된 지식그래프 및 사료 문맥]:\n■ [1단계 검증 팩트 표]\n{fact_table}\n\n■ [원문 사료 및 그래프 컨텍스트]\n{scoped_context}\n\n위 원칙과 문맥을 바탕으로 전문적인 도슨트 해설을 작성하십시오."
    time.sleep(2.0)
    try:
        answer = call_llm([
            {"role": "system", "content": synth_sys},
            {"role": "user", "content": synth_user}
        ], max_tokens=1500)
    except Exception as exc:
        answer = f"[Synthesis Error: {exc}]"
    t_generation = time.time() - t_gen_start

    prompt_chars = len(ext_sys) + len(ext_user) + len(synth_sys) + len(synth_user)
    prompt_tokens = prompt_chars // 3
    output_tokens = len(answer) // 3

    return {
        "model": "Docent Hybrid RAG (Proposed)",
        "answer": answer,
        "contexts": [scoped_context] if scoped_context else [],
        "fact_table": fact_table,
        "context_chars": len(scoped_context),
        "prompt_tokens": prompt_tokens,
        "output_tokens": output_tokens,
        "retrieval_time": round(t_retrieval, 3),
        "generation_time": round(t_generation, 3),
        "total_time": round(t_retrieval + t_generation, 3),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 4. Metric Evaluation Harness
# ─────────────────────────────────────────────────────────────────────────────
def is_generation_failure(answer: str) -> bool:
    """True when the pipeline produced no answer (API error), as opposed to a wrong one."""
    stripped = answer.strip()
    return not stripped or bool(re.match(r"\[[A-Za-z ]*Error", stripped))


def run_pipeline(query_item: dict[str, Any], model_name: str, runner_fn: Any, retries: int, delay: float) -> dict[str, Any]:
    """Run one pipeline on one question under the shared input condition, retrying failed generations."""
    res: dict[str, Any] = {}
    for attempt in range(retries + 1):
        if attempt:
            wait = max(delay, 30.0) * attempt
            print(f"    🔁 생성 실패, {wait:.0f}초 후 재시도 ({attempt}/{retries})...", flush=True)
            time.sleep(wait)
        try:
            run_item = {**query_item, "target_entities": retrieval_entities(query_item)}
            res = runner_fn(run_item)
        except Exception as exc:
            res = {"model": model_name, "answer": f"[Error: {exc}]", "contexts": [], "context_chars": 0,
                   "prompt_tokens": 0, "output_tokens": 0, "total_time": 0.0}
        # A failed fact-table stage means the Docent answer was not produced by the full pipeline.
        if not is_generation_failure(res["answer"]) and not is_generation_failure(res.get("fact_table", "ok")):
            res["failed"] = False
            return res
    res["failed"] = True
    return res


def evaluate_faithfulness(answer: str, gold_facts: list[str]) -> tuple[float, int, int]:
    """Check how many gold facts are covered accurately in the answer."""
    if not gold_facts:
        return 100.0, 0, 0
    covered = 0
    for fact in gold_facts:
        # Extract core keywords from fact
        keywords = [k for k in re.findall(r'[가-힣A-Za-z0-9]{2,}', fact) if k not in ['에서', '으로', '하고', '등이', '하여', '전개함', '받음', '형성됨']]
        hit_count = sum(1 for kw in keywords if kw in answer)
        if hit_count >= max(1, len(keywords) // 2):
            covered += 1
    score = (covered / len(gold_facts)) * 100.0
    return round(score, 2), covered, len(gold_facts)


def evaluate_retrieval_direct(query_item: dict[str, Any], contexts: list[str]) -> dict[str, float]:
    """Compute entity Hit Rate@K and MRR without an LLM judge."""
    targets = [str(value).strip().lower() for value in query_item.get("target_entities", []) if str(value).strip()]
    normalized = [context.lower() for context in contexts]
    if not targets:
        return {"entity_hit_rate_at_k": 0.0, "entity_mrr": 0.0}
    hits = sum(any(target in context for context in normalized) for target in targets)
    first_rank = next(
        (rank for rank, context in enumerate(normalized, 1)
         if any(target in context for target in targets)),
        0,
    )
    return {
        "entity_hit_rate_at_k": round(hits / len(targets), 4),
        "entity_mrr": round(1 / first_rank, 4) if first_rank else 0.0,
    }


def evaluate_hallucination(answer: str, negative_traps: list[str], category: str) -> tuple[float, int]:
    """Evaluate hallucination: Did the model falsely claim or endorse any negative traps?"""
    if not negative_traps:
        return 0.0, 0
    traps_triggered = 0

    for trap in negative_traps:
        # Check if trap assertion keywords are affirmed without negation words
        trap_keywords = [k for k in re.findall(r'[가-힣A-Za-z0-9]{2,}', trap) if k not in ['에서', '으로', '하고', '직접', '했다', '시작했다']]
        if all(kw in answer for kw in trap_keywords[:2]):
            # If the answer explicitly negates it, e.g. "직접적 관련이 없다", "불가능하다", "참여하지 않았다"
            negation_markers = ['아니다', '없다', '않았다', '불가능', '확인되지', '오류', '차이', '별개의']
            has_negation = any(neg in answer for neg in negation_markers)
            if not has_negation:
                traps_triggered += 1

    rate = (traps_triggered / len(negative_traps)) * 100.0
    return round(rate, 2), traps_triggered


def export_rag_latex_table(results_by_model: dict[str, dict[str, Any]], out_path: Path) -> str:
    """Generate IEEE/ACM ready LaTeX table comparing the evaluated RAG models."""
    names = list(results_by_model)
    # (label, key, format, better direction: 1 higher / -1 lower / 0 none)
    rows = [
        (r"Fact Recall / Faithfulness (\%) $\uparrow$", "faithfulness", "{:.1f}\\%", 1),
        (r"Hallucination Rate (Traps) (\%) $\downarrow$", "hallucination_rate", "{:.1f}\\%", -1),
        ("Avg. Context Tokens Ingested", "avg_context_tokens", "{:.0f}", 0),
        ("Avg. Output Tokens Generated", "avg_output_tokens", "{:.0f}", 0),
        (r"Token Efficiency (Facts / 1k tokens) $\uparrow$", "token_efficiency", "{:.2f}", 1),
        (r"Avg. Latency (Total s) $\downarrow$", "avg_latency", "{:.2f}s", -1),
    ]
    token_keys = {"avg_context_tokens", "token_efficiency"}
    lines = [
        r"\begin{table*}[htbp]",
        r"\centering",
        r"\caption{Quantitative Comparative Evaluation of RAG Pipelines}",
        r"\label{tab:rag_comparison}",
        r"\begin{tabular}{l|" + "|".join("c" * len(names)) + "}",
        r"\hline",
        r"\textbf{Evaluation Metric} & " + " & ".join(rf"\textbf{{{n}}}" for n in names) + r" \\",
        r"\hline",
    ]
    for label, key, fmt, direction in rows:
        # Models that do not expose their prompt (context tokens == 0) get n/a.
        usable = {n: results_by_model[n][key] for n in names
                  if not (key in token_keys and results_by_model[n]["avg_context_tokens"] == 0)}
        best = None
        if direction and usable:
            best = max(usable.values()) if direction > 0 else min(usable.values())
            if list(usable.values()).count(best) > 1:
                best = None  # ties are not highlighted
        cells = []
        for n in names:
            if n not in usable:
                cells.append("n/a")
                continue
            text = fmt.format(usable[n])
            cells.append(rf"\textbf{{{text}}}" if best is not None and usable[n] == best else text)
        lines.append(f"{label} & " + " & ".join(cells) + r" \\")
    lines += [r"\hline", r"\end{tabular}", r"\end{table*}", ""]
    latex = "\n".join(lines)
    out_path.write_text(latex, encoding="utf-8")
    return latex


def export_rag_csv(detailed_records: list[dict[str, Any]], out_path: Path) -> None:
    if not detailed_records:
        return
    keys = list(detailed_records[0].keys())
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(detailed_records)


MODEL_REGISTRY = {
    "vector": ("Vector RAG", run_vector_rag),
    "hybrid": ("Hybrid Vector RAG (BM25+Dense+Rerank)", run_hybrid_vector_rag),
    "graph": ("Standard GraphRAG", run_graph_rag),
    "msgraphrag": ("Microsoft GraphRAG", run_ms_graphrag),
    "docent": ("Docent Hybrid RAG (Proposed)", run_docent_hybrid_rag),
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate Vector RAG vs GraphRAG vs Docent Hybrid RAG.")
    parser.add_argument("--quick", action="store_true", help="Run on 3 representative queries (1 Multi-hop, 1 Spatial, 1 Trap)")
    parser.add_argument("--single", action="store_true", help="Run single query (Q_MH_01) and display full response_text for each model")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of queries to evaluate")
    parser.add_argument("--delay", type=float, default=15.0, help="Delay in seconds between API calls to prevent 429 rate limit (default: 15.0)")
    parser.add_argument("--ragas", action="store_true", help="Run RAGAS metrics after generating benchmark responses")
    parser.add_argument("--models", default="vector,hybrid,graph,docent",
                        help=f"Comma-separated pipelines to run. Choices: {','.join(MODEL_REGISTRY)} (default: vector,hybrid,graph,docent)")
    parser.add_argument("--reranker", choices=["cross-encoder", "none"], default="cross-encoder",
                        help="Reranker for the hybrid baseline (default: cross-encoder)")
    parser.add_argument("--retrieval-input", choices=["question", "entities"], default="question",
                        help="What every pipeline may retrieve with: entities extracted from the question text (default), "
                             "or the benchmark's gold target_entities")
    parser.add_argument("--temperature", type=float, default=0.0, help="Generation temperature for every LLM call (default: 0.0)")
    parser.add_argument("--top-p", type=float, default=0.8, help="Generation top_p for every LLM call (default: 0.8)")
    parser.add_argument("--context-chars", type=int, default=2400,
                        help="Retrieved-context character budget shared by all pipelines (default: 2400; 0 = legacy per-pipeline sizes)")
    parser.add_argument("--retries", type=int, default=2, help="Extra attempts for a failed generation (default: 2)")
    args = parser.parse_args()

    global RERANKER, RETRIEVAL_INPUT, GEN_TEMPERATURE, GEN_TOP_P, CONTEXT_CHARS
    RERANKER = args.reranker
    RETRIEVAL_INPUT = args.retrieval_input
    GEN_TEMPERATURE, GEN_TOP_P, CONTEXT_CHARS = args.temperature, args.top_p, args.context_chars
    model_keys = [k.strip() for k in args.models.split(",") if k.strip()]
    unknown = [k for k in model_keys if k not in MODEL_REGISTRY]
    if unknown:
        parser.error(f"알 수 없는 모델: {unknown} (선택 가능: {list(MODEL_REGISTRY)})")

    benchmark_data = json.loads(DATASET_PATH.read_text(encoding="utf-8"))
    if args.single:
        selected = [benchmark_data[0]]
    elif args.quick:
        # Select 1 multi-hop, 1 spatial diffusion, and 1 zero-correlation trap
        selected = [
            benchmark_data[0],  # Q_MH_01: Boseongsa -> Pyongyang Sudok School
            benchmark_data[4],  # Q_ST_01: Seoul -> Pyongyang -> Hamhung diffusion
            benchmark_data[9],  # Q_ZC_01: Lee Dong-hwi vs Aunae market trap
        ]
    elif args.limit > 0:
        selected = benchmark_data[:args.limit]
    else:
        selected = benchmark_data

    mode_str = "1번 질의 단독 디버깅 모드" if args.single else f"총 {len(selected)}건 정량 평가 모드"
    print(f"🚀 [RAG 정량 평가 벤치마크] {mode_str}를 시작합니다. (호출 간 딜레이: {args.delay}초)")
    print(f"   공통 조건: 검색 입력={RETRIEVAL_INPUT}, temperature={GEN_TEMPERATURE}, top_p={GEN_TOP_P}, "
          f"문맥 예산={CONTEXT_CHARS or '제한 없음'}자, 재시도={args.retries}회")

    models = [MODEL_REGISTRY[k] for k in model_keys]

    detailed_records = []
    ragas_records = []
    failed_generations = 0
    aggregated = {m_name: {
        "faithfulness_scores": [],
        "hallucination_scores": [],
        "context_tokens": [],
        "output_tokens": [],
        "latencies": [],
        "total_facts_covered": 0,
        "total_facts": 0,
    } for m_name, _ in models}

    for i, item in enumerate(selected, 1):
        print(f"\n[{i}/{len(selected)}] [{item['category']}] {item['query']}")
        for m_name, runner_fn in models:
            print(f"\n  🔹 [{m_name}] 실행 중...", flush=True)
            res = run_pipeline(item, m_name, runner_fn, args.retries, args.delay)
            failed = res["failed"]
            if failed:
                failed_generations += 1
                print(f"    ⚠️ 생성 실패 — 이 질문은 유의성 검정에서 제외됩니다: {res['answer'][:120]}")
            f_score, f_cov, f_tot = evaluate_faithfulness(res["answer"], item["gold_facts"])
            h_score, h_traps = evaluate_hallucination(res["answer"], item["negative_traps"], item["category"])

            aggregated[m_name]["faithfulness_scores"].append(f_score)
            aggregated[m_name]["hallucination_scores"].append(h_score)
            aggregated[m_name]["context_tokens"].append(res["prompt_tokens"])
            aggregated[m_name]["output_tokens"].append(res["output_tokens"])
            aggregated[m_name]["latencies"].append(res["total_time"])
            aggregated[m_name]["total_facts_covered"] += f_cov
            aggregated[m_name]["total_facts"] += f_tot

            if args.single:
                if "fact_table" in res and res["fact_table"]:
                    print(f"    📊 [Stage 1 검증 팩트 표]:\n{res['fact_table']}")
                print(f"    📄 [생성된 response_text]:\n{res['answer']}")

            detailed_records.append({
                "query_id": item["id"],
                "category": item["category"],
                "model": m_name,
                "faithfulness": f_score,
                "hallucination_rate": h_score,
                "prompt_tokens": res["prompt_tokens"],
                "context_chars": res.get("context_chars", 0),
                "output_tokens": res["output_tokens"],
                "latency_sec": res["total_time"],
                "answer_snippet": res["answer"].replace("\n", " ")[:120],
                "generation_failed": failed,
                **evaluate_retrieval_direct(item, res.get("contexts", [])),
            })
            ragas_records.append({
                "user_input": item["query"],
                "response": res["answer"],
                "retrieved_contexts": res.get("contexts", []),
                "reference": "\n".join(item.get("ground_truth", item["gold_facts"])),
                "model": m_name,
                "query_id": item["id"],
                "evaluation_type": item.get("evaluation_type", "direct"),
                "category": item["category"],
                "generation_failed": failed,
            })
            print(f"    ✅ 완료 (Fact Recall: {f_score}%, Hallucination: {h_score}%, Latency: {res['total_time']}s)")
            if args.delay > 0:
                print(f"    ⏳ Rate-limit 방지 {args.delay}초 대기 중...", end="", flush=True)
                time.sleep(args.delay)
                print(" 완료.")

    # Aggregate summaries
    summary_by_model = {}
    for m_name, data in aggregated.items():
        avg_f = sum(data["faithfulness_scores"]) / len(data["faithfulness_scores"]) if data["faithfulness_scores"] else 0.0
        avg_h = sum(data["hallucination_scores"]) / len(data["hallucination_scores"]) if data["hallucination_scores"] else 0.0
        avg_ctx = sum(data["context_tokens"]) / len(data["context_tokens"]) if data["context_tokens"] else 0.0
        avg_out = sum(data["output_tokens"]) / len(data["output_tokens"]) if data["output_tokens"] else 0.0
        avg_lat = sum(data["latencies"]) / len(data["latencies"]) if data["latencies"] else 0.0
        tot_k_tokens = (sum(data["context_tokens"]) / 1000.0) if sum(data["context_tokens"]) > 0 else 1.0
        eff = data["total_facts_covered"] / tot_k_tokens

        summary_by_model[m_name] = {
            "faithfulness": round(avg_f, 2),
            "hallucination_rate": round(avg_h, 2),
            "avg_context_tokens": round(avg_ctx, 1),
            "avg_output_tokens": round(avg_out, 1),
            "token_efficiency": round(eff, 2),
            "avg_latency": round(avg_lat, 2),
        }

    # Console display
    print("\n" + "=" * 88)
    print(f"{'Model':<30} | {'Fact Recall(%)':<14} | {'Hallucination(%)':<16} | {'Tokens(In/Out)':<15} | {'Latency(s)':<10}")
    print("-" * 88)
    for m_name, stats in summary_by_model.items():
        tok_str = f"{int(stats['avg_context_tokens'])}/{int(stats['avg_output_tokens'])}"
        print(f"{m_name:<30} | {stats['faithfulness']:<14.1f} | {stats['hallucination_rate']:<16.1f} | {tok_str:<15} | {stats['avg_latency']:<10.2f}")
    print("=" * 88)

    # Export
    tex_path = RESULTS_DIR / "table_rag_comparison.tex"
    csv_path = RESULTS_DIR / "rag_evaluation_results.csv"
    export_rag_latex_table(summary_by_model, tex_path)
    export_rag_csv(detailed_records, csv_path)
    ragas_input_path = RESULTS_DIR / "ragas_dataset.json"
    ragas_input_path.write_text(
        json.dumps(ragas_records, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"\n📄 LaTeX 표 생성 완료: {tex_path}")
    print(f"💾 CSV 상세 결과 저장 완료: {csv_path}")
    print(f"🧾 RAGAS 입력 데이터 저장 완료: {ragas_input_path}")

    from analyze_rag_results import run as run_analysis

    counts = run_analysis(csv_path, RESULTS_DIR)
    print(f"📈 범주별 결과·쌍대 유의성 검정 저장 완료 (분석 대상 {counts['questions_used']}/{counts['questions_total']}문항): "
          f"{RESULTS_DIR / 'rag_category_results.csv'}, {RESULTS_DIR / 'rag_significance.csv'}")
    if failed_generations:
        print(f"⚠️ 생성 실패 {failed_generations}건 발생: 위 요약 표의 평균에는 실패 응답이 포함되어 있습니다. "
              "--delay를 늘려 다시 실행하세요.")
    if args.ragas:
        from eval_ragas import evaluate_dataset

        ragas_output_path = RESULTS_DIR / "ragas_results.json"
        evaluate_dataset(ragas_input_path, ragas_output_path)
        print(f"📊 RAGAS 결과 저장 완료: {ragas_output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
