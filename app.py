import streamlit as st
import os
from neo4j import GraphDatabase
from streamlit_agraph import agraph, Node, Edge, Config
import json
import hashlib
import random
import time

try:
    import psycopg2
except Exception:
    psycopg2 = None

# RAG-related optional deps: FAISS/numpy for vector search; ollama for local LM fallback
try:
    import faiss
    import numpy as np
except Exception:
    faiss = None
    np = None

# keep ollama as fallback for local model
try:
    import ollama
except Exception:
    ollama = None


def _secret_or_env(key, default=""):
    """Streamlit secrets → 환경변수 → 기본값 순서로 설정값 가져오기"""
    # 1. Streamlit secrets에서 확인 (secrets.toml 자동 로드)
    if key in st.secrets:
        return str(st.secrets[key])
    
    # 2. 환경변수 확인
    val = os.getenv(key)
    if val:
        return str(val)
    
    # 3. 기본값 사용
    return str(default) if default else ""


# ==========================================
# 1. Neo4j 접속 정보 (.streamlit/secrets.toml 또는 환경변수)
# ==========================================
NEO4J_CONFIG = {
    "uri": _secret_or_env("NEO4J_URI", "bolt://11e.kr:7687"),
    "user": _secret_or_env("NEO4J_USER", "neo4j"),
    "password": _secret_or_env("NEO4J_PASSWORD", "")  # secrets.toml에서만 로드
}

# optional user-provided Hangul->Hanja mapping (data/hangul_to_hanja.json)
HANJA_MAP = {}
try:
    with open(os.path.join(os.getcwd(), 'data', 'hangul_to_hanja.json'), 'r', encoding='utf-8') as _f:
        HANJA_MAP = json.load(_f)
except Exception:
    HANJA_MAP = {}


class HybridHistoryDocent:
    def __init__(self):
        self.neo4j_driver = None
        try:
            if not NEO4J_CONFIG["password"]:
                st.warning("⚠️ NEO4J_PASSWORD가 설정되지 않았습니다.")
                return
            self.neo4j_driver = GraphDatabase.driver(NEO4J_CONFIG["uri"], auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"]))
        except Exception as e:
            st.error(f"❌ Neo4j 연결 실패: {e}")
    
    def close(self):
        if self.neo4j_driver:
            self.neo4j_driver.close()

    def _run_and_log(self, session, cypher, **params):
        """Run a Neo4j statement and record the cypher + params into Streamlit session state for debugging."""
        try:
            entry = {"cypher": cypher, "params": params}
            st.session_state.setdefault('last_neo4j_queries', []).append(entry)
        except Exception:
            pass
        return session.run(cypher, **params)

    def run_one_shot_query(self, terms, limit=200):
        """Run a single consolidated Cypher that finds nodes matching any term and their neighbors.

        Returns (cypher, params, simplified_results_list).
        """
        terms = [t for t in (terms or []) if t]
        # dedupe and limit terms to avoid excessive ORs
        terms = list(dict.fromkeys(terms))[:10]
        cypher = """
        MATCH (n)
        WHERE any(term IN $terms WHERE any(k IN ['명칭','제목','사건명','id','name','title']
              WHERE n[k] IS NOT NULL AND toLower(toString(n[k])) CONTAINS toLower(term)))
        OPTIONAL MATCH (n)-[r]-(m)
        RETURN DISTINCT n, r, m
        LIMIT $limit
        """
        params = {"terms": terms, "limit": int(limit)}
        results = []
        if not self.neo4j_driver:
            return cypher, params, results

        with self.neo4j_driver.session() as session:
            try:
                cursor = self._run_and_log(session, cypher, **params)
                for rec in cursor:
                    n = rec.get('n')
                    r = rec.get('r')
                    m = rec.get('m')
                    results.append({
                        'n_labels': list(n.labels) if n else [],
                        'n_props': dict(n.items()) if n else None,
                        'rel_type': getattr(r, 'type', None) if r else None,
                        'rel_props': dict(r.items()) if r else None,
                        'm_labels': list(m.labels) if m else [],
                        'm_props': dict(m.items()) if m else None,
                    })
            except Exception as e:
                # record the error as a result for visibility
                results.append({"error": str(e)})

        return cypher, params, results
    def get_hybrid_context(self, term):
        t_start = time.monotonic()
        found_ids = set()
        evidences = []
        nodes, edges = {}, []
        
        # --- [Step 1] Neo4j: 모든 노드 레이블에서 검색 ---
        try:
            if not self.neo4j_driver:
                st.warning("⚠️ Neo4j 연결 실패")
                return nodes, edges, evidences
            
            with self.neo4j_driver.session() as session:
                # prepare search terms: include hanja equivalents if present in mapping
                search_terms = [term]
                if term in HANJA_MAP:
                    search_terms.append(HANJA_MAP[term])
                else:
                    for k, v in HANJA_MAP.items():
                        if k and k in term and v:
                            search_terms.append(v)

                # Prefer fulltext index search if available (better tokenization for CJK)
                use_fulltext = True
                try:
                    self._run_and_log(session, "CALL db.index.fulltext.queryNodes(\"namesIndex\", \"probe\") YIELD node RETURN node LIMIT 1").consume()
                except Exception:
                    use_fulltext = False

                results_nodes = []
                if use_fulltext:
                    for st in list(dict.fromkeys(search_terms)):
                        try:
                            q = self._run_and_log(session, "CALL db.index.fulltext.queryNodes(\"namesIndex\", $q) YIELD node, score RETURN DISTINCT node, score ORDER BY score DESC LIMIT 50", q=st)
                            for rec in q:
                                results_nodes.append(rec['node'])
                        except Exception:
                            use_fulltext = False
                            break

                if not use_fulltext:
                    # fallback: case-insensitive CONTAINS across common fields
                    search_query = """
                    MATCH (n)
                    WHERE any(k IN ['명칭','제목','사건명','id','name','title'] 
                              WHERE n[k] IS NOT NULL AND toLower(toString(n[k])) CONTAINS toLower($term))
                    RETURN DISTINCT n
                    LIMIT 50
                    """
                    for st in list(dict.fromkeys(search_terms)):
                        res = self._run_and_log(session, search_query, term=st)
                        for record in res:
                            results_nodes.append(record['n'])

                # process resulting nodes
                for node in results_nodes:
                    node_id = node.get('id') or node.get('명칭') or node.get('name') or node.get('title') or 'unknown'
                    found_ids.add(str(node_id))

                    labels = list(node.labels)
                    if '문건' in labels:
                        evidences.append({
                            "doc": node.get('제목', '제목 미상'),
                            "quote": node.get('설명', '')[:100],
                            "concept": node_id,
                            "text": node.get('설명', '')[:200]
                        })
                    elif '사건' in labels:
                        evidences.append({
                            "doc": node.get('사건명', '사건명 미상'),
                            "quote": node.get('날짜', ''),
                            "concept": node_id,
                            "text": f"날짜: {node.get('날짜', '')}"
                        })
        except Exception as e:
            st.warning(f"⚠️ Neo4j 초기 검색 실패: {e}")
        # record time after initial search
        try:
            t_after_search = time.monotonic()
            st.session_state['last_neo4j_timings'] = st.session_state.get('last_neo4j_timings', {})
            st.session_state['last_neo4j_timings'].update({'search_seconds': round(t_after_search - t_start, 3)})
        except Exception:
            pass

        # --- [Step 2] Neo4j: 그래프 확장 및 관계 탐색 ---
        try:
            if not self.neo4j_driver:
                return nodes, edges, evidences
            
            with self.neo4j_driver.session() as session:
                # 찾은 노드들의 직접 연결 관계와 간접 연결 모두 찾기
                graph_query = """
                MATCH (n)
                WHERE any(k IN ['id','명칭','name','title'] WHERE n[k] IS NOT NULL AND toString(n[k]) IN $search_ids)
                OPTIONAL MATCH (n)-[r]-(m)
                RETURN DISTINCT n, r, m, labels(n) as n_labels, labels(m) as m_labels
                """
                results = self._run_and_log(session, graph_query, search_ids=list(found_ids))
                
                edges_set = set()
                
                def add_node(node, labels_list):
                    if not node:
                        return
                    # 원래 id/명칭을 raw_id로 보존하고, 컴포넌트 파일 요청 이슈를 피하기 위해 ASCII-safe 해시 id 사용
                    raw_id = node.get('id') or node.get('명칭') or node.get('name') or node.get('title') or getattr(node, 'element_id', getattr(node, 'id', 'unknown'))
                    nid = 'n' + hashlib.sha1(str(raw_id).encode('utf-8')).hexdigest()[:12]
                    if nid in nodes:
                        return nid

                    # 레이블에 따라 표시용 라벨과 색 결정 (표시는 한국어 유지)
                    if '문건' in labels_list:
                        display_label = "📜\n" + str(node.get('제목', '문건'))[:20]
                        color = "#F7A01F"
                    elif '인물' in labels_list:
                        display_label = "👤\n" + str(node.get('명칭', raw_id))
                        color = "#1CE1D4"
                    elif '사건' in labels_list:
                        display_label = "🔥\n" + str(node.get('사건명', raw_id))[:15]
                        color = "#FF6B6B"
                    elif '장소' in labels_list:
                        display_label = "📍\n" + str(node.get('명칭', raw_id))[:15]
                        color = "#4ECDC4"
                    elif '기관' in labels_list:
                        display_label = "🏢\n" + str(node.get('명칭', raw_id))[:15]
                        color = "#95E1D3"
                    else:
                        display_label = str(raw_id)
                        color = "#999999"

                    nodes[nid] = {"label": display_label, "color": color, "shape": "box", "raw_id": str(raw_id)}
                    return nid
                
                for rec in results:
                    n = rec['n']
                    m = rec['m']
                    r = rec['r']
                    n_labels = rec['n_labels']
                    m_labels = rec['m_labels']

                    n_nid = add_node(n, n_labels)
                    m_nid = add_node(m, m_labels) if m else None

                    # if there's a relationship, add an edge using hashed ids
                    if r is not None and n_nid and m_nid:
                        try:
                            rel_type = getattr(r, 'type', None) or r.__class__.__name__
                        except Exception:
                            rel_type = 'relatedTo'
                        edge = {'source': n_nid, 'target': m_nid, 'label': str(rel_type)}
                        # avoid duplicates
                        if edge not in edges:
                            edges.append(edge)
        except Exception as e:
            st.warning(f"⚠️ Neo4j 그래프 확장 실패: {e}")
        # record time after graph expansion
        try:
            t_after_expand = time.monotonic()
            timings = st.session_state.get('last_neo4j_timings', {})
            timings.update({'expand_seconds': round(t_after_expand - (t_after_search if 't_after_search' in locals() else t_start), 3),
                            'total_seconds': round(t_after_expand - t_start, 3)})
            st.session_state['last_neo4j_timings'] = timings
        except Exception:
            pass

        # 항상 튜플 반환: 노드/엣지/증거 리스트
        return nodes, edges, evidences

def load_faiss_index():
    """Attempt to load a FAISS index and its metadata.
    This is a conservative stub: if FAISS or numpy is unavailable, return (None, None, None).
    If you have a saved index and meta, extend this function to load them from files.
    """
    if faiss is None or np is None:
        return None, None, None
    try:
        # Placeholder: no default index path configured. Return None to indicate not available.
        return None, None, None
    except Exception:
        return None, None, None


# OpenAI integration removed from this app; using ollama/simple fallback instead


def _pg_conn():
    """Create a psycopg2 connection using Streamlit secrets or env vars."""
    if psycopg2 is None:
        return None
    host = _secret_or_env('PG_HOST', 'localhost')
    port = int(_secret_or_env('PG_PORT', '5432'))
    user = _secret_or_env('PG_USER', 'postgres')
    password = _secret_or_env('PG_PASSWORD', '')
    dbname = _secret_or_env('PG_DATABASE', 'postgres')
    try:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
        return conn
    except Exception:
        return None


def fetch_pg_rows_for_node(name, limit_per_table=10, max_text_cols=6):
    """Search all tables with a `tei` column for rows matching `name`.
    Returns list of dicts: {table, schema, rowid, id_col, match_cols, snippets, tei}
    """
    conn = _pg_conn()
    if conn is None:
        return []
    cur = conn.cursor()
    out = []
    queries_log = []
    try:
        cur.execute("""
        SELECT table_schema, table_name
        FROM information_schema.columns
        WHERE column_name = 'tei'
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        GROUP BY table_schema, table_name
        """)
        queries_log.append(("SELECT table_schema, table_name FROM information_schema.columns WHERE column_name = 'tei' AND table_schema NOT IN ('pg_catalog', 'information_schema') GROUP BY table_schema, table_name", None))
        tables = cur.fetchall()
        for schema, table in tables:
            fq = f'"{schema}"."{table}"' if schema and schema != 'public' else f'"{table}"'
            # determine the table's first column to use as id (ordinal_position=1)
            try:
                cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position ASC
                LIMIT 1
                """, (schema or 'public', table))
                first_col_res = cur.fetchone()
                id_col = first_col_res[0] if first_col_res else 'rowid'
            except Exception:
                id_col = 'rowid'

            # Discover text-like columns to search (limit for performance)
            text_cols = []
            try:
                cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                  AND data_type IN ('character varying','text','character')
                ORDER BY ordinal_position ASC
                """, (schema or 'public', table))
                for (cname,) in cur.fetchall():
                    if cname not in text_cols:
                        text_cols.append(cname)
                # prioritize common semantic columns if present
                for cname in ['명칭', '사건명', '제목']:
                    if cname in text_cols:
                        text_cols.remove(cname)
                        text_cols.insert(0, cname)
            except Exception:
                text_cols = []

            # build search predicates across id_col, tei, and a limited set of text columns
            search_cols = [id_col]
            if 'tei' not in search_cols:
                search_cols.append('tei')
            for cname in text_cols:
                if cname not in search_cols:
                    search_cols.append(cname)
                if len(search_cols) >= max_text_cols + 2:
                    break

            or_clauses = ' OR '.join([f'"{c}"::text ILIKE %s' for c in search_cols])
            q = f'SELECT * FROM {fq} WHERE ({or_clauses}) LIMIT {limit_per_table}'
            params = [f'%{name}%'] * len(search_cols)
            try:
                # log the parameterized query before execution
                queries_log.append((q, list(params)))
                cur.execute(q, params)
                rows = cur.fetchall()
                # map columns dynamically from cursor description
                cols = [d[0] for d in cur.description]
                # collect raw rows for debugging
                raw_rows = []
                for row in rows:
                    rowdict = {
                        'table': table,
                        'schema': schema,
                        'rowid': None,
                        'id_col': id_col,
                        'match_cols': search_cols,
                        'tei': None,
                        'snippets': {}
                    }
                    raw_row = {}
                    for idx, col in enumerate(cols):
                        val = row[idx]
                        raw_row[col] = val
                        if col == id_col:
                            rowdict['rowid'] = val
                        elif col == 'tei':
                            rowdict['tei'] = val
                        else:
                            rowdict['snippets'][col] = str(val) if val is not None else ''
                    raw_rows.append(raw_row)
                    out.append(rowdict)
                # attach raw rows to session state for UI inspection
                try:
                    st.session_state.setdefault('last_raw_rows', []).extend([{'table': table, 'rows': raw_rows}])
                except Exception:
                    pass
            except Exception:
                # skip problematic table
                continue
    finally:
        cur.close()
        conn.close()
        # store executed queries in session state for UI inspection
        try:
            st.session_state['last_queries'] = queries_log
        except Exception:
            pass
    return out


def fetch_pg_rows_for_nodes(names, limit_per_table=2, max_nodes=10):
    """Prefetch PG rows for multiple node names with simple session cache."""
    if not names:
        return []
    cache = st.session_state.setdefault('pg_cache', {})
    out = []
    seen = set()
    for name in names:
        if not name or name in seen:
            continue
        seen.add(name)
        if len(seen) > max_nodes:
            break
        if name in cache:
            rows = cache[name]
        else:
            rows = fetch_pg_rows_for_node(name, limit_per_table=limit_per_table)
            cache[name] = rows
        for r in rows:
            r['node_id'] = name
        out.extend(rows)
    return out


def _compact_row_snippets(rowdict, max_fields=6):
    """Build a compact text summary from row snippets for storytelling."""
    if not rowdict:
        return ""
    priority = ['제목', '사건명', '명칭', '경성지법검사국문서', '大正8特豫第8號']
    snippets = rowdict.get('snippets') or {}
    fields = []
    for k in priority:
        v = snippets.get(k)
        if v:
            fields.append(f"{k}: {v}")
    for k, v in snippets.items():
        if len(fields) >= max_fields:
            break
        if not v or k in priority:
            continue
        fields.append(f"{k}: {v}")
    return "; ".join(fields)


def build_pg_context(pg_rows, max_rows=20):
    """Create compact PG-derived context strings for storytelling."""
    if not pg_rows:
        return []
    items = []
    for r in pg_rows[:max_rows]:
        table = f"{r.get('schema')}.{r.get('table')}" if r.get('schema') else str(r.get('table'))
        nid = r.get('node_id') or ''
        rowid = r.get('rowid')
        snippet = _compact_row_snippets(r)
        if snippet:
            items.append(f"[{table}] node={nid} rowid={rowid} :: {snippet}")
    return items


# OpenAI embedding removed (not used)


def simple_fallback_summary(search_term, evidences, retrieved=None):
    """Compose a short, non-AI summary from evidences and optional retrieved texts."""
    parts = [f"'{search_term}' 관련 근거 요약:"]
    if not evidences and not retrieved:
        return parts[0] + " 근거가 충분치 않습니다."

    if evidences:
        for e in evidences[:5]:
            doc = e.get('doc', '문서명 미상')
            concept = e.get('concept', '')
            text = e.get('text', '')
            parts.append(f"- 문서: {doc} (대상: {concept}) — {text}")

    if retrieved:
        parts.append("추가 연관 문서:")
        for r in (retrieved[:5] if isinstance(retrieved, list) else [retrieved]):
            if isinstance(r, str):
                parts.append(f"- {r[:300]}")
            else:
                parts.append(f"- {str(r)[:300]}")

    parts.append("\n요약: 위의 근거들을 바탕으로 관련 문헌과 사료에서 핵심 정보를 확인해 주세요.")
    return "\n".join(parts)


def get_docent():
    """Return a cached HybridHistoryDocent instance (stored in Streamlit session state)."""
    if 'docent' not in st.session_state:
        st.session_state['docent'] = HybridHistoryDocent()
    return st.session_state['docent']

# ==========================================
# 3. UI 및 Gemma 3:12b 해설 파트
# ==========================================
st.set_page_config(layout="wide", page_title="역사 도슨트")
st.title("🇰🇷 대한민국 역사 도슨트 (Gemma4:26b)")

if "search_term" not in st.session_state:
    st.session_state.search_term = "유관순"
if "last_explained_term" not in st.session_state:
    st.session_state.last_explained_term = ""
if "last_explanation" not in st.session_state:
    st.session_state.last_explanation = ""
if "graph_epoch" not in st.session_state:
    st.session_state.graph_epoch = 0

with st.sidebar:
    st.header("🔍 통합 검색")
    term_input = st.text_input("검색어 입력", value=st.session_state.search_term)
    if st.button("탐색"):
        st.session_state.search_term = term_input
        st.rerun()

search_term = st.session_state.search_term.strip()
if len(search_term) < 2:
    st.warning("검색어는 2글자 이상 입력해 주세요.")
    st.stop()

docent = get_docent()
nodes_map, edges_list, evidences = docent.get_hybrid_context(search_term)

# Prefetch PG rows for visible nodes to reduce hallucination in storytelling
try:
    raw_ids = []
    for _k, _v in (nodes_map or {}).items():
        rid = _v.get('raw_id')
        if rid:
            raw_ids.append(str(rid))
    if raw_ids and st.session_state.get('pg_prefetch_term') != search_term:
        with st.spinner('PG 근거를 빠르게 수집 중...'):
            t_pg0 = time.monotonic()
            pg_rows = fetch_pg_rows_for_nodes(raw_ids, limit_per_table=2, max_nodes=12)
            t_pg1 = time.monotonic()
        st.session_state['pg_prefetch_term'] = search_term
        st.session_state['pg_prefetch_rows'] = pg_rows
        st.session_state['pg_prefetch_texts'] = build_pg_context(pg_rows)
        try:
            st.session_state['last_pg_prefetch_seconds'] = round(t_pg1 - t_pg0, 3)
        except Exception:
            pass
except Exception:
    pass

c1, c2 = st.columns([1.2, 1])
with c1:
    st.subheader(f"🕸️ '{search_term}' 지식망")
    st.caption("색상 안내: 📜 문건(#F7A01F), 👤 인물(#1CE1D4), 🔥 사건(#FF6B6B), 📍 장소(#4ECDC4), 🏢 기관(#95E1D3), 기타(#999999)")
    if st.button("그래프 정렬(재배치)"):
        st.session_state.graph_epoch += 1
        st.rerun()
    if nodes_map:
        # Build Node objects with display label and include raw_id as tooltip/title
        nodes = []
        for k, v in nodes_map.items():
            node_label = v.get('label')
            raw_id = v.get('raw_id', '')
            # pass raw_id as `title` so tooltip shows the original identifier/name
            nodes.append(Node(id=k, label=node_label, color=v.get('color'), shape=v.get('shape'), size=25, title=str(raw_id)))
        # convert edge dicts to streamlit_agraph Edge objects
        edges = []
        try:
            for e in edges_list:
                if isinstance(e, dict):
                    edges.append(Edge(source=e.get('source'), target=e.get('target'), label=e.get('label', '')))
                else:
                    edges.append(e)
        except Exception:
            edges = []

        # Shuffle node/edge order to encourage re-layout when requested
        if st.session_state.graph_epoch:
            rng = random.Random(st.session_state.graph_epoch)
            rng.shuffle(nodes)
            rng.shuffle(edges)

        # Prefer a repulsion-based solver and larger spacing to avoid node collisions
        config = Config(
            width=900,
            height=700,
            directed=True,
            physics=True,
            hierarchical=False,
            solver='repulsion',
            minVelocity=0.1,
            maxVelocity=50,
            stabilization=True,
            timestep=0.5,
            # layout hints (nodeSpacing used if hierarchical mode is enabled, but keep larger default)
            nodeSpacing=200,
            levelSeparation=200,
        )
        clicked = agraph(nodes=nodes, edges=edges, config=config)

        # 한방 쿼리(디버그용): 현재 검색어로 한 번에 Neo4j에서 노드+이웃을 가져오고 쿼리/결과를 표시
        try:
            if st.button("한방 Neo4j 쿼리 실행 (디버그)"):
                # build simple search terms (include hanja mapping if available)
                terms = [search_term]
                if search_term in HANJA_MAP:
                    terms.append(HANJA_MAP[search_term])
                cy, params, res = docent.run_one_shot_query(terms, limit=200)
                with st.expander('한방 쿼리: Cypher', expanded=True):
                    st.code(cy, language='cypher')
                    st.markdown('- params:')
                    st.json(params)
                with st.expander(f'결과 미리보기 ({len(res)} 레코드)', expanded=True):
                    for item in res[:50]:
                        if 'error' in item:
                            st.error(item['error'])
                        else:
                            try:
                                st.write(item['n_labels'], item.get('n_props'))
                                if item.get('rel_type'):
                                    st.write(' -> ', item.get('rel_type'), item.get('rel_props'))
                                if item.get('m_props'):
                                    st.write(' <- ', item.get('m_labels'), item.get('m_props'))
                                st.markdown('---')
                            except Exception:
                                st.write(item)
        except Exception:
            pass
        if clicked and clicked != search_term:
            # Resolve clicked value to nodes_map key (clicked may be id, label, or raw_id)
            resolved_key = None
            if clicked in nodes_map:
                resolved_key = clicked
            else:
                for k, v in nodes_map.items():
                    if v.get('label') == clicked or v.get('raw_id') == clicked:
                        resolved_key = k
                        break
            if resolved_key is None:
                resolved_key = clicked

            # Prevent repeated fetch loops: only act if this click is new
            if st.session_state.get('last_clicked') == resolved_key:
                pass
            else:
                raw_name = nodes_map.get(resolved_key, {}).get('raw_id', clicked)
                # set display name to raw_name so bottom panel shows human-friendly id
                st.session_state.selected_node = raw_name
                with st.spinner(f"'{raw_name}' 관련 Postgres 조회 중..."):
                    try:
                        rows = fetch_pg_rows_for_node(raw_name)
                    except Exception as e:
                        st.error(f"Postgres 조회 실패: {e}")
                        rows = []
                st.session_state.selected_node_rows = rows
                st.session_state['last_clicked'] = resolved_key
                st.rerun()
        # clicked handling complete; do nothing if user didn't click
        
    else:
        st.info("그래프를 구성할 데이터가 없습니다.")

with c2:
    st.subheader("📜 도슨트 해설")
    pg_texts = st.session_state.get('pg_prefetch_texts') or []
    
    # 해설 생성 버튼을 눈에 띄게 상단에 배치
    generate_clicked = st.button("✨ 해설 생성", key="generate_explanation", type="primary", use_container_width=True)
    
    if evidences or pg_texts:
        context_str = ""
        if evidences:
            context_str = "\n".join([f"- 문서: {e['doc']}\n  내용: {e['text']}...\n  (주석: {e['quote']})" for e in evidences[:5]])
        if pg_texts:
            if context_str:
                context_str += "\n\n"
            context_str += "[PG 근거]\n" + "\n".join(f"- {t}" for t in pg_texts[:20])
            st.caption(f"PG 근거 {len(pg_texts)}건 반영")
            # show which nodes were actually included in PG prefetch
            pg_rows = st.session_state.get('pg_prefetch_rows') or []
            node_list = []
            for r in pg_rows:
                nid = r.get('node_id')
                if nid and nid not in node_list:
                    node_list.append(nid)
            with st.expander("PG 근거에 포함된 노드", expanded=False):
                if node_list:
                    for nid in node_list:
                        st.write(nid)
                else:
                    st.caption("포함된 노드가 없습니다.")
        st.markdown("**RAG 설정**: 로컬 ollama를 우선 사용하며, 없으면 간단 요약(fallback)을 제공합니다.")

        if generate_clicked:
            with st.status("해설 작성 중...", expanded=True) as status:
                st.write("문헌 및 사료 근거를 분석하고 있습니다.")
                # RAG: use local ollama if available, otherwise a simple fallback summary
                idx, meta, dim = load_faiss_index()
                prompt = (
                    f"당신은 역사 도슨트입니다. 다음 사료와 PG 근거를 바탕으로 '{search_term}'에 대해 "
                    "균형잡힌 해설을 작성하세요.\n"
                    "- 사실/추정/논쟁 지점을 구분해 서술합니다.\n"
                    "- 감정적·과장 표현을 피하고 중립적 톤을 유지합니다.\n"
                    "- 근거가 부족한 부분은 명확히 한계를 밝힙니다.\n\n"
                    f"[사료/PG 근거]\n{context_str}"
                )

                # 저장: 생성에 사용된 입력(프롬프트, Neo4j evidences, PG 텍스트/행, 실행된 쿼리)
                try:
                    st.session_state['last_generation_inputs'] = {
                        'prompt': prompt,
                        'neo4j_evidences': evidences,
                        'pg_texts': pg_texts,
                        'pg_rows': st.session_state.get('pg_prefetch_rows', []),
                        'pg_queries': st.session_state.get('last_queries', []),
                        'neo4j_queries': st.session_state.get('last_neo4j_queries', []),
                    }
                except Exception:
                    pass

                if ollama is not None:
                    try:
                        st.write("로컬 모델(Gemma4:26b)로 해설을 생성하고 있습니다...")
                        t_model0 = time.monotonic()
                        response = ollama.generate(model="gemma4:26b", prompt=prompt)
                        t_model1 = time.monotonic()
                        st.session_state.last_explained_term = search_term
                        st.session_state.last_explanation = response['response']
                        # log model timing and compose summary timings
                        try:
                            st.session_state['last_model_inference_seconds'] = round(t_model1 - t_model0, 3)
                            st.session_state['last_generation_timing'] = {
                                'model_seconds': st.session_state.get('last_model_inference_seconds'),
                                'pg_prefetch_seconds': st.session_state.get('last_pg_prefetch_seconds'),
                                'neo4j': st.session_state.get('last_neo4j_timings')
                            }
                        except Exception:
                            pass
                        status.update(label="해설 생성이 완료되었습니다!", state="complete", expanded=False)
                    except Exception as e:
                        st.write(f"Ollama 연결 실패: {e}. 간단 요약으로 대체합니다.")
                        st.session_state.last_explained_term = search_term
                        st.session_state.last_explanation = simple_fallback_summary(search_term, evidences, retrieved=pg_texts)
                        status.update(label="해설 생성 완료 (간단 요약)", state="complete", expanded=False)
                else:
                    st.write("Ollama 모듈이 없습니다. 간단 요약으로 대체합니다.")
                    st.session_state.last_explained_term = search_term
                    st.session_state.last_explanation = simple_fallback_summary(search_term, evidences, retrieved=pg_texts)
                    status.update(label="해설 생성 완료 (간단 요약)", state="complete", expanded=False)

        if st.session_state.last_explained_term == search_term and st.session_state.last_explanation:
            st.info(st.session_state.last_explanation)
        else:
            st.caption("해설 생성을 누르면 현재 근거로 설명을 생성합니다.")
        # show generation inputs (prompt, PG/Neo4j data) for debugging/fallback inspection
        gen_inputs = st.session_state.get('last_generation_inputs')
        if gen_inputs:
            with st.expander('🧾 생성에 사용된 입력(프롬프트·쿼리·원자료)', expanded=False):
                try:
                    st.subheader('Prompt')
                    st.code(gen_inputs.get('prompt', ''), language='text')
                except Exception:
                    pass
                try:
                    st.subheader('Neo4j evidences')
                    st.write(gen_inputs.get('neo4j_evidences', []))
                except Exception:
                    pass
                try:
                    st.subheader('PG texts (build_pg_context 결과)')
                    st.write(gen_inputs.get('pg_texts', []))
                except Exception:
                    pass
                try:
                    st.subheader('PG raw rows (prefetch)')
                    st.write(gen_inputs.get('pg_rows', [])[:20])
                except Exception:
                    pass
                try:
                    st.subheader('실행된 PG 쿼리 (최근)')
                    st.write(gen_inputs.get('pg_queries', [])[-10:])
                except Exception:
                    pass
                try:
                    st.subheader('실행된 Neo4j 쿼리 (최근)')
                    st.write(gen_inputs.get('neo4j_queries', [])[-10:])
                except Exception:
                    pass
    else:
        st.write("사료/PG 근거를 찾을 수 없습니다.")

# Display selected node's Postgres rows (if any)
if 'selected_node' in st.session_state and st.session_state.get('selected_node'):
    st.markdown('---')
    st.subheader(f"🔎 선택된 노드: {st.session_state['selected_node']}")
    rows = st.session_state.get('selected_node_rows', [])
    if not rows:
        st.info('선택된 노드에 해당하는 Postgres 행을 찾지 못했습니다.')
    else:
        for r in rows[:50]:
            source_info = f"{r.get('schema')}.{r.get('table')}" if r.get('schema') else str(r.get('table'))
            with st.expander(f"{source_info} - id_col={r.get('id_col')} - rowid={r['rowid']}"):
                st.markdown(f"**출처 테이블**: {source_info}")
                st.markdown(f"**검색 사용 컬럼**: {', '.join(r.get('match_cols') or [])}")
                for k, v in r['snippets'].items():
                    st.markdown(f"**{k}**: {v}")
                if r.get('tei'):
                    st.markdown('**TEI (요약)**')
                    tei = r['tei'] or ''
                    st.code((tei[:2000] + '...') if len(tei) > 2000 else tei, language='xml')
        # Additionally, show raw rows for the first result's table for quick inspection
        try:
            first_table = rows[0]['table'] if rows else None
            last_raw = st.session_state.get('last_raw_rows', [])
            if first_table and last_raw:
                for item in reversed(last_raw):
                    if item.get('table') == first_table:
                        st.markdown('---')
                        st.subheader(f'🔬 Raw rows for {first_table}')
                        for raw in item.get('rows', [])[:5]:
                            st.json(raw)
                        break
        except Exception:
            pass
    # show executed queries for debugging (if any)
    def _format_query_with_params(q, params):
        try:
            out = str(q)
            for p in (params or []):
                # represent strings with quotes for readability
                rep = repr(p)
                out = out.replace('%s', rep, 1)
            return out
        except Exception:
            return q

    last_qs = st.session_state.get('last_queries', [])
    if last_qs:
        with st.expander('🧪 디버그: 실행된 쿼리/원시 결과', expanded=False):
            st.subheader('🧾 실행된 쿼리 (최근)')
            for q, params in last_qs[-20:]:
                formatted = _format_query_with_params(q, params)
                st.markdown(f"- SQL: ``{formatted.strip()}``")
                st.markdown(f"  - raw SQL: ``{q.strip()}``")
                st.markdown(f"  - params: {params}")
            # show recent Neo4j queries (if any)
            last_neo = st.session_state.get('last_neo4j_queries', [])
            if last_neo:
                st.markdown('---')
                st.subheader('🔗 최근 Neo4j 쿼리')
                for e in last_neo[-20:]:
                    try:
                        st.code(e.get('cypher', '').strip(), language='cypher')
                        st.markdown(f"- params: {e.get('params')}")
                    except Exception:
                        st.write(e)
            # show raw returned rows for last queries (debug)
            last_raw = st.session_state.get('last_raw_rows', [])
            if last_raw:
                st.markdown('---')
                st.subheader('🗂️ 최근 raw rows (테이블별)')
                for item in last_raw[-10:]:
                    st.markdown(f"**{item.get('table')}**")
                    for r in item.get('rows', [])[:5]:
                        st.write(r)
            # Session debug snapshot (show key session_state vars)
            try:
                st.markdown('---')
                st.subheader('🧪 Session debug snapshot')
                sd = {
                    'selected_node': st.session_state.get('selected_node'),
                    'last_clicked': st.session_state.get('last_clicked'),
                    'selected_node_rows_count': len(st.session_state.get('selected_node_rows') or []),
                    'last_raw_rows_tables': [it.get('table') for it in (st.session_state.get('last_raw_rows') or [])],
                    'last_queries_count': len(st.session_state.get('last_queries') or []),
                }
                st.json(sd)
            except Exception:
                pass
# -----------------------------------------------------------------------------
# Bottom debug panel: always-visible recent Neo4j queries
# -----------------------------------------------------------------------------
try:
    recent_neo = st.session_state.get('last_neo4j_queries', [])
    if recent_neo:
        st.markdown('---')
        st.subheader('🔍 최근 Neo4j 쿼리 (하단)')
        # 최근 10개를 내림차순(최신 먼저)으로 표시
        for entry in list(recent_neo[-10:])[::-1]:
            cy = entry.get('cypher', '') or ''
            params = entry.get('params') or {}
            header = cy.splitlines()[0].strip() if cy.strip() else '(empty)'
            with st.expander(header, expanded=False):
                st.code(cy, language='cypher')
                try:
                    st.write('params:')
                    st.json(params)
                except Exception:
                    st.write(params)
except Exception:
    pass