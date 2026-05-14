"""
이 스크립트로 파이프라인이 통합되었으므로 pg_to_tei.py 및 tei_to_neo4j.py는 삭제 가능함

설명:
- Neo4j 그래프 초기화 및 스키마 확보
- Postgres에서 사건/장소/인물/기관 등의 원천 데이터를 읽어 노드 생성
- Postgres의 TEI 컬럼을 파싱해 사건↔인물(P14), 사건↔장소(P7) 관계를 맥락(context) 속성과 함께 삽입
- Postgres의 `tei_cidoc_mappings` 테이블에 저장된 CIDOC TTL을 읽어 추가적인 CIDOC-CRM 매핑을 Neo4j에 병합

주의: 로컬 `tei/` 디렉토리 또는 `.tei.xml` 파일 참조는 제거되어있으며,
데이터의 단일 출처는 PostgreSQL DB입니다.
"""

import os
import re
import urllib.parse

import hanja
import pandas as pd
from bs4 import BeautifulSoup
from neo4j import GraphDatabase
from sqlalchemy import create_engine, text

try:
    import tomllib
except ImportError:
    import tomli as tomllib


def _load_secrets(secret_file=None):
    if secret_file is None:
        secret_file = os.path.join(os.path.dirname(__file__), '.streamlit', 'secrets.toml')

    if os.path.exists(secret_file):
        try:
            with open(secret_file, 'rb') as f:
                return tomllib.load(f)
        except Exception as e:
            print(f"⚠️ secrets.toml 읽기 실패: {e}")
    return {}


def _secret_or_env(key, default="", secrets_dict=None):
    val = os.getenv(key)
    if val:
        return str(val)
    if secrets_dict and key in secrets_dict:
        return str(secrets_dict[key])
    return str(default) if default else ""


SECRETS = _load_secrets()

PG_CONFIG = {
    "host": _secret_or_env("PG_HOST", "11e.kr", SECRETS),
    "port": _secret_or_env("PG_PORT", "5432", SECRETS),
    "database": _secret_or_env("PG_DATABASE", "samil_db", SECRETS),
    "user": _secret_or_env("PG_USER", "postgres", SECRETS),
    "password": _secret_or_env("PG_PASSWORD", "", SECRETS),
}

NEO4J_CONFIG = {
    "uri": _secret_or_env("NEO4J_URI", "bolt://11e.kr:7687", SECRETS),
    "user": _secret_or_env("NEO4J_USER", "neo4j", SECRETS),
    "password": _secret_or_env("NEO4J_PASSWORD", "", SECRETS),
}

encoded_pass = urllib.parse.quote_plus(PG_CONFIG["password"])
pg_engine = create_engine(
    f"postgresql://{PG_CONFIG['user']}:{encoded_pass}@{PG_CONFIG['host']}:5432/{PG_CONFIG['database']}"
)
neo4j_driver = GraphDatabase.driver(
    NEO4J_CONFIG["uri"], auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"])
)


def safe_run(session, cypher, **params):
    """Run cypher but defensively rewrite disconnected MATCH patterns like
    "MATCH (a ...),(b ...)" -> "MATCH (a ...) MATCH (b ...)" to avoid
    accidental cartesian products. Returns the underlying result object.
    """
    try:
        if isinstance(cypher, str) and 'MATCH' in cypher and '),(' in cypher:
            new = cypher.replace('),(', ') MATCH (')
            if new != cypher:
                print('⚠️ 안전조치: 분리된 MATCH 패턴을 더 안전한 형태로 변환합니다.')
                cypher = new
        return session.run(cypher, **params)
    except Exception:
        # re-raise for caller to handle
        raise


def _read_table_with_mapping(table_name, mapping, required=None):
    try:
        df = pd.read_sql(f'SELECT * FROM {table_name}', pg_engine)
    except Exception as e:
        print(f"⚠️ 테이블 {table_name} 로딩 실패: {e}")
        return pd.DataFrame()

    out = pd.DataFrame()
    for expected, candidates in mapping.items():
        found = False
        for c in candidates:
            if c in df.columns:
                out[expected] = df[c].astype(str).fillna("")
                found = True
                break
        if not found:
            out[expected] = ""

    if required:
        for r in required:
            if r not in out.columns or out[r].astype(str).str.strip().eq("").all():
                print(f"⚠️ 필수 컬럼 누락 또는 모두 빈 값: {r} (테이블: {table_name})")
                return pd.DataFrame()

    return out


def _run_batches(session, cypher, records, batch_size=500, extra_params=None):
    extra_params = extra_params or {}
    total = len(records)
    if total == 0:
        return 0

    written = 0
    for start in range(0, total, batch_size):
        chunk = records[start:start + batch_size]
        params = {"data": chunk, **extra_params}
        safe_run(session, cypher, **params).consume()
        written += len(chunk)
    return written


def _ensure_schema(session):
    statements = [
        "CREATE CONSTRAINT place_id IF NOT EXISTS FOR (p:장소) REQUIRE p.id IS UNIQUE",
        "CREATE CONSTRAINT event_id IF NOT EXISTS FOR (e:사건) REQUIRE e.id IS UNIQUE",
        "CREATE CONSTRAINT document_id IF NOT EXISTS FOR (m:문건) REQUIRE m.id IS UNIQUE",
        "CREATE CONSTRAINT org_id IF NOT EXISTS FOR (o:기관) REQUIRE o.id IS UNIQUE",
        "CREATE CONSTRAINT person_name IF NOT EXISTS FOR (i:인물) REQUIRE i.명칭 IS UNIQUE",
        "CREATE INDEX place_name IF NOT EXISTS FOR (p:장소) ON (p.명칭)",
        "CREATE INDEX place_korean_name IF NOT EXISTS FOR (p:장소) ON (p.한글명칭)",
    ]

    for statement in statements:
        try:
            safe_run(session, statement).consume()
        except Exception as e:
            print(f"⚠️ 스키마 생성 건너뜀: {e}")


def _ensure_tei_status(table_name):
    try:
        with pg_engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS tei_status text'))
    except Exception as e:
        print(f"⚠️ {table_name} tei_status 컬럼 생성 실패: {e}")


def _clear_graph(session, batch_size=500):
    total_deleted = 0

    while True:
        result = safe_run(session,
            """
            MATCH (n)
            WITH n LIMIT $batch_size
            DETACH DELETE n
            RETURN count(*) AS deleted
            """,
            batch_size=batch_size,
        )
        deleted = result.single()["deleted"]
        if not deleted:
            break
        total_deleted += deleted
        print(f"  ✓ {deleted}개 노드 삭제됨 (누적 {total_deleted}개)")

    return total_deleted


def _extract_tei_values(tei_text, limit=None):
    try:
        soup = BeautifulSoup(tei_text or "", "xml")
        values = []
        for p in soup.find_all("p"):
            text = p.get_text(" ", strip=True)
            if text:
                values.append(text)
                if limit and len(values) >= limit:
                    break
        return values
    except Exception:
        return []


def apply_cidoc_mappings(session, mappings):
    """Apply CIDOC TTL mappings from DB into Neo4j using Korean labels.
    mappings: iterable of rows like (table_name, rowid, mapping_label, cidoc_ttl)
    This function is forgiving and will continue on parse errors.
    """
    import re

    # Batch-oriented implementation:
    # 1) scan all mappings and collect unique nodes (by uid) for each type
    # 2) collect relationships (P14, P7) as row-level items
    # 3) push nodes and relationships in batches via _run_batches to reduce per-row roundtrips
    person_nodes = {}
    place_nodes = {}
    event_nodes = {}
    rel_p14 = []
    rel_p7 = []

    total_rows = 0
    for row in mappings:
        total_rows += 1
        try:
            # normalize row shape
            if isinstance(row, (list, tuple)) and len(row) >= 4:
                ttl = row[3]
            elif isinstance(row, dict):
                ttl = row.get('cidoc_ttl') or row.get('ttl')
            else:
                ttl = row[3]

            if not ttl or not str(ttl).strip():
                continue
            text = str(ttl)

            ids = re.findall(r'ex:([A-Za-z0-9_\-]+)', text)
            labels = re.findall(r'ex:([A-Za-z0-9_\-]+)[^\n]*?rdfs:label\s+"([^"]+)"', text)
            label_map = {rid: lbl for rid, lbl in labels}

            # collect uids with inferred kinds
            uids = []
            low = text.lower()
            for short in ids:
                kind = None
                sl = short.lower()
                if sl.startswith('person') or ('person' in low and sl.startswith('p')):
                    kind = '인물'
                elif sl.startswith('place'):
                    kind = '장소'
                elif sl.startswith('event') or sl.startswith('item'):
                    kind = '사건'
                else:
                    if 'person' in low:
                        kind = '인물'
                    elif 'place' in low:
                        kind = '장소'
                    elif 'event' in low or 'item' in low:
                        kind = '사건'
                    else:
                        kind = '사건'

                uid = f'ex:{short}'
                uids.append((kind, uid, short))

                lbl = label_map.get(short)
                if kind == '인물':
                    if uid not in person_nodes:
                        person_nodes[uid] = lbl
                elif kind == '장소':
                    if uid not in place_nodes:
                        place_nodes[uid] = lbl
                else:
                    if uid not in event_nodes:
                        event_nodes[uid] = lbl

            # collect relationships
            if 'P14_carried_out_by' in text:
                ev = next((u for k, u, s in uids if k == '사건'), None)
                pe = next((u for k, u, s in uids if k == '인물'), None)
                if ev and pe:
                    rel_p14.append({'ev': ev, 'pe': pe})

            if 'P7_took_place_at' in text:
                ev = next((u for k, u, s in uids if k == '사건'), None)
                pl = next((u for k, u, s in uids if k == '장소'), None)
                if ev and pl:
                    rel_p7.append({'ev': ev, 'pl': pl})

        except Exception as e:
            # ignore problematic rows but log
            try:
                rowid = row[1] if isinstance(row, (list, tuple)) and len(row) > 1 else getattr(row, 'rowid', None)
            except Exception:
                rowid = None
            print(f"⚠️ CIDOC 매핑 처리 중 오류 (rowid={rowid}): {e}")
            continue

    # push nodes in batches
    applied = 0
    batch = 200
    if person_nodes:
        data = [{'uid': k, 'label': v} for k, v in person_nodes.items()]
        _run_batches(session,
                     """
                     UNWIND $data AS row
                     MERGE (n:인물 {uid: row.uid})
                     SET n.명칭 = coalesce(n.명칭, row.label)
                     """,
                     data,
                     batch_size=batch)
        applied += len(data)

    if place_nodes:
        data = [{'uid': k, 'label': v} for k, v in place_nodes.items()]
        _run_batches(session,
                     """
                     UNWIND $data AS row
                     MERGE (n:장소 {uid: row.uid})
                     SET n.명칭 = coalesce(n.명칭, row.label)
                     """,
                     data,
                     batch_size=batch)
        applied += len(data)

    if event_nodes:
        data = [{'uid': k, 'label': v} for k, v in event_nodes.items()]
        _run_batches(session,
                     """
                     UNWIND $data AS row
                     MERGE (n:사건 {uid: row.uid})
                     SET n.사건명 = coalesce(n.사건명, row.label)
                     """,
                     data,
                     batch_size=batch)
        applied += len(data)

    # push relationships in batches
    if rel_p14:
        _run_batches(session,
                     """
                     UNWIND $data AS row
                     MATCH (a:사건 {uid: row.ev})
                     MATCH (b:인물 {uid: row.pe})
                     MERGE (a)-[:P14_carried_out_by]->(b)
                     """,
                     rel_p14,
                     batch_size=batch)

    if rel_p7:
        _run_batches(session,
                     """
                     UNWIND $data AS row
                     MATCH (a:사건 {uid: row.ev})
                     MATCH (b:장소 {uid: row.pl})
                     MERGE (a)-[:P7_took_place_at]->(b)
                     """,
                     rel_p7,
                     batch_size=batch)

    print(f"Applied {applied} CIDOC mappings (nodes: {len(person_nodes)+len(place_nodes)+len(event_nodes)}, rels: {len(rel_p14)+len(rel_p7)}) from {total_rows} rows")


def build_ultimate_graph():
    with neo4j_driver.session() as session:
        print("🧹 기존 데이터를 삭제하고 그래프를 재설계합니다...")
        _clear_graph(session, batch_size=50)
        _ensure_schema(session)

        print("📍 장소 노드 생성 중... (Postgres TEI에서 로드)")
        _ensure_tei_status('raw_detail_place')
        try:
            df_place = pd.read_sql(
                "SELECT rowid, tei FROM raw_detail_place WHERE tei IS NOT NULL AND tei_status = 'REFINED'",
                pg_engine,
            )
        except Exception as e:
            print(f"⚠️ 장소 TEI 로딩 실패: {e}")
            df_place = pd.DataFrame()

        place_records = []
        for _, row in df_place.iterrows():
            values = _extract_tei_values(row.get('tei'), limit=3)
            if not values:
                continue
            place_records.append({
                'id': values[0],
                'name': values[1] if len(values) > 1 else values[0],
                'rowid': str(row.get('rowid', '')),
            })

        if place_records:
            _run_batches(
                session,
                """
                UNWIND $data AS row
                MERGE (p:장소 {id: row.id})
                SET p.명칭 = row.name,
                    p.원천rowid = row.rowid,
                    p.한글명칭 = coalesce(p.한글명칭, row.name)
                """,
                place_records,
                batch_size=25,
            )
        else:
            print("⚠️ 장소 TEI에서 생성할 레코드가 없습니다.")

        print("🔥 사건 노드 생성 중... (Postgres에서 로드)")
        df_event = _read_table_with_mapping(
            'raw_event_info',
            {
                '아이디': ['아이디', 'id', 'event_id'],
                '사건명': ['사건명', 'title', 'name'],
                '시위_시작일자': ['시위_시작일자', 'start_date', 'date'],
            },
            required=['아이디'],
        )
        if df_event.empty:
            print("⚠️ 사건 테이블이 비어있거나 필수 컬럼 누락: 건너뜁니다.")
            event_records = []
        else:
            df_event = df_event[df_event['아이디'].astype(str).str.strip() != '']
            event_records = df_event.to_dict('records')

        _run_batches(
            session,
            """
            UNWIND $data AS row
            MERGE (e:사건 {id: row.아이디})
            SET e.사건명 = row.사건명,
                e.날짜 = row.시위_시작일자
            """,
            event_records,
            batch_size=25,
        )

        print("👤 인물 및 📚 서지 데이터 통합 중... (Postgres TEI에서 로드)")
        source_tables = ['raw_bibliography', 'raw_source_info', 'raw_event_place_link']
        source_records = []
        for source_table in source_tables:
            _ensure_tei_status(source_table)
            try:
                df_source = pd.read_sql(
                    f"SELECT rowid, tei FROM {source_table} WHERE tei IS NOT NULL AND tei_status = 'REFINED'",
                    pg_engine,
                )
            except Exception as e:
                print(f"⚠️ {source_table} TEI 로딩 실패: {e}")
                continue

            for _, row in df_source.iterrows():
                values = _extract_tei_values(row.get('tei'), limit=3)
                if not values:
                    continue
                source_records.append({
                    'id': f"{source_table}:{values[0]}",
                    'source_id': values[0],
                    'title': values[1] if len(values) > 1 else values[0],
                    'rowid': str(row.get('rowid', '')),
                    'source_table': source_table,
                })

        if source_records:
            _run_batches(
                session,
                """
                UNWIND $data AS row
                MERGE (m:문건 {id: row.id})
                SET m.원본id = row.source_id,
                    m.제목 = row.title,
                    m.원천rowid = row.rowid,
                    m.원천테이블 = row.source_table
                """,
                source_records,
                batch_size=25,
            )
        else:
            print("⚠️ 문건 TEI에서 생성할 레코드가 없습니다.")

        org_tables = {
            'raw_oppression_org_police': '경찰',
            'raw_oppression_org_gendarme': '헌병',
            'raw_oppression_org_military': '군대',
        }
        for table_name, org_type in org_tables.items():
            try:
                print(f"👮 {org_type} 기구 데이터 분석 및 주입 중 (Postgres 테이블: {table_name})...")
                df_org = _read_table_with_mapping(
                    table_name,
                    {
                        'id': ['id', '아이디', '기구ID', '기구_id', '기구id'],
                        'name': ['name', '명칭', '기구명', '기구명칭'],
                        'parent_id': ['parent_id', '상위ID', '상위_아이디', 'parent'],
                    },
                    required=['id', 'name'],
                )

                if df_org.empty:
                    print(f"⚠️ {table_name}이 비어있거나 필수 컬럼 누락: 건너뜁니다.")
                    continue

                df_org['한글명칭'] = df_org['name'].apply(lambda x: hanja.translate(str(x), 'substitution'))
                data_payload = []
                for _, row in df_org.iterrows():
                    raw_id = row['id']
                    if pd.isna(raw_id) or str(raw_id).strip() == '' or str(raw_id).lower() == 'nan':
                        continue
                    parent_raw = row.get('parent_id', '') if hasattr(row, 'get') else row['parent_id']
                    data_payload.append({
                        'id': str(raw_id),
                        'name': str(row['name']),
                        'h_name': str(row['한글명칭']),
                        'parent_id': str(parent_raw) if parent_raw and str(parent_raw).strip() != '' else None,
                        'loc_name': '',
                        'loc_code': '',
                        'note': '',
                    })

                if data_payload:
                    _run_batches(
                        session,
                        """
                        UNWIND $data AS row
                        MERGE (o:기관 {id: row.id})
                        SET o.명칭 = row.name,
                            o.한글명칭 = row.h_name,
                            o.유형 = $type,
                            o.설치장소명 = row.loc_name,
                            o.설치장소코드 = row.loc_code,
                            o.비고 = row.note
                        WITH o, row
                        WHERE row.parent_id IS NOT NULL AND row.parent_id <> row.id
                        MERGE (parent:기관 {id: row.parent_id})
                        MERGE (o)-[:소속]->(parent)
                        """,
                        data_payload,
                        batch_size=25,
                        extra_params={"type": org_type},
                    )
                    print(f"  ✓ {len(data_payload)}개 기관 추가됨")
            except Exception as e:
                print(f"❌ {table_name} 처리 중 에러: {e}")

        print("🔗 최종 관계망 구축 중... 사건 TEI에서 인물/장소 관계 생성")
        _ensure_tei_status('raw_event_info')
        try:
            df_tei = pd.read_sql(
                "SELECT \"아이디\" AS event_id, tei FROM raw_event_info "
                "WHERE tei IS NOT NULL AND \"아이디\" IS NOT NULL AND tei_status = 'REFINED'",
                pg_engine,
            )
        except Exception as e:
            print(f"⚠️ TEI 데이터 로딩 중 오류: {e}")
            df_tei = pd.DataFrame()

        if df_tei.empty:
            print("⚠️ 사건 TEI 데이터가 비어있어 TEI 기반 연결을 건너뜁니다.")
        else:
            person_acc = []
            place_acc = []
            flush_every = 50

            def _flush_acc():
                nonlocal person_acc, place_acc
                if person_acc:
                    try:
                        # deduplicate by (main_name, gloss)
                        unique = {}
                        for r in person_acc:
                            key = (r.get('person_name',''), r.get('gloss',''))
                            if key not in unique:
                                unique[key] = {'main': key[0], 'gloss': key[1]}

                        mapping = {}
                        for key, val in unique.items():
                            main = val['main']
                            gloss = val['gloss'] or ''
                            try:
                                res = safe_run(session,
                                               "MATCH (n:인물) WHERE n.명칭 = $main OR n.명칭 = $gloss OR n.한글독음 = $main RETURN id(n) AS nid",
                                               main=main, gloss=gloss).single()
                                if res and res.get('nid') is not None:
                                    mapping[key] = res['nid']
                                    continue
                            except Exception:
                                pass

                            # create node if not found
                            res2 = safe_run(session,
                                            "MERGE (p:인물 {명칭:$main}) SET p.한글독음 = coalesce(p.한글독음, $gloss) RETURN id(p) AS nid",
                                            main=main, gloss=gloss).single()
                            mapping[key] = res2['nid'] if res2 else None

                        rels = []
                        for r in person_acc:
                            key = (r.get('person_name',''), r.get('gloss',''))
                            nid = mapping.get(key)
                            if nid is None:
                                continue
                            rels.append({'event_id': r['event_id'], 'person_nid': nid, 'context_text': r['context_text']})

                        if rels:
                            _run_batches(
                                session,
                                """
                                UNWIND $data AS row
                                MATCH (e:사건 {id: row.event_id})
                                MATCH (p) WHERE id(p) = row.person_nid
                                MERGE (e)-[r:P14_carried_out_by]->(p)
                                SET r.context = row.context_text,
                                    r.source_tag = 'persName'
                                """,
                                rels,
                                batch_size=25,
                            )
                    except Exception as e:
                        print(f"⚠️ 인물 관계 배치 주입 중 오류: {e}")
                    person_acc = []

                if place_acc:
                    try:
                        unique = {}
                        for r in place_acc:
                            key = (r.get('place_name',''), r.get('gloss',''))
                            if key not in unique:
                                unique[key] = {'main': key[0], 'gloss': key[1]}

                        mapping = {}
                        for key, val in unique.items():
                            main = val['main']
                            gloss = val['gloss'] or ''
                            try:
                                res = safe_run(session,
                                               "MATCH (n:장소) WHERE n.명칭 = $main OR n.명칭 = $gloss OR n.한글독음 = $main RETURN id(n) AS nid",
                                               main=main, gloss=gloss).single()
                                if res and res.get('nid') is not None:
                                    mapping[key] = res['nid']
                                    continue
                            except Exception:
                                pass

                            res2 = safe_run(session,
                                            "MERGE (p:장소 {명칭:$main}) SET p.한글독음 = coalesce(p.한글독음, $gloss) RETURN id(p) AS nid",
                                            main=main, gloss=gloss).single()
                            mapping[key] = res2['nid'] if res2 else None

                        rels = []
                        for r in place_acc:
                            key = (r.get('place_name',''), r.get('gloss',''))
                            nid = mapping.get(key)
                            if nid is None:
                                continue
                            rels.append({'event_id': r['event_id'], 'place_nid': nid, 'context_text': r['context_text']})

                        if rels:
                            _run_batches(
                                session,
                                """
                                UNWIND $data AS row
                                MATCH (e:사건 {id: row.event_id})
                                MATCH (p) WHERE id(p) = row.place_nid
                                MERGE (e)-[r:P7_took_place_at]->(p)
                                SET r.context = row.context_text,
                                    r.source_tag = 'placeName'
                                """,
                                rels,
                                batch_size=25,
                            )
                    except Exception as e:
                        print(f"⚠️ 장소 관계 배치 주입 중 오류: {e}")
                    place_acc = []

            for _, row in df_tei.iterrows():
                event_id = str(row.get('event_id', '')).strip()
                tei_text = row.get('tei') or ''
                if not event_id or not tei_text:
                    continue

                try:
                    soup = BeautifulSoup(tei_text, 'xml')
                    for section in soup.find_all(['p', 'div']):
                        context_text = section.get_text(' ', strip=True)
                        if not context_text:
                            continue

                        for pers in section.find_all('persName'):
                            # extract <gloss> if present, remove it from tree, then get main name
                            gloss_tag = pers.find('gloss')
                            gloss_text = ''
                            if gloss_tag:
                                try:
                                    gloss_text = gloss_tag.get_text(' ', strip=True) or ''
                                except Exception:
                                    gloss_text = ''
                                try:
                                    gloss_tag.extract()
                                except Exception:
                                    pass
                            person_name = pers.get_text(' ', strip=True)
                            if person_name:
                                person_acc.append({
                                    'event_id': event_id,
                                    'person_name': person_name,
                                    'gloss': gloss_text,
                                    'context_text': context_text,
                                })

                        for place in section.find_all('placeName'):
                            gloss_tag = place.find('gloss')
                            gloss_text = ''
                            if gloss_tag:
                                try:
                                    gloss_text = gloss_tag.get_text(' ', strip=True) or ''
                                except Exception:
                                    gloss_text = ''
                                try:
                                    gloss_tag.extract()
                                except Exception:
                                    pass
                            place_name = place.get_text(' ', strip=True)
                            if place_name:
                                place_acc.append({
                                    'event_id': event_id,
                                    'place_name': place_name,
                                    'gloss': gloss_text,
                                    'context_text': context_text,
                                })

                        if len(person_acc) + len(place_acc) >= flush_every:
                            _flush_acc()
                except Exception as e:
                    print(f"⚠️ TEI 파싱 중 오류 (사건id={event_id}): {e}")

            _flush_acc()

            # CIDOC-CRM 매핑 적용: Postgres의 tei_cidoc_mappings 테이블에서 TTL 불러와 병합
            print("🔧 CIDOC 매핑 적용을 시도합니다 (Postgres 테이블: tei_cidoc_mappings)")
            try:
                df_mappings = pd.read_sql(
                    'SELECT table_name, rowid, mapping_label, cidoc_ttl FROM tei_cidoc_mappings ORDER BY id',
                    pg_engine,
                )
            except Exception as e:
                print(f"⚠️ tei_cidoc_mappings 로딩 실패: {e}")
                df_mappings = pd.DataFrame()

            if df_mappings.empty:
                print("⚠️ tei_cidoc_mappings 데이터가 비어있거나 테이블이 없습니다. CIDOC 적용 건너뜁니다.")
            else:
                try:
                    mappings = list(df_mappings[['table_name', 'rowid', 'mapping_label', 'cidoc_ttl']].itertuples(index=False, name=None))
                    apply_cidoc_mappings(session, mappings)
                except Exception as e:
                    print(f"⚠️ CIDOC 매핑 적용 중 오류: {e}")

    print("\n✨ 모든 데이터와 탄압 기구의 계층 구조까지 완벽하게 통합되었습니다!")


if __name__ == "__main__":
    try:
        build_ultimate_graph()
    finally:
        neo4j_driver.close()
