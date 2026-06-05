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
from neo4j.exceptions import ClientError
from sqlalchemy import create_engine, text
import time

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

RELATION_MAPPING = {
    "부친": "P152_has_parent",
    "모친": "P152_has_parent",
    "딸": "P152_has_parent",
    "아들": "P152_has_parent",
    "자녀": "P152_has_parent",
    "동지": "foaf:knows",
    "소속": "foaf:member"
}


def _split_names(val):
    """Split names in a cell by common delimiters."""
    if not val or pd.isna(val):
        return []
    import re
    # Split by ;, / | · and whitespace
    parts = re.split(r'[;,/|·\s]+', str(val).strip())
    return [p for p in parts if p and p.lower() != 'none' and p.lower() != 'nan']

PG_CONFIG = {
    "host": _secret_or_env("PG_HOST", "11e.kr", SECRETS),
    "port": _secret_or_env("PG_PORT", "5432", SECRETS),
    "database": _secret_or_env("PG_DATABASE", "historical", SECRETS),
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


def _run_batches(session, cypher, records, batch_size=20000, extra_params=None):
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
    import re
    statements = [
        "CREATE CONSTRAINT place_id IF NOT EXISTS FOR (p:장소) REQUIRE p.id IS UNIQUE",
        "CREATE CONSTRAINT event_id IF NOT EXISTS FOR (e:사건) REQUIRE e.id IS UNIQUE",
        "CREATE CONSTRAINT document_id IF NOT EXISTS FOR (m:문건) REQUIRE m.id IS UNIQUE",
        "CREATE CONSTRAINT org_id IF NOT EXISTS FOR (o:기관) REQUIRE o.id IS UNIQUE",
        "CREATE CONSTRAINT person_name IF NOT EXISTS FOR (i:인물) REQUIRE i.명칭 IS UNIQUE",
        "CREATE CONSTRAINT person_uid IF NOT EXISTS FOR (i:인물) REQUIRE i.uid IS UNIQUE",
        "CREATE CONSTRAINT place_uid IF NOT EXISTS FOR (p:장소) REQUIRE p.uid IS UNIQUE",
        "CREATE CONSTRAINT event_uid IF NOT EXISTS FOR (e:사건) REQUIRE e.uid IS UNIQUE",
        "CREATE INDEX place_name IF NOT EXISTS FOR (p:장소) ON (p.명칭)",
        "CREATE INDEX place_korean_name IF NOT EXISTS FOR (p:장소) ON (p.한글명칭)",
        "DROP INDEX namesIndex IF EXISTS",
        "CREATE FULLTEXT INDEX namesIndex IF NOT EXISTS FOR (n:인물|장소|사건|기관|문건) ON EACH [n.명칭, n.사건명, n.제목, n.name, n.title, n.한글독음, n.한글명칭]",
    ]

    for statement in statements:
        try:
            safe_run(session, statement).consume()
        except ClientError as e:
            # Neo.ClientError.Schema.IndexWithNameAlreadyExists or ConstraintWithNameAlreadyExists
            if "AlreadyExists" in e.code:
                # Extract name using regex
                match = re.search(r'(?:CONSTRAINT|INDEX)\s+([^\s]+)', statement, re.IGNORECASE)
                if match:
                    name = match.group(1)
                    print(f"⚠️ 스키마 이름 충돌 감지 ('{name}'): 삭제 후 다시 생성합니다.")
                    try:
                        if "CONSTRAINT" in statement.upper():
                            session.run(f"DROP CONSTRAINT {name} IF EXISTS").consume()
                        else:
                            session.run(f"DROP INDEX {name} IF EXISTS").consume()
                        # Retry original statement
                        safe_run(session, statement).consume()
                    except Exception as retry_err:
                        print(f"❌ 스키마 재생성 실패: {retry_err}")
                else:
                    print(f"⚠️ 스키마 생성 실패 (이름 추출 불가): {e}")
            else:
                print(f"⚠️ 스키마 생성 건너뜀: {e}")
        except Exception as e:
            print(f"⚠️ 스키마 생성 건너뜀: {e}")


def _ensure_tei_status(table_name):
    try:
        with pg_engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS tei_status text'))
    except Exception as e:
        print(f"⚠️ {table_name} tei_status 컬럼 생성 실패: {e}")


def _clear_graph(session, batch_size=50000):
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

    id_pattern = re.compile(r'ex:([A-Za-z0-9_\-]+)')
    label_pattern = re.compile(r'ex:([A-Za-z0-9_\-]+)[^\n]*?rdfs:label\s+"([^"]+)"')

    person_nodes = {}
    place_nodes = {}
    event_nodes = {}
    rel_p14 = []
    rel_p7 = []

    total_rows = 0
    for row in mappings:
        total_rows += 1
        try:
            if isinstance(row, (list, tuple)) and len(row) >= 4:
                ttl = row[3]
            elif isinstance(row, dict):
                ttl = row.get('cidoc_ttl') or row.get('ttl')
            else:
                ttl = row[3]

            if not ttl:
                continue
            text = str(ttl)

            ids = id_pattern.findall(text)
            if not ids:
                continue

            labels = label_pattern.findall(text)
            label_map = {rid: lbl for rid, lbl in labels}

            uids = []
            low = text.lower()
            for short in ids:
                sl = short.lower()
                kind = '사건' # default
                if sl.startswith('person') or ('person' in low and sl.startswith('p')):
                    kind = '인물'
                elif sl.startswith('place'):
                    kind = '장소'
                elif sl.startswith('event') or sl.startswith('item'):
                    kind = '사건'
                elif 'person' in low:
                    kind = '인물'
                elif 'place' in low:
                    kind = '장소'

                uid = f'ex:{short}'
                uids.append((kind, uid))

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

            if 'P14_carried_out_by' in text:
                ev = next((u for k, u in uids if k == '사건'), None)
                pe = next((u for k, u in uids if k == '인물'), None)
                if ev and pe:
                    rel_p14.append({'ev': ev, 'pe': pe})

            if 'P7_took_place_at' in text:
                ev = next((u for k, u in uids if k == '사건'), None)
                pl = next((u for k, u in uids if k == '장소'), None)
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
    batch = 5000
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
        _clear_graph(session, batch_size=50000)
        _ensure_schema(session)

        print("📍 장소 노드 생성 중... (Postgres TEI에서 로드)")
        _ensure_tei_status('raw_detail_place')
        try:
            df_place = pd.read_sql(
                "SELECT rowid, tei FROM raw_detail_place WHERE tei IS NOT NULL",
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
                batch_size=5000,
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
            batch_size=5000,
        )

        print("👤 인물 및 📚 서지 데이터 통합 중... (Postgres TEI에서 로드)")
        source_tables = ['raw_bibliography', 'raw_source_info', 'raw_event_place_link']
        source_records = []
        for source_table in source_tables:
            _ensure_tei_status(source_table)
            try:
                df_source = pd.read_sql(
                    f"SELECT rowid, tei FROM {source_table} WHERE tei IS NOT NULL",
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
                batch_size=5000,
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
                        batch_size=5000,
                        extra_params={"type": org_type},
                    )
                    print(f"  ✓ {len(data_payload)}개 기관 추가됨")
            except Exception as e:
                print(f"❌ {table_name} 처리 중 에러: {e}")

        print("🔗 최종 관계망 구축 중... 사건 TEI에서 인물/장소 관계 생성")
        _ensure_tei_status('raw_event_info')
        _ensure_tei_status('raw_source_info')
        try:
            df_event_tei = pd.read_sql(
                'SELECT "아이디" AS event_id, tei FROM raw_event_info WHERE tei IS NOT NULL',
                pg_engine,
            )
            df_source_tei = pd.read_sql(
                'SELECT "사건아이디" AS event_id, tei FROM raw_source_info WHERE tei IS NOT NULL AND "사건아이디" IS NOT NULL',
                pg_engine,
            )
            df_tei = pd.concat([df_event_tei, df_source_tei], ignore_index=True)
        except Exception as e:
            print(f"⚠️ TEI 데이터 로딩 중 오류: {e}")
            df_tei = pd.DataFrame()

        if df_tei.empty:
            print("⚠️ 사건 TEI 데이터가 비어있어 TEI 기반 연결을 건너뜁니다.")
        else:
            person_acc = []
            place_acc = []
            flush_every = 10000

            def _flush_acc():
                nonlocal person_acc, place_acc
                if person_acc:
                    try:
                        _run_batches(
                            session,
                            """
                            UNWIND $data AS row
                            MATCH (e:사건 {id: row.event_id})
                            MERGE (p:인물 {명칭: row.person_name})
                            ON CREATE SET p.한글독음 = row.gloss
                            ON MATCH SET p.한글독음 = coalesce(p.한글독음, row.gloss)
                            MERGE (e)-[r:P14_carried_out_by]->(p)
                            SET r.context = row.context_text,
                                r.source_tag = 'persName'
                            """,
                            person_acc,
                            batch_size=2000,
                        )
                    except Exception as e:
                        print(f"⚠️ 인물 관계 배치 주입 중 오류: {e}")
                    person_acc = []

                if place_acc:
                    try:
                        _run_batches(
                            session,
                            """
                            UNWIND $data AS row
                            MATCH (e:사건 {id: row.event_id})
                            MERGE (p:장소 {명칭: row.place_name})
                            ON CREATE SET p.한글독음 = row.gloss
                            ON MATCH SET p.한글독음 = coalesce(p.한글독음, row.gloss)
                            MERGE (e)-[r:P7_took_place_at]->(p)
                            SET r.context = row.context_text,
                                r.source_tag = 'placeName'
                            """,
                            place_acc,
                            batch_size=2000,
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

            print("👨‍👩‍👧‍👦 인물 간 관계망 분석 및 주입 중 (원천 컬럼 기반)...")
            # 분석 대상 테이블들
            relation_source_tables = ['raw_bibliography', 'raw_event_info', 'raw_source_info']
            MAIN_PERSON_KWS = ['피고인', '성명', '이름', '인물', '명칭']

            for table_name in relation_source_tables:
                try:
                    df_rel = pd.read_sql(f'SELECT * FROM "{table_name}"', pg_engine)
                    cols = df_rel.columns.tolist()

                    # 주체(Main Person) 컬럼 식별
                    subject_cols = [c for c in cols if any(kw in c for kw in MAIN_PERSON_KWS) and c not in RELATION_MAPPING]
                    # 관계(Relation) 컬럼 식별
                    rel_cols = [c for c in cols if any(rk in c for rk in RELATION_MAPPING.keys())]

                    if not subject_cols or not rel_cols:
                        continue

                    print(f"  ✓ {table_name}: 주체 컬럼({len(subject_cols)}), 관계 컬럼({len(rel_cols)}) 발견")

                    rel_p_p_acc = [] # (src_name, rel_type, dst_name)

                    for _, row in df_rel.iterrows():
                        subjects = []
                        for sc in subject_cols:
                            subjects.extend(_split_names(row.get(sc)))
                        
                        if not subjects:
                            continue

                        for rc in rel_cols:
                            # 매핑되는 관계 키 찾기
                            matched_key = next((k for k in RELATION_MAPPING.keys() if k in rc), None)
                            if not matched_key:
                                continue
                            
                            rel_type = RELATION_MAPPING[matched_key]
                            related_names = _split_names(row.get(rc))
                            
                            for p_related in related_names:
                                for p_subject in subjects:
                                    if p_related == p_subject:
                                        continue
                                    
                                    # 방향성 및 타입 결정
                                    if matched_key in ("딸", "아들", "자녀"):
                                        # 자녀 -> 부모 (Subject가 부모)
                                        rel_p_p_acc.append({'src': p_related, 'rel': rel_type, 'dst': p_subject})
                                    elif matched_key in ("부친", "모친"):
                                        # 자녀 -> 부모 (Subject가 자녀)
                                        rel_p_p_acc.append({'src': p_subject, 'rel': rel_type, 'dst': p_related})
                                    else:
                                        # 기본: 주체 -> 관계인물
                                        rel_p_p_acc.append({'src': p_subject, 'rel': rel_type, 'dst': p_related})

                    if rel_p_p_acc:
                        # 관계 타입별로 묶어서 실행
                        rel_types_acc = set(r['rel'] for r in rel_p_p_acc)
                        for rt in rel_types_acc:
                            batch = [r for r in rel_p_p_acc if r['rel'] == rt]
                            query = """
                                UNWIND $data AS row
                                MERGE (a:인물 {명칭: row.src})
                                MERGE (b:인물 {명칭: row.dst})
                                MERGE (a)-[:`REL_TYPE`]->(b)
                            """.replace("REL_TYPE", rt)
                            _run_batches(session, query, batch, batch_size=5000)
                        print(f"    ✓ {len(rel_p_p_acc)}개 인물 관계 추가됨")

                except Exception as e:
                    print(f"⚠️ {table_name} 관계 분석 중 오류: {e}")

            # CIDOC-CRM 매핑 적용: Postgres의 tei_cidoc_mappings 테이블에서 TTL 불러와 병합
            print("🔧 CIDOC 매핑 적용을 시도합니다 (Postgres 테이블: tei_cidoc_mappings)")
            try:
                # Use chunksize for memory efficiency and faster incremental processing
                chunks = pd.read_sql(
                    'SELECT table_name, rowid, mapping_label, cidoc_ttl FROM tei_cidoc_mappings ORDER BY id',
                    pg_engine,
                    chunksize=10000
                )
                total_processed = 0
                for chunk in chunks:
                    mappings = list(chunk.itertuples(index=False, name=None))
                    apply_cidoc_mappings(session, mappings)
                    total_processed += len(chunk)
                    print(f"  ✓ CIDOC 매핑 {total_processed}행 처리 중...")
            except Exception as e:
                print(f"⚠️ tei_cidoc_mappings 로딩/처리 실패: {e}")

    print("\n✨ 모든 데이터와 탄압 기구의 계층 구조까지 완벽하게 통합되었습니다!")


if __name__ == "__main__":
    try:
        build_ultimate_graph()
    finally:
        neo4j_driver.close()
