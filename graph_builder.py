import os
import re
import urllib.parse

import hanja
import pandas as pd
from bs4 import BeautifulSoup
from neo4j import GraphDatabase
from sqlalchemy import create_engine

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

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

encoded_pass = urllib.parse.quote_plus(PG_CONFIG["password"])
pg_engine = create_engine(
    f"postgresql://{PG_CONFIG['user']}:{encoded_pass}@{PG_CONFIG['host']}:5432/{PG_CONFIG['database']}"
)
neo4j_driver = GraphDatabase.driver(
    NEO4J_CONFIG["uri"], auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"])
)


def _read_csv_flexible(filepath):
    encodings = ("utf-8-sig", "utf-16", "cp949", "euc-kr", "iso-8859-1")
    for encoding in encodings:
        try:
            with open(filepath, "r", encoding=encoding, errors="strict") as fh:
                lines = [
                    line.rstrip("\n\r")
                    for line in fh
                    if (
                        (clean := line.rstrip("\n\r").replace("\ufeff", "").strip())
                        and not clean.lstrip('"').lstrip("'").startswith("//")
                        and re.sub(r"[\s,\t'\"]+", "", clean)
                    )
                ]

            if not lines:
                continue

            sample = "\n".join(lines[:5])
            delimiter = "\t" if sample.count("\t") >= sample.count(",") else ","

            from io import StringIO

            return pd.read_csv(StringIO("\n".join(lines)), sep=delimiter, dtype=str).fillna("")
        except Exception:
            continue
    raise ValueError(f"CSV 읽기 실패: {filepath}")


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
        session.run(cypher, **params).consume()
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
            session.run(statement).consume()
        except Exception as e:
            print(f"⚠️ 스키마 생성 건너뜀: {e}")


def _clear_graph(session, batch_size=500):
    total_deleted = 0

    while True:
        result = session.run(
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


def build_ultimate_graph():
    with neo4j_driver.session() as session:
        print("🧹 기존 데이터를 삭제하고 그래프를 재설계합니다...")
        _clear_graph(session, batch_size=50)
        _ensure_schema(session)

        print("📍 장소 노드 생성 중... (Postgres TEI에서 로드)")
        try:
            df_place = pd.read_sql('SELECT rowid, tei FROM raw_detail_place WHERE tei IS NOT NULL', pg_engine)
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
            try:
                df_source = pd.read_sql(f'SELECT rowid, tei FROM {source_table} WHERE tei IS NOT NULL', pg_engine)
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
        try:
            df_tei = pd.read_sql(
                'SELECT "아이디" AS event_id, tei FROM raw_event_info WHERE tei IS NOT NULL AND "아이디" IS NOT NULL',
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
                    _run_batches(
                        session,
                        """
                        UNWIND $data AS row
                        MATCH (e:사건 {id: row.event_id})
                        MERGE (p:인물 {명칭: row.person_name})
                        MERGE (e)-[r:P14_carried_out_by]->(p)
                        SET r.context = row.context_text,
                            r.source_tag = 'persName'
                        """,
                        person_acc,
                        batch_size=25,
                    )
                    person_acc = []

                if place_acc:
                    _run_batches(
                        session,
                        """
                        UNWIND $data AS row
                        MATCH (e:사건 {id: row.event_id})
                        MATCH (pl:장소)
                        WHERE pl.명칭 = row.place_name OR pl.한글명칭 = row.place_name
                        MERGE (e)-[r:P7_took_place_at]->(pl)
                        SET r.context = row.context_text,
                            r.source_tag = 'placeName'
                        """,
                        place_acc,
                        batch_size=25,
                    )
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
                            person_name = pers.get_text(' ', strip=True)
                            if person_name:
                                person_acc.append({
                                    'event_id': event_id,
                                    'person_name': person_name,
                                    'context_text': context_text,
                                })

                        for place in section.find_all('placeName'):
                            place_name = place.get_text(' ', strip=True)
                            if place_name:
                                place_acc.append({
                                    'event_id': event_id,
                                    'place_name': place_name,
                                    'context_text': context_text,
                                })

                        if len(person_acc) + len(place_acc) >= flush_every:
                            _flush_acc()
                except Exception as e:
                    print(f"⚠️ TEI 파싱 중 오류 (사건id={event_id}): {e}")

            _flush_acc()

    print("\n✨ 모든 데이터와 탄압 기구의 계층 구조까지 완벽하게 통합되었습니다!")


if __name__ == "__main__":
    try:
        build_ultimate_graph()
    finally:
        neo4j_driver.close()
