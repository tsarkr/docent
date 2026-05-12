# Docent

이 저장소는 역사 자료(TEI/PG)와 Neo4j 그래프를 통합해 역사 도슨트(검색 및 해설)를 제공하는 도구입니다.

주요 기능

- PostgreSQL 테이블의 `tei` 컬럼에서 TEI(XML)를 읽어 처리합니다.
- TEI에서 인물(persName), 장소(placeName) 등을 추출하여 Neo4j에 관계로 주입합니다.
- Postgres의 원자료(행)과 Neo4j 그래프를 결합해 근거기반 해설을 생성합니다.
- Streamlit 기반 UI를 제공하여 검색, 그래프 시각화, 해설 생성, 디버깅 쿼리 확인을 지원합니다.

빠른 시작

1. 가상환경 생성 및 의존성 설치

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

2. 환경 변수 설정(예)

- `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`
- `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`

3. Streamlit 앱 실행

```bash
.venv/bin/streamlit run app.py
```

파일과 스크립트

- `app.py` — Streamlit UI 및 Neo4j/Postgres 연동 로직
- `graph_builder.py` — Postgres → Neo4j 데이터 적재 스크립트(TEI 파싱/관계 생성)
- `scripts/` — TEI/PG 처리 유틸 및 변환 스크립트
- `data/` — 원자료(샘플 CSV/TEI) 및 매핑 설정

**스크립트 실행 순서 (권장 워크플로)**

1. **환경 준비**: 가상환경을 활성화하고 `requirements.txt`를 설치합니다. Streamlit의 `secrets.toml` 또는 환경변수에 Postgres/Neo4j 접속 정보를 설정하세요.

2. **데이터 매핑 확인 / 재생성**: CSV/Excel 헤더와 DB 컬럼 매핑을 최신화하려면 `upload_data.py`를 실행해 `data/column_mappings/`를 갱신합니다. (이 스크립트는 파일의 헤더를 감지하고 매핑 JSON을 출력합니다.)

3. **TEI 컬럼을 Postgres에 준비**: TEI를 Postgres에 넣거나 기존 테이블에 `tei` 컬럼을 복사하려면 `scripts/pg_to_tei.py` 또는 `scripts/pg_to_pg_with_tei.py`를 사용하세요.

4. **CIDOC 매핑 생성 (선택적 / dry-run 권장)**: TEI 기반으로 CIDOC-TTL 스니펫을 생성하려면 `scripts/generate_cidoc_mappings.py`를 사용합니다. 먼저 `--dry-run`으로 결과를 확인한 뒤 실제로 저장/삽입하세요.

5. **TEI → Neo4j 적용**: Neo4j에 TEI에서 추출한 노드/관계를 넣으려면 `scripts/tei_to_neo4j.py`를 실행합니다. 대량 쓰기 전에는 `--wipe` 같은 옵션을 확인하고, 테스트 환경에서 먼저 실행하세요.

6. **Streamlit UI 실행 및 검증**: 모든 데이터가 준비되면 `streamlit run app.py`로 앱을 띄워 검색·그래프·해설 생성 흐름을 확인합니다. 해설 생성 지연 문제는 하단 디버그 패널(`PG`, `Neo4j`, `Model` 타이밍)을 확인하세요.

팁:
- `scripts/generate_cidoc_mappings.py`는 `--dry-run` 옵션으로 TTL 생성을 미리 볼 수 있습니다.
- `data/` 폴더는 기본적으로 `.gitignore`에 포함되어 있을 수 있으니, 매핑 파일을 버전관리하려면 `data/column_mappings/*.json`만 별도 폴더로 옮기거나 `.gitignore`를 수정하세요.
- 실행 전 DB 연결 정보는 `.streamlit/secrets.toml` 또는 환경변수로 설정하세요.

데이터베이스 주의사항

- 프로덕션에서 실행하기 전에는 반드시 Postgres와 Neo4j의 백업을 권장합니다.
- Neo4j에 대량 쓰기 시 메모리 설정(`dbms.memory.heap.max_size`)을 모니터링하세요.

기여 및 라이선스

- **라이선스: 미정** — 이 저장소의 공식 라이선스는 아직 결정되지 않았습니다. 사용·배포·재사용 관련 권한은 라이선스가 명시되기 전까지 제한됩니다. 라이선스를 지정하거나 변경하려면 이슈를 통해 논의해 주세요.

- 문의: 저장소 내 이슈/PR로 요청해 주세요.
