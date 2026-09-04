# Docent

3·1운동 역사 자료를 PostgreSQL, TEI/XML, CIDOC-CRM, Neo4j로 연결하는 데이터 처리 저장소입니다. 원자료 CSV/XLSX를 PostgreSQL의 `raw_*` 테이블로 적재하고, 행 단위 TEI를 생성·정제한 뒤 엔티티와 관계를 Neo4j 지식 그래프로 구성합니다. PHP 기반 도슨트와 MCP 서버는 완성된 PostgreSQL/Neo4j 데이터를 조회하는 소비 계층입니다.

이 문서는 현재 저장소의 Python 파이프라인과 PHP/MCP 구성만 설명합니다. `app.py`와 `app_v2.py`는 실행 대상과 문서 범위에서 제외합니다.

## 전체 흐름

```text
data/*.csv, data/*.xlsx
	│
	▼
upload_data.py
	│  PostgreSQL raw_* 테이블 + data/column_mappings/*.json
	▼
scripts/pg_to_pg_with_tei.py
	│  각 행에 rowid, tei 컬럼 생성
	▼
scripts/tag_tei_with_dict.py
	│  한자/인명/장소/날짜 등의 TEI 태그 정제
	▼
Extracted_Historical_Entities.csv/.xlsx
	│  사람 검토(HITL)
	▼
scripts/link_persnames_i815.py
	│  i815 인명사전 단일 매칭을 persName ref에 연결
	▼
scripts/generate_cidoc_mappings.py
	│  tei_cidoc_mappings 테이블 + CIDOC_Timeline_Mappings.ttl
	▼
graph_builder.py
	   Neo4j 노드·관계·CIDOC 매핑 재구축
```

## 요구사항

- Python 3.11 이상 권장
- PostgreSQL: 원자료와 TEI/CIDOC 중간 산출물 저장
- Neo4j 5.x: 지식 그래프 저장
- `scripts/tag_tei_with_dict.py` 실행 시 Ollama와 `qwen3.6` 모델
- i815 연결 단계 실행 시 인터넷 연결
- PHP 웹 계층을 사용할 경우 PHP와 Composer 의존성

Python 의존성은 [requirements.txt](requirements.txt)에 있습니다. PHP 의존성은 [composer.json](composer.json)에 정의되어 있으며 현재 `vendor/`에 설치된 라이브러리도 포함되어 있습니다.

## 설정

Python 스크립트는 우선 환경변수를 읽고, 일부 스크립트는 저장소의 `.streamlit/secrets.toml`을 보조 설정으로 읽습니다. 비밀번호와 토큰은 저장소에 커밋하지 마세요.

```toml
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "historical"
PG_USER = "postgres"
PG_PASSWORD = "..."

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "..."
```

주요 환경변수는 `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`, `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`입니다. `pg_to_pg_with_tei.py`는 `TEI_TABLES`로 처리할 테이블을 쉼표로 제한할 수 있고, 기본적으로 `TEI_TABLE_PREFIX=raw_`인 테이블을 처리합니다.

`tag_tei_with_dict.py`는 `.streamlit/secrets.toml`의 PostgreSQL 설정과 로컬 Ollama의 `qwen3.6` 모델을 사용합니다. 실행 전에 다음을 준비합니다.

```bash
ollama pull qwen3.6
```

## 설치

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
```

## 표준 파이프라인 실행

모든 단계를 순서대로 실행하려면 저장소 루트에서 다음을 실행합니다.

```bash
.venv/bin/python run_pipeline.py
```

실행 순서는 다음과 같습니다.

1. `upload_data.py`: `data/`의 CSV/XLSX를 PostgreSQL에 적재하고 컬럼 매핑 JSON을 갱신합니다.
2. `scripts/pg_to_pg_with_tei.py`: `raw_*` 테이블에 `rowid`와 `tei`를 만들고 행별 TEI를 생성합니다.
3. `scripts/tag_tei_with_dict.py --all --limit 0 --skip-hitl`: TEI를 정제하고 Ollama 문맥 추출로 인명을 확정합니다.
4. `scripts/link_persnames_i815.py --apply`: 3글자 한글 후보를 i815 인명사전과 비교하고 단일 매칭만 DB에 반영합니다.
5. `scripts/extract_all_entities.py`: `persName`, `placeName`, `term`을 `Extracted_Historical_Entities.csv`로 추출합니다.
6. `scripts/generate_cidoc_mappings.py`: 인물 태그를 순차 활동으로 해석해 CIDOC TTL을 생성하고 PostgreSQL에 저장합니다.
7. `graph_builder.py`: PostgreSQL 원자료, TEI, 기관 계층, 인물 관계, CIDOC 매핑을 Neo4j에 적재합니다.

5단계 뒤에는 `Extracted_Historical_Entities.xlsx`가 생성됩니다. 오케스트레이터는 기본적으로 이 파일을 사람이 검토한 뒤 Enter를 누를 때 다음 단계로 진행합니다. 자동 실행이 필요하면 다음처럼 설정합니다.

```bash
SKIP_HITL_PAUSE=1 .venv/bin/python run_pipeline.py
```

TEI 태깅의 처리량은 `USE_PROCESS_POOL`, `MAX_WORKERS`, `CHUNK_SIZE` 환경변수로 조정할 수 있습니다.

## 개별 스크립트

### 원자료 적재: `upload_data.py`

현재 `data/`에 있는 사건, 서지, 세부 장소, 사건-장소 연결, 출처, 경찰·군대·헌병 기관 파일을 읽습니다. CSV의 인코딩(`utf-8-sig`, `cp949`, `utf-16`, `euc-kr`)과 쉼표/탭 구분자를 자동 탐지하고, 헤더를 정규화해 `data/column_mappings/`에 매핑을 저장합니다.

실제 적재 시 기존 대상 테이블을 `DROP TABLE ... CASCADE`로 삭제한 뒤 다시 생성합니다. 따라서 이미 생성된 `tei`, `tei_status` 같은 파생 컬럼도 사라집니다. 원자료를 다시 적재한 뒤에는 TEI 이후 단계를 반드시 다시 실행해야 합니다.

```bash
.venv/bin/python upload_data.py
```

### TEI 생성: `scripts/pg_to_pg_with_tei.py`

PostgreSQL의 `raw_*` 테이블을 읽어 모든 행에 `rowid`와 `tei`를 추가합니다. 사전 카탈로그에서 인물, 장소, 기관, 사건 후보를 만들고, 날짜는 `<date when="YYYY-MM-DD">`, 인물은 `<persName>`, 장소는 `<placeName>` 등으로 태깅합니다.

```bash
TEI_TABLES=raw_event_info,raw_bibliography \
  .venv/bin/python scripts/pg_to_pg_with_tei.py
```

### TEI 정제: `scripts/tag_tei_with_dict.py`

한자 독음 보정, 지명·직함 오인식 제거, Ollama 기반 인명 추출, `<persName ref="...">` 삽입을 수행합니다. 처리 완료 행에는 `tei_status=REFINED`가 기록되어 재실행 시 건너뜁니다.

```bash
.venv/bin/python scripts/tag_tei_with_dict.py --limit 100 --skip-hitl
.venv/bin/python scripts/tag_tei_with_dict.py --all --skip-hitl
```

### i815 인명 연결: `scripts/link_persnames_i815.py`

엔티티 CSV에서 정확히 3글자인 한글 `persName`/`term` 후보를 추출하고 i815 API 전체 색인을 로컬 캐시로 저장합니다. 후보에 정확히 하나의 검색 결과가 있을 때만 해당 URL을 TEI의 `persName ref`에 기록합니다.

```bash
# DB를 변경하지 않고 미리 확인
.venv/bin/python scripts/link_persnames_i815.py --dry-run --limit 20

# 실제 반영
.venv/bin/python scripts/link_persnames_i815.py --apply

# i815 캐시를 무시하고 다시 생성
.venv/bin/python scripts/link_persnames_i815.py --apply --refresh-index
```

결과는 `data/i815_person_index.json`과 `data/persname_match_summary.csv`에 저장됩니다.

### 엔티티 추출: `scripts/extract_all_entities.py`

TEI가 있는 모든 `raw_*` 테이블을 스트리밍 방식으로 읽고 다음 열을 가진 UTF-8 BOM CSV를 생성합니다.

`출처_테이블`, `사료_ROWID`, `사건아이디`, `태그_유형(Class)`, `고유_식별자(Ref/Type)`, `표기_한자(Hanja)`, `한글_독음(Gloss)`

```bash
.venv/bin/python scripts/extract_all_entities.py
```

### CIDOC 생성: `scripts/generate_cidoc_mappings.py`

`tei_cidoc_mappings` 테이블을 만들고, 각 행의 `persName`을 `E7_Activity`와 `E39_Actor`로 표현합니다. 활동은 `E5_Event`에 `P9_consists_of`로 연결되고, 활동 간 순서는 `P134_was_continued_by`, 행위자는 `P14_carried_out_by`로 연결됩니다. 통합 TTL은 `CIDOC_Timeline_Mappings.ttl`에 저장됩니다.

```bash
.venv/bin/python scripts/generate_cidoc_mappings.py
```

### Neo4j 구축: `graph_builder.py`

실행할 때마다 Neo4j의 모든 노드를 `DETACH DELETE`한 뒤 전체 그래프를 재생성합니다. 다음 노드와 관계를 구성합니다.

- 노드: `장소`, `사건`, `문건`, `기관`, `인물`
- 주요 관계: `P14_carried_out_by`, `P7_took_place_at`, `P152_has_parent`, `foaf:knows`, `foaf:member`, `소속`
- 제약조건/인덱스: 장소·사건·문건·기관 ID, 인물·장소·사건 UID, 장소명, 다중 라벨 전문 검색 인덱스

```bash
.venv/bin/python graph_builder.py
```

## 데이터와 산출물

- `data/`: 원자료 CSV/XLSX, i815 캐시, 매칭 요약
- `data/column_mappings/`: 원본 헤더와 PostgreSQL 컬럼의 추적용 JSON
- PostgreSQL `raw_*`: 원자료 테이블 및 행별 `rowid`, `tei`, `tei_status`
- PostgreSQL `tei_cidoc_mappings`: 행별 CIDOC Turtle
- `Extracted_Historical_Entities.csv`: 엔티티 검토용 마스터 CSV
- `Extracted_Historical_Entities.xlsx`: HITL 검토용 워크북
- `CIDOC_Timeline_Mappings.ttl`: 전체 CIDOC-CRM Turtle

## PHP 도슨트와 MCP

`docent.php`는 PostgreSQL 근거와 Neo4j 그래프를 함께 사용하는 PHP 도슨트 엔드포인트입니다. 검색, 그래프 조회, PostgreSQL 사료 선조회, 해설 생성을 POST AJAX 작업으로 처리하며 세션 CSRF, Origin 검사, 입력 정제, 요청 제한을 적용합니다. `docent_eng.php`는 `DOCENT_LANG=en`을 설정한 영어 진입점입니다. PHP 설정은 `.env`에서 읽고, Composer 패키지는 `composer.json`으로 관리합니다.

```bash
composer install
php -S 127.0.0.1:8080
```

실제 배포에서는 PHP 서버의 문서 루트와 `docent.php` 라우팅을 확인해야 합니다. 저장소 루트에는 현재 `index.php`가 없고 `index.html`은 배포된 `https://11e.kr/index.php`를 iframe으로 가리키므로, `index.html`을 로컬 PHP 도슨트의 실행 방법으로 오해하면 안 됩니다.

`mcp_server.py`는 표준 입출력 기반 MCP 서버입니다. 제공 도구는 `query_postgres`, `get_postgres_schema`, `query_neo4j`, `get_neo4j_schema`이며 PostgreSQL 쿼리는 `SELECT` 또는 `WITH`로 시작하는 읽기 쿼리만 허용합니다.

```bash
.venv/bin/python mcp_server.py
```

클라이언트 설정 예시는 [mcp_client_config.json](mcp_client_config.json)을 사용합니다. 설정 파일의 절대 경로는 각 개발 환경에 맞게 수정해야 합니다.

## 운영상 주의사항

- `upload_data.py`는 대상 테이블을 삭제 후 재생성하므로 운영 DB에서 먼저 백업하고 실행합니다.
- `graph_builder.py`는 Neo4j 전체 그래프를 삭제 후 재구축하므로 개발/검증 DB에서 먼저 실행합니다.
- `tei_status=REFINED`가 남아 있으면 태깅 단계가 해당 행을 건너뜁니다. 원문을 다시 처리하려면 상태를 정리하거나 원자료 적재부터 다시 수행해야 합니다.
- `generate_cidoc_mappings.py`와 `graph_builder.py`는 PostgreSQL의 `tei_cidoc_mappings`를 전제로 합니다. CIDOC 생성 전에 TEI와 인명 태깅을 완료해야 합니다.
- PostgreSQL과 Neo4j 접속 비밀번호는 `.streamlit/secrets.toml` 또는 `.env`를 외부에 공개하지 않습니다.
- 대량 처리 전에는 `--limit`, `--dry-run`, 별도 데이터베이스를 사용해 결과를 확인합니다. `--apply`와 전체 그래프 재구축은 명시적으로 확인한 뒤 실행합니다.

## 라이선스

저장소에 [LICENSE](LICENSE)가 포함되어 있습니다. 원자료의 이용 조건과 외부 i815 데이터의 이용 정책도 별도로 확인해야 합니다.
