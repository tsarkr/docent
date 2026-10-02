# Docent

3·1운동 역사 원자료를 PostgreSQL, TEI/XML, CIDOC-CRM, Neo4j로 연결하고,
검색 결과를 근거로 다국어 해설을 생성하는 연구용 데이터·RAG 저장소입니다.

저장소는 다음 네 계층으로 구성됩니다.

1. **전처리 계층**: CSV/XLSX 원자료를 PostgreSQL `raw_*` 테이블에 적재하고
   행 단위 TEI를 생성·정제합니다.
2. **온톨로지 계층**: TEI의 인물·장소·사건 정보를 CIDOC-CRM 활동과 행위자로
   변환하고, Neo4j 노드·관계로 적재합니다.
3. **서비스 계층**: [docent.php](./docent.php)가 Neo4j와 PostgreSQL을 함께
   검색하고 Gemini 기반 해설을 생성합니다. [mcp_server.py](./mcp_server.py)는
   IDE/데스크톱 MCP 클라이언트용 읽기 도구를 제공합니다.
4. **평가 계층**: [evaluation/](./evaluation/)에서 Vector RAG, Standard
   GraphRAG, Docent Hybrid RAG를 비교하고 RAGAS 및 보조 검색 지표를 산출합니다.

현재 저장소에는 `app.py` 또는 Streamlit 앱이 없습니다. 웹 UI의 실행 대상은
[index.html](./index.html) 또는 [docent.php](./docent.php)입니다.

## 아키텍처

```text
data/*.csv, data/*.xlsx
        │
        ▼
upload_data.py
        │  PostgreSQL raw_* 테이블
        ▼
scripts/pg_to_pg_with_tei.py
        │  rowid + TEI/XML
        ▼
scripts/tag_tei_with_dict.py
        │  사전/LLM 기반 TEI 정제
        ▼
scripts/link_persnames_i815.py
scripts/extract_all_entities.py
        │
        ├── Extracted_Historical_Entities.csv/.xlsx
        └── scripts/generate_cidoc_mappings.py
                    │
                    ├── tei_cidoc_mappings
                    └── CIDOC_Timeline_Mappings.ttl
                                │
                                ▼
                    scripts/graph_builder.py
                                │
                                ▼
                    Neo4j 그래프
                       │       │
          vectorize_neo4j.py    │
          apply_vectors_to_neo4j.py
                       │       │
                       ▼       ▼
                   vector_store/  PHP Docent / MCP
```

## 요구사항

- Python 3.11 이상
- PostgreSQL
- Neo4j 5.x
- Ollama
  - TEI 인명 보조 추출: 기본 `qwen3.6`
  - 임베딩: 기본 `nomic-embed-text`
  - RAGAS 로컬 Judge: 기본 `gemma4:26b`
- PHP와 Composer
  - `laudis/neo4j-php-client`
  - `vlucas/phpdotenv`
- i815 인명 연결 단계의 최초 실행에는 인터넷 연결

Python 의존성은 [requirements.txt](./requirements.txt), PHP 의존성은
[composer.json](./composer.json)에 정의되어 있습니다.

## 설치

```bash
python3 -m venv .venv3.14
.venv3.14/bin/python -m pip install -r requirements.txt
composer install
```

저장소의 실행 스크립트는 `.venv3.14/bin/python`, `.venv/bin/python3`,
`.venv/bin/python` 순으로 Python 실행 파일을 탐색합니다. 다른 가상환경을
사용한다면 아래 예시의 실행 파일을 해당 환경의 경로로 바꾸십시오.

Ollama 모델은 별도로 준비합니다.

```bash
ollama pull qwen3.6
ollama pull nomic-embed-text
ollama pull gemma4:26b       # RAGAS 및 직접 Context Precision Judge 사용 시
```

## 설정

Python 코드는 환경변수를 우선하고, [scripts/config.py](./scripts/config.py)를
통해 `.streamlit/secrets.toml`을 보조 설정으로 읽습니다. PHP 코드는
환경변수, `.env`, `.streamlit/secrets.toml` 순으로 설정을 읽습니다.

비밀번호와 API 키는 저장소에 커밋하지 마십시오.

```toml
# .streamlit/secrets.toml 또는 환경변수
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "historical"
PG_USER = "postgres"
PG_PASSWORD = "change-me"

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "change-me"

GEMINI_API_KEY = "..."
GEMINI_MODEL = "gemini-flash-lite-latest"
```

주요 선택 설정:

| 변수 | 기본값 | 사용처 |
|---|---|---|
| `TEI_TABLE_PREFIX` | `raw_` | TEI 처리 대상 테이블 접두사 |
| `TEI_TABLES` | 전체 `raw_*` | TEI 처리 테이블을 쉼표로 제한 |
| `TEI_COMMIT_BATCH_SIZE` | `50` | TEI DB 커밋 배치 |
| `USE_PROCESS_POOL` | `1` | TEI 태깅 프로세스 풀 사용 |
| `MAX_WORKERS` | CPU 기반 | TEI 태깅 worker 수 |
| `CHUNK_SIZE` | `200` | TEI 태깅 처리 묶음 |
| `SKIP_HITL_PAUSE` | 미설정 | `1/true/yes/on`이면 사람 검토 일시정지 생략 |
| `PIPELINE_STAGE` | `all` | `all`, `rdbms`, `neo4j` |
| `OLLAMA_HOST` | Ollama 기본 주소 | 임베딩·시소러스 Ollama 주소 |
| `OLLAMA_EMBED_MODEL` | `nomic-embed-text` | Neo4j 임베딩 모델 |
| `GEMINI_TEMPERATURE` | `0.1` | PHP Gemini 호출 temperature |
| `GEMINI_TOP_P` | `0.85` | PHP Gemini 호출 top-p |
| `RAGAS_PROVIDER` | `ollama` | `ollama` 또는 `gemini` |

## 전체 데이터 파이프라인

### Dry-run 먼저 확인

```bash
.venv3.14/bin/python run_pipeline.py --dry-run
```

### 전체 실행

```bash
.venv3.14/bin/python run_pipeline.py
```

`run_pipeline.py`는 다음 단계를 순서대로 실행합니다.

1. [upload_data.py](./upload_data.py): `data/`의 CSV/XLSX를 PostgreSQL
   `raw_*` 테이블로 적재하고 컬럼 매핑을 생성합니다.
2. [scripts/pg_to_pg_with_tei.py](./scripts/pg_to_pg_with_tei.py): 원자료 행에
   `rowid`와 TEI 컬럼을 생성합니다.
3. [scripts/tag_tei_with_dict.py](./scripts/tag_tei_with_dict.py): 한자·인명·
   장소·날짜를 정제하고 `<persName>`, `<placeName>` 등의 태그를 보완합니다.
4. [scripts/link_persnames_i815.py](./scripts/link_persnames_i815.py):
   단일 매칭되는 인명 후보에 i815 참조를 연결합니다.
5. [scripts/extract_all_entities.py](./scripts/extract_all_entities.py):
   TEI 엔티티를 `Extracted_Historical_Entities.csv`로 추출하고 XLSX 변환을
   준비합니다.
6. 사람이 XLSX를 검토합니다. 기본 실행은 이 시점에서 Enter 입력을 기다립니다.
7. [scripts/generate_cidoc_mappings.py](./scripts/generate_cidoc_mappings.py):
   `tei_cidoc_mappings`와 [CIDOC_Timeline_Mappings.ttl](./CIDOC_Timeline_Mappings.ttl)을
   생성합니다.
8. [scripts/graph_builder.py](./scripts/graph_builder.py): PostgreSQL 결과를
   Neo4j 노드·관계로 구축합니다.
9. [thesaurus_to_neo4j.py](./thesaurus_to_neo4j.py): 시소러스를 Neo4j에 적재합니다.
10. [scripts/vectorize_neo4j.py](./scripts/vectorize_neo4j.py):
    Neo4j 노드 임베딩을 [vector_store/](./vector_store/)에 생성합니다.
11. [scripts/apply_vectors_to_neo4j.py](./scripts/apply_vectors_to_neo4j.py):
    임베딩을 Neo4j 노드의 `embedding` 속성에 기록합니다.

사람 검토 없이 실행하려면:

```bash
.venv3.14/bin/python run_pipeline.py --skip-hitl
```

`--skip-thesaurus-embed` 옵션은 호환성을 위해 유지되어 있습니다. 현재
오케스트레이터는 시소러스 적재 시 개별 임베딩을 건너뛰고, 뒤의
`vectorize_neo4j.py` 단계에서 전체 그래프 임베딩을 생성하는 흐름을 사용합니다.
따라서 임베딩 자체를 만들지 않으려면 `run_pipeline.py` 전체 실행 대신
`--stage rdbms`만 실행하거나, Neo4j 단계의 임베딩 스크립트를 개별적으로
실행하십시오.

전처리와 그래프 구축을 분리하면 불필요한 재처리를 피할 수 있습니다.

```bash
# PostgreSQL/TEI/CIDOC만 실행
.venv3.14/bin/python run_pipeline.py --stage rdbms --skip-hitl

# 이미 준비된 PostgreSQL 결과로 Neo4j·시소러스·벡터 구축
.venv3.14/bin/python run_pipeline.py --stage neo4j
```

원자료를 다시 적재하면 대상 테이블이 삭제 후 재생성되므로 `tei`,
`tei_status` 같은 파생 컬럼도 사라집니다. 원자료를 다시 적재한 뒤에는
TEI 이후 단계를 다시 실행해야 합니다. [scripts/graph_builder.py](./scripts/graph_builder.py)는
그래프를 변경하므로 검색만 필요한 경우 실행하지 마십시오.

### 개별 전처리 명령

```bash
# 원자료 적재
.venv3.14/bin/python upload_data.py

# 특정 테이블만 TEI 처리
TEI_TABLES=raw_event_info,raw_bibliography \
  .venv3.14/bin/python scripts/pg_to_pg_with_tei.py

# TEI 정제
.venv3.14/bin/python scripts/tag_tei_with_dict.py --limit 100 --skip-hitl
.venv3.14/bin/python scripts/tag_tei_with_dict.py --all --skip-hitl

# i815 연결: 먼저 dry-run
.venv3.14/bin/python scripts/link_persnames_i815.py --dry-run --limit 20
.venv3.14/bin/python scripts/link_persnames_i815.py --apply

# 엔티티·CIDOC 생성
.venv3.14/bin/python scripts/extract_all_entities.py
.venv3.14/bin/python scripts/generate_cidoc_mappings.py
```

`upload_data.py`와 그래프 구축은 데이터베이스를 변경합니다. 운영 데이터에
실행하기 전에 연결 대상과 백업을 확인하십시오.

## PHP 도슨트 실행

[docent.php](./docent.php)는 CSRF·Origin·rate-limit 검사를 거친 AJAX API와
웹 UI를 제공합니다. HTML이 필요하면 PHP 서버의 document root를 저장소 루트로
두고 실행합니다.

```bash
php -S 127.0.0.1:8000
```

브라우저에서 <http://127.0.0.1:8000/docent.php> 또는
[index.html](./index.html)을 엽니다.

도슨트 파이프라인의 CLI 디버깅 진입점은
[scripts/run_docent_pipeline.php](./scripts/run_docent_pipeline.php)입니다.

```bash
php scripts/run_docent_pipeline.php "수원 사강리 시위 과정" ko
```

주요 AJAX action:

| Action | 기능 |
|---|---|
| `analyze` | 질의 언어·초점·검색 키워드·의도 분석 |
| `graph` | Text-to-Cypher, Neo4j 탐색, Graph evidence 생성 |
| `pg_prefetch` | 그래프 후보명을 이용한 PostgreSQL 원문 사료 조회 |
| `explain` | evidence와 원문을 합쳐 Stage 1 팩트 추출 및 Stage 2 해설 생성 |
| `node_detail` | 특정 그래프 노드와 연결된 원문 상세 조회 |

### 검색·생성 흐름

```text
질의
  │
  ├─ analyze: Gemini JSON 의도/키워드 분석
  │            실패 시 로컬 토큰 기반 fallback
  │
  ├─ graph: Text-to-Cypher 생성·읽기 전용 검증·Neo4j 실행
  │         결과 부족/개체명 존재 시 namesIndex 및 CONTAINS fallback
  │
  ├─ pg_prefetch: PostgreSQL 원문 조회
  │
  └─ explain:
       evidence budget 적용
       → GraphRAG entity/multi-hop 보강
       → Stage 1 fact table
       → Stage 2 다국어 역사 해설
```

생성된 Cypher는 PHP `error_log`에 다음 prefix로 기록됩니다.

```text
[Docent][Text-to-Cypher] generated query ...
[Docent][Neo4j] executing dynamic Cypher:
[Docent][Neo4j] dynamic Cypher returned N record(s)
```

운영 환경의 PHP-FPM, Apache 또는 Nginx error log에서 확인하십시오. 읽기 전용
가드가 `MATCH`, `OPTIONAL MATCH`, `WITH`, `CALL`로 시작하는 쿼리만 허용하고,
`DELETE`, `CREATE`, `MERGE`, `SET`, `DROP` 등 변경 키워드를 차단합니다.

### 시간 조건의 한계

사건 날짜는 정적 Event 노드의 `날짜`/`발생일자` 속성으로 처리합니다. 관계
자체의 유효기간(`valid_from`, `valid_to`)은 구현되어 있지 않습니다. 따라서
“1919년 3월 이전의 소속”과 같은 관계의 시간 이력 질의는 정확하게 판정할 수
없으며, 사건 날짜 기반 근사 또는 `자료에서 확인되지 않음`으로 제한해야 합니다.

## MCP 서버

[mcp_server.py](./mcp_server.py)는 다음 읽기 도구를 제공합니다.

- `query_postgres`: `SELECT`/`WITH` 쿼리
- `get_postgres_schema`: PostgreSQL 스키마 조회
- `query_neo4j`: Cypher 조회
- `get_neo4j_schema`: Neo4j label·relationship type 조회

실행:

```bash
.venv3.14/bin/python mcp_server.py
```

MCP 클라이언트에는 `mcp_client_config.json`의 절대경로를 현재 환경에 맞게
수정해 등록합니다. MCP 서버는 데이터베이스에 직접 연결하므로 비밀정보를
클라이언트 설정 파일에 평문으로 추가하지 말고 환경변수 또는
`.streamlit/secrets.toml`을 사용하십시오.

## RAG 평가

평가 데이터셋은
[evaluation/dataset/multihop_benchmark_dataset.json](./evaluation/dataset/multihop_benchmark_dataset.json)에
있습니다. 비교 대상은 다음 세 시스템입니다.

- **Vector RAG**: PostgreSQL 원문 chunk 기반 검색
- **Standard GraphRAG**: 그래프 triple/context 기반 검색
- **Docent Hybrid RAG**: 그래프·원문을 결합한 검색 및 엄격 모드 해설

기본 전체 평가:

```bash
.venv3.14/bin/python evaluation/eval_rag_comparison.py --delay 15
```

빠른 검증:

```bash
# 대표 3문항
.venv3.14/bin/python evaluation/eval_rag_comparison.py --quick --delay 0

# Q_MH_01 한 문항과 전체 response_text 출력
.venv3.14/bin/python evaluation/eval_rag_comparison.py --single --delay 0

# 앞에서부터 N문항
.venv3.14/bin/python evaluation/eval_rag_comparison.py --limit 10 --delay 0
```

통합 실행기는 NER/관계 추출 평가와 RAG 비교를 연속 실행합니다.

```bash
.venv3.14/bin/python evaluation/run_benchmark.py --quick --delay 0
.venv3.14/bin/python evaluation/run_benchmark.py --quick --ragas --delay 0
```

선택적으로 데이터셋을 다시 생성할 수 있습니다. 자동 템플릿 문항이 늘어나면
질의 중복성과 ground truth 독립성을 다시 검토해야 합니다.

```bash
.venv3.14/bin/python evaluation/generate_benchmark_dataset.py \
  --target-count 30
```

### RAGAS

`eval_rag_comparison.py`는 질문, 응답, 검색 context, reference를
[evaluation/results/ragas_dataset.json](./evaluation/results/ragas_dataset.json)에
저장합니다. RAGAS 실행 결과는
[evaluation/results/ragas_results.json](./evaluation/results/ragas_results.json)에
저장되며, 기본 metric은 Faithfulness, Context Precision with Reference,
Context Recall입니다.

기본 로컬 설정:

```bash
RAGAS_PROVIDER=ollama \
.venv3.14/bin/python evaluation/eval_ragas.py
```

Gemini Judge를 사용하려면 `GEMINI_API_KEY`와 다음 선택 설정을 준비합니다.

```bash
RAGAS_PROVIDER=gemini \
GEMINI_RAGAS_MODEL=gemini-flash-lite-latest \
GEMINI_EMBEDDING_MODEL=models/gemini-embedding-001 \
.venv3.14/bin/python evaluation/eval_ragas.py
```

`evaluation_type=abstention`인 부정 검증 문항은 Context Precision/Recall의
정답 회수율을 정의하기 어렵기 때문에 Faithfulness만 평가합니다. RAGAS 내부
구조화 출력이 실패한 경우 0점으로 대체하지 않고 실행을 실패시켜야 합니다.

### 보조 Context Precision Judge

RAGAS parser와 별도로 Ollama JSON API를 직접 사용하는 Judge가 있습니다.
이는 RAGAS 점수가 아니라 보조 지표이며 결과 파일도 분리합니다.

```bash
.venv3.14/bin/python evaluation/eval_context_precision_judge.py \
  --input evaluation/results/ragas_dataset.json \
  --output evaluation/results/context_precision_judge_results.json \
  --model gemma4:26b \
  --host http://localhost:11434 \
  --workers 3
```

### 평가 산출물

| 파일 | 내용 |
|---|---|
| `evaluation/results/rag_evaluation_results.csv` | 질의·모델별 상세 결과 |
| `evaluation/results/table_rag_comparison.tex` | 논문용 비교 표 |
| `evaluation/results/ragas_dataset.json` | RAGAS 입력 |
| `evaluation/results/ragas_results.json` | RAGAS metric 결과 |
| `evaluation/results/context_precision_judge_results.json` | 직접 Judge 결과 |
| `evaluation/results/table_ner_performance.tex` | NER/RE LaTeX 결과 표 |
| `evaluation/results/ner_evaluation_results.csv` | NER/RE 상세 결과 |

CSV의 `entity_hit_rate_at_k`와 `entity_mrr`는 LLM과 무관한 진단 지표입니다.
현재 MRR은 모든 정답 엔티티의 완전한 순위가 아니라, `target_entities` 중
하나가 처음 등장한 context rank를 사용합니다. 따라서 MRR 1.0000을 검색
품질의 완벽한 증명으로 해석하지 말고 Context Precision, source/event ID
Hit@K, 질의 유형별 결과와 함께 보고해야 합니다.

현재 벤치마크 `N=30`은 효과크기 탐색과 시스템 검증에는 유용하지만, 모집단
전체에 대한 통계적 유의성이나 일반화를 단독으로 주장하기에는 작습니다.
논문에는 질의별 결과, bootstrap 신뢰구간, 유형별 결과와 함께 데이터셋의
템플릿·엔티티 앵커 편향을 명시하십시오.

## 검증

파괴적 DB 작업 없이 실행할 수 있는 기본 검증:

```bash
php -l includes/actions.php
php -l docent.php
.venv3.14/bin/python -m py_compile run_pipeline.py mcp_server.py
.venv3.14/bin/python run_pipeline.py --dry-run
.venv3.14/bin/python scripts/verify_pipeline.py
```

데이터베이스 연결 검증은 실제 `PG_*`/`NEO4J_*` 설정이 필요합니다. 원격 DB를
대상으로 할 때는 `upload_data.py`, `graph_builder.py`, `apply_vectors_to_neo4j.py`
를 먼저 실행하지 말고 읽기 전용 확인 쿼리와 `--dry-run`으로 연결·대상을
검증하십시오.

## 주요 디렉터리와 파일

| 경로 | 역할 |
|---|---|
| [data/](./data/) | 원자료, i815 인명 색인, 매핑 파일 |
| [scripts/](./scripts/) | PostgreSQL·TEI·CIDOC·Neo4j 처리 |
| [includes/](./includes/) | PHP 설정, DB, 보안, Gemini, prompt, action |
| [evaluation/](./evaluation/) | 벤치마크·RAGAS·NER/RE 평가 |
| [vector_store/](./vector_store/) | 외부 임베딩 산출물 |
| [Latex/](./Latex/) | 논문 원고와 빌드 산출물 |
| [run_pipeline.py](./run_pipeline.py) | 전체 전처리 오케스트레이터 |
| [docent.php](./docent.php) | PHP 웹/API 진입점 |
| [mcp_server.py](./mcp_server.py) | PostgreSQL/Neo4j MCP 서버 |

## 운영상 주의

- `upload_data.py`는 대상 테이블을 삭제 후 재생성하므로 운영 DB에서 신중히
  실행합니다.
- `graph_builder.py`와 `apply_vectors_to_neo4j.py`는 Neo4j를 변경합니다.
- `--wipe` 등 파괴적 옵션이나 DB 초기화 명령은 백업과 대상 확인 후 실행합니다.
- 원자료, TEI, ground truth, 검색 context는 서로 다른 증거 층위입니다. 모델
  응답에 맞춰 ground truth를 수정하지 말고, 원문·그래프 근거를 먼저 검토합니다.
- 날짜가 없는 정적 edge를 temporal relation으로 해석하지 않습니다.
- API key·DB 비밀번호·원격 접속 정보를 로그와 커밋에 남기지 않습니다.

## 라이선스

라이선스 정보는 [LICENSE](./LICENSE)를 참조하십시오.
