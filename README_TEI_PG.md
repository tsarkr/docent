작업 요약

이 모듈은 Postgres 테이블의 현재 데이터로 TEI(XML) 프래그먼트를 `tei` 칼럼에 갱신하고, TEI 하나당 복수개의 CIDOC-CRM TTL 매핑을 생성해 `tei_cidoc_mappings` 테이블에 보관합니다.

환경 변수

- `PGHOST` (기본: localhost)
- `PGPORT` (기본: 5432)
- `PGUSER` (기본: postgres)
- `PGPASSWORD` (기본: '')
- `PGDATABASE` (기본: postgres)

설치

가상환경을 활성화한 뒤 요구 패키지를 설치하세요:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

작업 흐름

1. Postgres 테이블에서 `tei` 칼럼 갱신하기:

```bash
PGHOST=... PGUSER=... PGPASSWORD=... PGDATABASE=... .venv/bin/python scripts/pg_to_pg_with_tei.py
```

2. TEI 당 복수 CIDOC-CRM 매핑 생성 및 저장:

```bash
PGHOST=... PGUSER=... PGPASSWORD=... PGDATABASE=... .venv/bin/python scripts/generate_cidoc_mappings.py
```

데이터베이스 변경 주의

- 스크립트는 CSV 파일 이름을 정규화하여 테이블명을 생성합니다. 동일한 테이블명이 이미 존재하면 데이터를 삽입하거나 스킵합니다.
- 프로덕션에서 사용하기 전에 백업을 권장합니다.

확장

- `generate_cidoc_mappings.py`는 현재 단순 휴리스틱으로 TTL을 생성합니다. CIDOC-CRM 매핑 규칙(yaml)을 추가해 정교하게 확장할 수 있습니다.
