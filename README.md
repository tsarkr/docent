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

데이터베이스 주의사항

- 프로덕션에서 실행하기 전에는 반드시 Postgres와 Neo4j의 백업을 권장합니다.
- Neo4j에 대량 쓰기 시 메모리 설정(`dbms.memory.heap.max_size`)을 모니터링하세요.

기여 및 라이선스

- **라이선스: 미정** — 이 저장소의 공식 라이선스는 아직 결정되지 않았습니다. 사용·배포·재사용 관련 권한은 라이선스가 명시되기 전까지 제한됩니다. 라이선스를 지정하거나 변경하려면 이슈를 통해 논의해 주세요.

- 문의: 저장소 내 이슈/PR로 요청해 주세요.
