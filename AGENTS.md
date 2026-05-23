**Agent Instructions for Docent**

이 문서는 AI 코딩 에이전트가 이 저장소에서 빠르게 생산적으로 작업하도록 돕는 간결한 안내입니다. 자세한 사용법과 배경은 [README.md](README.md) 를 참고하세요.

- **목적**: 이 저장소는 TEI/PG 원자료를 가공해 Neo4j 그래프와 결합하는 파이프라인 및 Streamlit 기반 UI를 제공합니다.

- **빠른 시작(핵심 커맨드)**:
  - **가상환경 생성/활성화**: `python -m venv .venv` → `. .venv/bin/activate` (사용자 환경에 따라 `.venv3.14` 등 이름이 다를 수 있습니다.)
  - **의존성 설치**: `pip install -r requirements.txt`
  - **Streamlit 실행**: `.venv/bin/streamlit run app.py` (또는 `.venv3.14/bin/streamlit run app.py`)
  - **전체 파이프라인**: `.venv/bin/python run_pipeline.py`

- **핵심 파일 / 위치(빠른 링크)**:
  - 프로젝트 개요: [README.md](README.md)
  - 의존성: [requirements.txt](requirements.txt)
  - UI 앱: [app.py](app.py)
  - 그래프 적재: [graph_builder.py](graph_builder.py)
  - 전체 오케스트레이션: [run_pipeline.py](run_pipeline.py)
  - 데이터 및 매핑: [data/](data)
  - 유틸 스크립트: [scripts/](scripts)

- **주요 개발 규칙·관습 (간단)**:
  - 데이터 원본(샘플 CSV/TEI)은 `data/`에 위치합니다. 매핑 JSON은 `data/column_mappings/`에 있습니다.
  - 매핑을 재생성하려면 `upload_data.py`를 사용하세요.
  - TEI 태깅 기본 스크립트: `scripts/tag_tei_with_dict.py` (사전 기반). LLM 태깅은 별도 스크립트를 사용합니다.
  - DB 연결 정보는 환경변수 또는 `.streamlit/secrets.toml`로 제공됩니다. 실행 전 반드시 확인하세요.

- **에이전트 작업 가이드라인(간결한 규칙)**:
  - **링크하라(Embed 금지)**: 이미 존재하는 문서나 스크립트는 복사하지 말고 해당 파일을 링크하세요.
  - **파괴적 명령 금지**: 대량 삭제나 DB 초기화(`--wipe` 등) 명령을 실행하기 전에 사용자 확인을 요구하세요.
  - **dry-run 우선**: 제공되는 `--dry-run` 옵션을 우선 사용해 출력 결과를 확인하세요.
  - **환경 확인**: 가상환경 활성화, `requirements.txt` 설치 여부, 그리고 `PG_*`/`NEO4J_*` 환경변수를 확인한 뒤 명령을 실행하세요.
  - **간단한 수정 원칙**: 에이전트가 파일을 생성/수정할 때는 최소 변경을 유지하고, 변경 내용을 요약해 PR용 메시지를 준비하세요.

- **추가 제안**:
  - 리포지토리가 확장되면 `AGENTS/specific-areas.md`와 같은 영역별 에이전트 안내 파일을 분리해 두는 것을 권장합니다.

--
_생성된 파일: `AGENTS.md` — 에이전트가 빠르게 들어와 작업하도록 최소한의 실행법·핵심 파일 링크·안전 수칙을 제공._
