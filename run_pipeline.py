#!/usr/bin/env python3
"""Orchestrate the RDBMS preprocessing and Neo4j serving pipelines separately.

RDBMS preprocessing:
1) upload_data.py
2) scripts/pg_to_pg_with_tei.py
3) scripts/tag_tei_with_dict.py
4) scripts/link_persnames_i815.py
5) scripts/extract_all_entities.py
6) HITL pause for xlsx review
7) scripts/generate_cidoc_mappings.py

Neo4j serving:
1) scripts/graph_builder.py
2) thesaurus_to_neo4j.py
3) scripts/vectorize_neo4j.py
4) scripts/apply_vectors_to_neo4j.py

Each step is run synchronously; on error the process exits with non-zero code.
"""
import subprocess
import sys
import os
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parent
# .venv 환경 반영
PY_CANDIDATES = (
    ROOT / '.venv3.14' / 'bin' / 'python',
    ROOT / '.venv' / 'bin' / 'python3',
    ROOT / '.venv' / 'bin' / 'python',
)
PY = next((str(path) for path in PY_CANDIDATES if path.exists()), sys.executable)

XLSX_PATH = ROOT / 'Extracted_Historical_Entities.xlsx'

RDBMS_STEPS = [
    (ROOT / 'upload_data.py', []),
    (ROOT / 'scripts' / 'pg_to_pg_with_tei.py', []),
    (ROOT / 'scripts' / 'tag_tei_with_dict.py', ['--all', '--limit', '0', '--skip-hitl'], {
        'USE_PROCESS_POOL': os.getenv('USE_PROCESS_POOL', '1'),
        'MAX_WORKERS': os.getenv('MAX_WORKERS', str(max(4, (os.cpu_count() or 4)))),
        'CHUNK_SIZE': os.getenv('CHUNK_SIZE', '200'),
    }),
    (ROOT / 'scripts' / 'link_persnames_i815.py', ['--apply']),
    (ROOT / 'scripts' / 'extract_all_entities.py', []),
    (ROOT / 'scripts' / 'generate_cidoc_mappings.py', []),
]

NEO4J_STEPS = [
    (ROOT / 'scripts' / 'graph_builder.py', []),
]


def build_steps(
    skip_thesaurus_embed=False,
    thesaurus_csv=None,
    thesaurus_batch_size=None,
    ollama_url=None,
    embedding_model=None,
    stage='all',
):
    """Build only the requested pipeline stage.

    ``rdbms`` stops after PostgreSQL/TEI/CIDOC preprocessing. ``neo4j``
    assumes that preprocessing is already complete and only rebuilds the
    graph, thesaurus links, and vectors. ``all`` preserves the legacy flow.
    """
    if stage not in {'all', 'rdbms', 'neo4j'}:
        raise ValueError(f'지원하지 않는 파이프라인 단계: {stage}')
    thesaurus_args = []
    csv_path = thesaurus_csv or os.getenv('THESAURUS_CSV')
    batch_size = thesaurus_batch_size or os.getenv('THESAURUS_BATCH_SIZE')
    ollama_host = ollama_url or os.getenv('OLLAMA_HOST')
    ollama_model = embedding_model or os.getenv('OLLAMA_EMBED_MODEL')
    if csv_path:
        thesaurus_args.extend(['--csv', csv_path])
    if batch_size:
        thesaurus_args.extend(['--batch-size', str(batch_size)])
    if ollama_host:
        thesaurus_args.extend(['--ollama-url', ollama_host])
    if ollama_model:
        thesaurus_args.extend(['--embedding-model', ollama_model])
    # Embeddings are generated once, after thesaurus relationships are loaded,
    # so isolated flat thesaurus vectors cannot be written by the pipeline.
    thesaurus_args.append('--skip-embed')
    neo4j_steps = NEO4J_STEPS + [
        (ROOT / 'thesaurus_to_neo4j.py', thesaurus_args),
        (ROOT / 'scripts' / 'vectorize_neo4j.py', []),
        (ROOT / 'scripts' / 'apply_vectors_to_neo4j.py', []),
    ]
    if stage == 'rdbms':
        return RDBMS_STEPS
    if stage == 'neo4j':
        return neo4j_steps
    return RDBMS_STEPS + neo4j_steps


def run_step(path, args, extra_env=None):
    print(f'--- START: {path.relative_to(ROOT)}')
    cmd = [PY, str(path)] + args
    try:
        env = os.environ.copy()
        current_pythonpath = env.get('PYTHONPATH', '')
        env['PYTHONPATH'] = (
            f"{ROOT}:{current_pythonpath}" if current_pythonpath else str(ROOT)
        )
        if extra_env:
            env.update({k: str(v) for k, v in extra_env.items() if v is not None and str(v) != ''})
        subprocess.run(cmd, check=True, env=env, cwd=ROOT)
    except subprocess.CalledProcessError as e:
        print(f'ERROR: step {path} failed with exit {e.returncode}')
        sys.exit(e.returncode)
    except FileNotFoundError:
        print(f'ERROR: script not found: {path}')
        sys.exit(1)
    print(f'--- DONE: {path.relative_to(ROOT)}')


def convert_csv_to_xlsx(csv_path, xlsx_path):
    print(f'🔁 Converting {csv_path.name} to {xlsx_path.name}...')
    try:
        import pandas as pd
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        df.to_excel(xlsx_path, index=False)
        print(f'✅ Conversion complete: {xlsx_path.name}')
    except ImportError:
        print('⚠️ pandas not found, skipping XLSX conversion. Please use the CSV file.')
    except Exception as e:
        print(f'⚠️ Conversion failed: {e}')


def pause_for_hitl(xlsx_path):
    print('--- HOLD: HITL review required')
    print(f'Please review and update the workbook before continuing: {xlsx_path}')
    print('When the workbook is ready, press Enter to continue or Ctrl+C to stop.')
    input()
    print('--- RESUME: continuing after HITL review')


def main():
    parser = argparse.ArgumentParser(description='Docent 전체 데이터 파이프라인 실행')
    parser.add_argument(
        '--stage',
        choices=('all', 'rdbms', 'neo4j'),
        default=os.getenv('PIPELINE_STAGE', 'all'),
        help='실행 범위: rdbms(전처리만), neo4j(기존 RDBMS 결과로 그래프만), all(전체)',
    )
    parser.add_argument(
        '--skip-hitl',
        action='store_true',
        help='엔티티 XLSX 검토 일시정지를 건너뜁니다.',
    )
    parser.add_argument(
        '--skip-thesaurus-embed',
        action='store_true',
        help='시소러스 적재 후 Ollama 임베딩을 건너뜁니다.',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='실행하지 않고 단계와 필수 파일만 확인합니다.',
    )
    parser.add_argument('--thesaurus-csv', help='시소러스 CSV 경로')
    parser.add_argument('--thesaurus-batch-size', type=int, help='시소러스 Neo4j 배치 크기')
    parser.add_argument('--ollama-url', help='Ollama 서버 주소')
    parser.add_argument('--embedding-model', help='Ollama 임베딩 모델명')
    args = parser.parse_args()
    skip_hitl_pause = args.skip_hitl or os.getenv('SKIP_HITL_PAUSE', '').strip().lower() in {'1', 'true', 'yes', 'on'}
    if args.thesaurus_batch_size is not None and args.thesaurus_batch_size < 1:
        parser.error('--thesaurus-batch-size는 1 이상이어야 합니다.')
    steps = build_steps(
        args.skip_thesaurus_embed,
        args.thesaurus_csv,
        args.thesaurus_batch_size,
        args.ollama_url,
        args.embedding_model,
        stage=args.stage,
    )
    print('Pipeline orchestrator starting')
    if args.dry_run:
        missing_paths = []
        for number, step in enumerate(steps, start=1):
            path, step_args = step[:2]
            state = 'OK' if path.exists() else 'MISSING'
            print(f'{number:02d}. [{state}] {path.relative_to(ROOT)} {step_args}')
            if not path.exists():
                missing_paths.append(path)
        if not (ROOT / 'data' / '교육부 국사편찬위원회_한국역사용어시소러스 정보_20211028.csv').exists():
            print('ERROR: 기본 시소러스 CSV가 없습니다.')
            return 1
        if missing_paths:
            return 1
        return 0

    for step in steps:
        if len(step) == 2:
            path, args = step
            extra_env = None
        else:
            path, args, extra_env = step
        if not path.exists():
            print(f'ERROR: required script missing: {path}')
            sys.exit(1)
        
        run_step(path, args or [], extra_env)

        # Refresh XLSX after extract_all_entities.py
        if path.name == 'extract_all_entities.py':
            csv_path = ROOT / 'Extracted_Historical_Entities.csv'
            xlsx_path = ROOT / 'Extracted_Historical_Entities.xlsx'
            if csv_path.exists():
                convert_csv_to_xlsx(csv_path, xlsx_path)

        if path.name == 'extract_all_entities.py' and not skip_hitl_pause:
            xlsx_path = ROOT / 'Extracted_Historical_Entities.xlsx'
            if not xlsx_path.exists():
                # Fallback to CSV if XLSX conversion failed
                xlsx_path = ROOT / 'Extracted_Historical_Entities.csv'
            
            if not xlsx_path.exists():
                print(f'ERROR: HITL file missing: {xlsx_path}')
                sys.exit(1)
            pause_for_hitl(xlsx_path)

    print('Pipeline finished successfully')


if __name__ == '__main__':
    main()
