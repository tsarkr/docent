#!/usr/bin/env python3
"""Orchestrator to run the pipeline in order, aborting on failure.

Sequence:
 1) upload_data.py
 2) scripts/pg_to_pg_with_tei.py
 3) scripts/tag_tei_with_dict.py
 4) scripts/link_persnames_i815.py
 5) scripts/extract_all_entities.py
 6) HITL pause for xlsx review
 7) scripts/generate_cidoc_mappings.py
 8) graph_builder.py

Each step is run synchronously; on error the process exits with non-zero code.
"""
import subprocess
import sys
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
# .venv 환경 반영
PY_VENV = ROOT / '.venv' / 'bin' / 'python3'
if PY_VENV.exists():
    PY = str(PY_VENV)
else:
    PY = sys.executable

XLSX_PATH = ROOT / 'Extracted_Historical_Entities.xlsx'

STEPS = [
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
    (ROOT / 'graph_builder.py', []),
]


def run_step(path, args, extra_env=None):
    print(f'--- START: {path.relative_to(ROOT)}')
    cmd = [PY, str(path)] + args
    try:
        env = os.environ.copy()
        if extra_env:
            env.update({k: str(v) for k, v in extra_env.items() if v is not None and str(v) != ''})
        subprocess.run(cmd, check=True, env=env)
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
    skip_hitl_pause = os.getenv('SKIP_HITL_PAUSE', '').strip().lower() in {'1', 'true', 'yes', 'on'}
    print('Pipeline orchestrator starting')
    for step in STEPS:
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
