#!/usr/bin/env python3
"""Orchestrator to run the pipeline in order, aborting on failure.

Sequence:
 1) upload_data.py
 2) scripts/pg_to_pg_with_tei.py
 3) scripts/tag_tei_with_gemma.py
 4) scripts/generate_cidoc_mappings.py
 5) graph_builder.py

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

STEPS = [
    (ROOT / 'upload_data.py', []),
    (ROOT / 'scripts' / 'pg_to_pg_with_tei.py', []),
    (ROOT / 'scripts' / 'tag_tei_with_dict.py', ['--all', '--limit', '0'], {
        'USE_PROCESS_POOL': os.getenv('USE_PROCESS_POOL', '1'),
        'MAX_WORKERS': os.getenv('MAX_WORKERS', str(max(4, (os.cpu_count() or 4)))),
        'CHUNK_SIZE': os.getenv('CHUNK_SIZE', '200'),
    }),
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


def main():
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
    print('Pipeline finished successfully')


if __name__ == '__main__':
    main()
