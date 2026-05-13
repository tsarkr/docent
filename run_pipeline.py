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
PY = sys.executable

STEPS = [
    (ROOT / 'upload_data.py', []),
    (ROOT / 'scripts' / 'pg_to_pg_with_tei.py', []),
    # tag_tei_with_gemma requires --input and --output; we'll determine defaults at runtime
    (ROOT / 'scripts' / 'tag_tei_with_gemma.py', None),
    (ROOT / 'scripts' / 'generate_cidoc_mappings.py', []),
    (ROOT / 'graph_builder.py', []),
]


def run_step(path, args):
    print(f'--- START: {path.relative_to(ROOT)}')
    cmd = [PY, str(path)] + args
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f'ERROR: step {path} failed with exit {e.returncode}')
        sys.exit(e.returncode)
    except FileNotFoundError:
        print(f'ERROR: script not found: {path}')
        sys.exit(1)
    print(f'--- DONE: {path.relative_to(ROOT)}')


def main():
    print('Pipeline orchestrator starting')
    for path, args in STEPS:
        if not path.exists():
            print(f'ERROR: required script missing: {path}')
            sys.exit(1)
        # Special-case tag_tei_with_gemma to provide input/output and DB table
        if path.name == 'tag_tei_with_gemma.py':
            # try to find a CSV in data/ to process (prefer data-*.csv)
            data_dir = ROOT / 'data'
            csv_candidates = list(data_dir.glob('*.csv'))
            input_file = None
            for c in csv_candidates:
                # skip already tagged files
                if c.name.endswith('.tagged.csv'):
                    continue
                # prefer the specific data file if present
                if c.name.startswith('data-'):
                    input_file = c
                    break
            if input_file is None and csv_candidates:
                # pick first non-tagged csv
                for c in csv_candidates:
                    if not c.name.endswith('.tagged.csv'):
                        input_file = c
                        break

            if input_file is None:
                print('ERROR: no input CSV found for tag_tei_with_gemma in data/'); sys.exit(1)

            output_file = input_file.with_name(input_file.stem + '.tagged.csv')
            # default DB table to update status for TEI refinement
            update_table = 'raw_source_info'
            step_args = [
                '--input', str(input_file),
                '--output', str(output_file),
                '--update-db-table', update_table,
            ]
            run_step(path, step_args)
        else:
            run_step(path, args or [])
    print('Pipeline finished successfully')


if __name__ == '__main__':
    main()
