#!/usr/bin/env python3
"""Neo4j cleanup utility.
- 기본: 레이블별 노드 수를 조회(dry-run)
- `--confirm` 옵션을 주면 Person/Place/Event 레이블의 모든 노드를 삭제(DETACH DELETE)

Uses .streamlit/secrets.toml or environment variables NEO4J_URI/NEO4J_USER/NEO4J_PASSWORD.
"""
import os, sys, argparse
try:
    import tomllib
except Exception:
    import tomli as tomllib

def _load_secrets(secret_file=None):
    root = os.getcwd()
    if secret_file is None:
        secret_file = os.path.join(root, '.streamlit', 'secrets.toml')
    if os.path.exists(secret_file):
        try:
            with open(secret_file, 'rb') as f:
                return tomllib.load(f)
        except Exception:
            return {}
    return {}

def _secret_or_env(key, default='', secrets_dict=None):
    val = os.getenv(key)
    if val:
        return str(val)
    if secrets_dict and key in secrets_dict:
        return str(secrets_dict[key])
    return str(default) if default else ''

SECRETS = _load_secrets()

NEO_URI = _secret_or_env('NEO4J_URI', 'bolt://localhost:7687', SECRETS)
NEO_USER = _secret_or_env('NEO4J_USER', 'neo4j', SECRETS)
NEO_PW = _secret_or_env('NEO4J_PASSWORD', 'password', SECRETS)

def main(confirm=False):
    try:
        from neo4j import GraphDatabase
    except Exception:
        print('neo4j driver not installed. Install with pip install neo4j')
        return 1

    try:
        driver = GraphDatabase.driver(NEO_URI, auth=(NEO_USER, NEO_PW))
    except Exception as e:
        print('Failed to create Neo4j driver:', e)
        print('Check .streamlit/secrets.toml or environment variables for NEO4J_URI/NEO4J_USER/NEO4J_PASSWORD')
        return 1

    labels = ('Person', 'Place', 'Event')
    try:
        with driver.session() as session:
            for label in labels:
                try:
                    res = session.run(f"MATCH (n:{label}) RETURN count(n) AS c")
                    cnt = list(res)[0]['c'] if res is not None else 0
                except Exception as e:
                    print(f'Could not count nodes for label {label}: {e}')
                    cnt = 'unknown'
                print(f'{label}: {cnt} nodes')

            if not confirm:
                print('\nDry-run: no nodes were deleted. Use --confirm to delete.')
                return 0

            print('\nConfirm flag set: deleting nodes...')
            for label in labels:
                try:
                    session.run(f"MATCH (n:{label}) DETACH DELETE n")
                    print(f'Deleted nodes with label {label}')
                except Exception as e:
                    print(f'Failed to delete nodes with label {label}: {e}')
            print('Deletion complete.')
    except Exception as e:
        print('Session error:', e)
        return 1
    return 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Neo4j cleanup (dry-run by default).')
    parser.add_argument('--confirm', action='store_true', help='Actually delete nodes')
    args = parser.parse_args()
    sys.exit(main(confirm=args.confirm))
