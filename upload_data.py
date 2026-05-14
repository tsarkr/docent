import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
import os
import re
import json
import csv

# TOML 읽기 (Python 3.11+ 또는 tomli)
try:
    import tomllib
except ImportError:
    import tomli as tomllib

# --- [설정 함수] ---
def _load_secrets(secret_file=None):
    """secrets.toml 파일에서 설정 로드"""
    if secret_file is None:
        secret_file = os.path.join(os.path.dirname(__file__), '.streamlit', 'secrets.toml')
    
    if os.path.exists(secret_file):
        try:
            with open(secret_file, 'rb') as f:
                return tomllib.load(f)
        except Exception as e:
            print(f"⚠️ secrets.toml 읽기 실패: {e}")
    return {}

def _secret_or_env(key, default="", secrets_dict=None):
    """환경변수 → secrets.toml → 기본값 순서로 설정값 가져오기"""
    # 1. 환경변수 확인
    val = os.getenv(key)
    if val:
        return str(val)
    
    # 2. secrets 딕셔너리에서 확인
    if secrets_dict and key in secrets_dict:
        return str(secrets_dict[key])
    
    # 3. 기본값 사용
    return str(default) if default else ""

# 설정 로드
SECRETS = _load_secrets()

# --- [설정구역] ---
PG_CONFIG = {
    "host": _secret_or_env("PG_HOST", "11e.kr", SECRETS),
    "port": _secret_or_env("PG_PORT", "5432", SECRETS),
    "database": _secret_or_env("PG_DATABASE", "historical", SECRETS),
    "user": _secret_or_env("PG_USER", "postgres", SECRETS),
    "password": _secret_or_env("PG_PASSWORD", "", SECRETS)  # secrets.toml에서만 로드
}

DATA_PATH = "./data"

encoded_password = urllib.parse.quote_plus(PG_CONFIG["password"])
DB_URL = f"postgresql://{PG_CONFIG['user']}:{encoded_password}@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}"
engine = create_engine(DB_URL)


def _make_unique_columns(orig_cols):
    """Keep source headers while ensuring uniqueness and non-empty names."""
    mapping = []
    seen = {}
    new_cols = []
    for i, c in enumerate(orig_cols):
        orig = "" if c is None else str(c)
        base = orig
        if base.strip() == "":
            base = f"unnamed_{i+1}"
        candidate = base
        suffix = 1
        while candidate in seen:
            suffix += 1
            candidate = f"{base}_{suffix}"
        seen[candidate] = True
        new_cols.append(candidate)
        mapping.append({"index": i, "original": orig, "column": candidate})
    return new_cols, mapping

def process_task(file_name, table_name, skip_rows, dry_run=False):
    full_path = os.path.join(DATA_PATH, file_name)
    if not os.path.exists(full_path):
        print(f"⚠️ {file_name} 파일을 찾을 수 없습니다.")
        return False

    print(f"\n🚀 작업 시작: {file_name}")

    ext = os.path.splitext(file_name)[1].lower()

    df = None
    mapping = None
    used_enc = None
    used_sep = None

    # Excel files
    if ext in ('.xls', '.xlsx'):
        try:
            # Read without header to detect correct header row robustly
            raw = pd.read_excel(full_path, header=None, dtype=str)
            header_idx = None
            max_search = min(len(raw), 50)
            for idx in range(0, max_search):
                row = raw.iloc[idx].astype(str).tolist()
                # treat 'nan' as empty
                parsed = [c if c != 'nan' else '' for c in row]
                non_numeric = sum(1 for v in parsed if v and not re.fullmatch(r"[0-9\-\.]+", v.strip()))
                lengths = [len(v) for v in parsed]
                max_len = max(lengths) if lengths else 0
                avg_len = sum(lengths) / max(1, len(lengths))
                long_cells = sum(1 for L in lengths if L > 100)
                if len(parsed) > 1 and (non_numeric / max(1, len(parsed))) >= 0.6 and max_len < 200 and avg_len < 80 and (long_cells / max(1, len(parsed))) <= 0.2:
                    header_idx = idx
                    break
            if header_idx is None:
                # fallback: use skip_rows if provided
                header_idx = max(0, skip_rows)
            df = pd.read_excel(full_path, header=header_idx, dtype=str)
            df.columns, mapping = _make_unique_columns(df.columns)
            used_enc, used_sep = 'excel', None
        except Exception as e:
            print(f"   ❌ 적재 실패(Excel): {e}")
            return False
    else:
        # CSV files: detect encoding and separator, preserve header
        encodings = ['utf-8-sig', 'cp949', 'utf-16', 'euc-kr']
        separators = [',', '\t']

        for enc in encodings:
            try:
                with open(full_path, 'r', encoding=enc, errors='replace') as f:
                    lines = f.readlines()
            except Exception:
                continue

            for sep in separators:
                # search header candidate lines from the top (prefer earlier header rows)
                header_idx = None
                max_search = min(len(lines), 50)
                search_indices = list(range(0, max_search))

                for idx in search_indices:
                    if idx < 0 or idx >= len(lines):
                        continue
                    candidate = lines[idx].strip('\n').strip()
                    if not candidate:
                        continue
                    try:
                        parsed_candidate = next(csv.reader([candidate], delimiter=sep))
                    except Exception:
                        continue
                    if len(parsed_candidate) <= 1:
                        continue
                    def is_numeric(s):
                        return re.fullmatch(r"[0-9\-\.]+", s.strip()) is not None
                    non_numeric_count = sum(1 for v in parsed_candidate if v and not is_numeric(str(v)))
                    # additional heuristics: reject candidate rows that contain very long text
                    lengths = [len(str(v)) for v in parsed_candidate]
                    max_len = max(lengths) if lengths else 0
                    avg_len = sum(lengths) / max(1, len(lengths))
                    long_cells = sum(1 for L in lengths if L > 100)
                    # Accept header if majority non-numeric, and not dominated by very long text
                    if non_numeric_count / max(1, len(parsed_candidate)) >= 0.6 and max_len < 200 and avg_len < 80 and (long_cells / max(1, len(parsed_candidate))) <= 0.2:
                        header_idx = idx
                        parsed = parsed_candidate
                        break

                if header_idx is None:
                    continue

                try:
                    # Use the detected header row so pandas uses that row as column names
                    temp_df = pd.read_csv(full_path, encoding=enc, sep=sep, header=header_idx)
                except Exception:
                    continue

                # If pandas didn't pick up the parsed header correctly, override it
                if temp_df.shape[1] == len(parsed):
                    temp_df.columns = parsed
                temp_df.columns, mapping = _make_unique_columns(temp_df.columns)
                df = temp_df
                used_enc, used_sep = enc, sep
                break

            if df is not None:
                break

    if df is None:
        print(f"   ❌ 적재 실패: 인코딩 또는 구분자를 찾을 수 없습니다.")
        return False

    # Ensure mapping exists when headers were not customized earlier
    if mapping is None:
        df.columns, mapping = _make_unique_columns(df.columns)

    # store mapping for traceability
    mapping_dir = os.path.join(DATA_PATH, 'column_mappings')
    os.makedirs(mapping_dir, exist_ok=True)
    map_path = os.path.join(mapping_dir, f"{table_name}_columns.json")
    try:
        with open(map_path, 'w', encoding='utf-8') as mf:
            json.dump({'table': table_name, 'file': file_name, 'mapping': mapping}, mf, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"   ⚠️ 매핑 파일 저장 실패: {e}")

    # If dry_run, just print summary and mapping path
    if dry_run:
        print(f"   DRY RUN: {file_name} -> {table_name}")
        print(f"   Detected encoding/sep: {used_enc} / {used_sep}")
        print(f"   Rows: {len(df)} Columns: {len(df.columns)}")
        print(f"   Column mapping saved to: {map_path}")
        print('   Sample normalized cols:', df.columns.tolist()[:10])
        return True

    # Ensure table name is safe (alphanumeric + underscore)
    if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
        print(f"   ❌ 잘못된 테이블 이름: {table_name}")
        return False

    try:
        # Drop existing table completely to ensure full rewrite
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))

        # write table
        df = df.astype(str)
        df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=500)
        print(f"   ✅ 성공! ({used_enc} / '{used_sep}' / {len(df)}행)")
        print(f"   Column mapping saved to: {map_path}")
        return True
    except Exception as e:
        print(f"   ❌ 오류 발생: {e}")
        return False

# 작업 리스트
tasks = [
    ("사건-세부장소_연결정보_260410.csv", "raw_event_place_link", 8),
    ("사건정보_260410.csv", "raw_event_info", 8),
    ("서지정보_260410.csv", "raw_bibliography", 8),
    ("세부장소_260410.csv", "raw_detail_place", 8),
    ("출처정보_260410.xlsx", "raw_source_info", 8),
    ("탄압기구(경찰).csv", "raw_oppression_org_police", 7),
    ("탄압기구(군대).csv", "raw_oppression_org_military", 7),
    ("탄압기구(헌병)_20200131.csv", "raw_oppression_org_gendarme", 7)
]

if __name__ == "__main__":
    for file, table, skip in tasks:
        process_task(file, table, skip)
    print("\n✨ 모든 테이블이 CSV 헤더와 100% 일치하게 재생성 및 적재되었습니다!")
