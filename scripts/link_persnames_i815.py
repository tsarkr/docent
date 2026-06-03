#!/usr/bin/env python3
"""
Find 3-char Korean name candidates from Extracted_Historical_Entities.csv,
search the i815 인명사전 API, and if a candidate yields exactly one match, wrap occurrences
in TEI with <persName ref="...">...</persName> across all raw_* tables.

Usage:
  python scripts/link_persnames_i815.py --csv Extracted_Historical_Entities.csv --dry-run
  python scripts/link_persnames_i815.py --csv Extracted_Historical_Entities.csv --apply
"""

import argparse
import csv
import json
import logging
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import requests
import tomllib
import psycopg2
import xml.etree.ElementTree as ET

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

I815_API = "https://search.i815.or.kr/openApiData.do"
I815_BASE = "https://search.i815.or.kr"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
CACHE_PATH = DATA_DIR / 'i815_person_index.json'
SUMMARY_PATH = DATA_DIR / 'persname_match_summary.csv'
SECRETS_PATH = PROJECT_ROOT / '.streamlit' / 'secrets.toml'
I815_TIMEOUT = 30
I815_RETRIES = 3


def is_three_hangul(name: str) -> bool:
    """Check if the given string consists of exactly 3 Hangul syllables."""
    s = re.sub(r'[\s\(\)\[\]"\']', '', name.strip())
    hangul = [ch for ch in s if '\uAC00' <= ch <= '\uD7A3']
    return len(hangul) == 3 and len(s) == len(hangul)


def extract_candidates(csv_path: Path) -> List[str]:
    """Extract distinct 3-character Hangul names labeled as persName or term from CSV."""
    names = set()
    with csv_path.open('r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            tag_class = (row.get('태그_유형(Class)') or '').strip()
            if tag_class not in ('persName', 'term'):
                continue
            gloss = (row.get('한글_독음(Gloss)') or '').strip()
            if gloss:
                token = gloss.split()[0]
                if is_three_hangul(token):
                    names.add(token)
    return sorted(names)


def fetch_i815_page(page: int, timeout: int = I815_TIMEOUT) -> str:
    """Fetch a single page of results from the i815 API with retry logic."""
    params = {'type': '4', 'page': str(page)}
    last_error = None
    for attempt in range(1, I815_RETRIES + 1):
        try:
            r = requests.get(I815_API, params=params, timeout=timeout)
            r.raise_for_status()
            return r.text
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt < I815_RETRIES:
                sleep_seconds = attempt * 2
                logger.warning(f"i815 page {page} request failed (attempt {attempt}/{I815_RETRIES}): {exc}; retrying in {sleep_seconds}s")
                time.sleep(sleep_seconds)
            else:
                logger.error(f"Failed to fetch i815 page {page} after {I815_RETRIES} attempts.")
                raise last_error
    raise last_error


def parse_i815_items(xml_text: str) -> Tuple[int, int, List[Dict[str, str]]]:
    """Parse the i815 XML response and return total count, page count, and item list."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.error(f"XML parse error: {e}")
        return 0, 0, []

    def text_of(parent, tag):
        node = parent.find(tag)
        return (node.text or '').strip() if node is not None and node.text else ''

    items = []
    for item in root.findall('.//item'):
        name = text_of(item, 'name')
        aliases = text_of(item, 'aliases')
        name_hanja = text_of(item, 'nameHanja')
        detail_url = text_of(item, 'url') or text_of(item, 'link') or ''
        if not detail_url:
            detail_url = f"{I815_BASE}/dictionary/main.do?searchKeyword={requests.utils.quote(name)}"
        
        items.append({
            'name': name,
            'aliases': aliases,
            'name_hanja': name_hanja,
            'url': detail_url,
        })

    total_count = int((root.findtext('.//total_count') or '0').strip() or 0)
    page_count = int((root.findtext('.//page_count') or '0').strip() or 0)
    return total_count, page_count, items


def build_i815_index(limit_pages: int = 0) -> Tuple[List[Dict[str, str]], bool, int]:
    """Fetch all pages from the i815 API to build a complete index."""
    index = []
    page = 1
    total_pages = None
    
    while True:
        try:
            xml_text = fetch_i815_page(page)
        except Exception:
            break

        total_count, page_count, items = parse_i815_items(xml_text)
        if total_pages is None:
            total_pages = page_count
            logger.info(f"Loading i815 index: {total_count} records across {page_count} pages")
            
        if not items:
            break
            
        index.extend(items)
        if limit_pages and page >= limit_pages:
            break
        if total_pages is not None and page >= total_pages:
            break
        page += 1
        
    complete = total_pages is not None and (not limit_pages or page >= total_pages)
    return index, complete, total_pages or 0


def load_i815_index_cache(cache_path: Path = CACHE_PATH) -> Optional[List[Dict[str, str]]]:
    """Load the i815 index from local JSON cache if it exists and is complete."""
    if not cache_path.exists():
        return None
    try:
        with cache_path.open('r', encoding='utf-8') as f:
            payload = json.load(f)
        if payload.get('complete', False) and isinstance(payload.get('items'), list):
            return payload['items']
    except Exception as e:
        logger.warning(f"Failed to load cache from {cache_path}: {e}")
    return None


def save_i815_index_cache(index: List[Dict[str, str]], cache_path: Path = CACHE_PATH, complete: bool = False, total_pages: int = 0):
    """Save the i815 index to a local JSON cache file."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'item_count': len(index),
        'items': index,
        'complete': complete,
        'total_pages': total_pages,
    }
    with cache_path.open('w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def build_search_map(index: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    """Optimizes searching by pre-mapping all names and aliases to their records for O(1) lookups."""
    search_map = defaultdict(list)
    seen_map = defaultdict(set)

    for item in index:
        primary = (item.get('name') or '').strip()
        alias_text = item.get('aliases') or ''
        aliases = [a.strip() for a in re.split(r'[;,/|⋮·\s]+', alias_text) if a.strip()]
        
        keys = set(aliases)
        if primary:
            keys.add(primary)
            
        record_id = item.get('url', '')
        for key in keys:
            if record_id not in seen_map[key]:
                search_map[key].append(item)
                seen_map[key].add(record_id)
                
    return dict(search_map)


def load_secrets() -> Dict[str, str]:
    """Load database credentials from streamlit secrets.toml."""
    if not SECRETS_PATH.exists():
        raise FileNotFoundError(f"Secrets file not found at {SECRETS_PATH}")
    with open(SECRETS_PATH, 'rb') as f:
        return tomllib.load(f)


def get_db_connection(secrets: Dict[str, str]):
    """Establish connection to PostgreSQL."""
    return psycopg2.connect(
        host=secrets.get('PG_HOST'), 
        user=secrets.get('PG_USER'),
        password=secrets.get('PG_PASSWORD'), 
        dbname=secrets.get('PG_DATABASE')
    )


def find_raw_tables(cur) -> List[str]:
    """Find all relevant raw_* tables containing a 'tei' column."""
    cur.execute("""
        SELECT table_name FROM information_schema.columns
        WHERE column_name = 'tei' 
          AND table_schema = 'public' 
          AND table_name LIKE 'raw_%' 
          AND table_name NOT LIKE '%_bak'
        GROUP BY table_name
    """)
    return [r[0] for r in cur.fetchall()]


def wrap_name_in_tei(tei_text: str, name: str, url: str) -> str:
    """
    Safely wraps a `name` inside TEI text with <persName ref="...">...</persName>.
    It prevents wrapping names that are already within an existing <persName> block.
    """
    if not tei_text or name not in tei_text:
        return tei_text

    # Split into list of XML tags and text nodes
    tokens = re.split(r'(<[^>]+>)', tei_text)
    result = []
    in_persname = 0
    pattern = re.compile(rf'\b{re.escape(name)}\b')

    for token in tokens:
        if token.startswith('<') and token.endswith('>'):
            # XML Tag
            if token.startswith('<persName') and not token.endswith('/>'):
                in_persname += 1
            elif token.startswith('</persName>'):
                in_persname = max(0, in_persname - 1)
            result.append(token)
        else:
            # Text Node
            if in_persname > 0:
                result.append(token)
            else:
                new_token = pattern.sub(f'<persName ref="{url}">{name}</persName>', token)
                result.append(new_token)
                
    return "".join(result)


def apply_updates(conn, cur, tables: List[str], name: str, url: str, dry_run: bool = True) -> int:
    """Apply DB updates for a matched candidate name."""
    total_updates = 0
    for table in tables:
        # Pre-filter rows that contain the name
        cur.execute(f"SELECT rowid, tei FROM \"{table}\" WHERE tei ILIKE %s", (f'%{name}%',))
        rows = cur.fetchall()
        
        for rowid, tei in rows:
            tei_str = tei or ''
            new_tei = wrap_name_in_tei(tei_str, name, url)
            
            if new_tei != tei_str:
                total_updates += 1
                if dry_run:
                    logger.info(f"[DRY RUN] Would update {table} (rowid={rowid}) for name '{name}'")
                else:
                    cur.execute(f"UPDATE \"{table}\" SET tei = %s WHERE rowid = %s", (new_tei, rowid))
                    
    if not dry_run and total_updates > 0:
        conn.commit()
        
    return total_updates


def save_summary(results: List[Tuple[str, List[Dict[str, str]]]]):
    """Save match results summary to a CSV file."""
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with SUMMARY_PATH.open('w', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f)
        w.writerow(['name', 'num_matches', 'matches'])
        for name, matches in results:
            match_desc = ';'.join([f"{m.get('name', '')}|{m.get('url', '')}" for m in matches])
            w.writerow([name, len(matches), match_desc])
    logger.info(f"Summary saved to {SUMMARY_PATH}")


def main():
    p = argparse.ArgumentParser(description="Link 3-char Korean person names to i815 and wrap TEI")
    p.add_argument('--csv', default=str(PROJECT_ROOT / 'Extracted_Historical_Entities.csv'), help="Path to input CSV")
    p.add_argument('--dry-run', action='store_true', default=False, help="Preview DB updates without committing")
    p.add_argument('--apply', action='store_true', default=False, help="Apply DB updates (must be explicit)")
    p.add_argument('--limit', type=int, default=0, help="Limit number of candidates to process (0 = all)")
    p.add_argument('--index-pages', type=int, default=0, help="Limit i815 API pages to load (0 = all pages)")
    p.add_argument('--refresh-index', action='store_true', default=False, help="Ignore cache and rebuild i815 index")
    args = p.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        logger.error(f"CSV not found: {csv_path}. Please generate it first by running scripts/extract_all_entities.py")
        sys.exit(1)

    candidates = extract_candidates(csv_path)
    if args.limit and args.limit > 0:
        candidates = candidates[:args.limit]
    logger.info(f"Found {len(candidates)} distinct 3-char candidates")

    try:
        secrets = load_secrets()
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)

    # Initialize i815 index
    index = None
    if not args.refresh_index:
        index = load_i815_index_cache()
        if index is not None:
            logger.info(f"Loaded cached i815 index: {len(index)} person records")

    if index is None:
        logger.info("Building local i815 person index...")
        index, complete, total_pages = build_i815_index(limit_pages=args.index_pages)
        logger.info(f"Index size: {len(index)} person records")
        if complete:
            save_i815_index_cache(index, complete=True, total_pages=total_pages)
            logger.info(f"Saved i815 index cache to {CACHE_PATH}")
        else:
            logger.info("Index is partial; cache not saved")

    # Build O(1) search map for fast lookups
    search_map = build_search_map(index)

    # Database updates
    dry_run = not args.apply or args.dry_run
    if dry_run:
        logger.info("Running in DRY-RUN mode. Database will not be modified.")
    else:
        logger.warning("Running in APPLY mode. Database will be modified.")

    results = []
    
    try:
        with get_db_connection(secrets) as conn:
            with conn.cursor() as cur:
                tables = find_raw_tables(cur)
                logger.info(f"Found {len(tables)} raw tables to process.")

                for name in candidates:
                    matches = search_map.get(name, [])
                    logger.info(f"'{name}': {len(matches)} matches")
                    
                    if len(matches) == 1 and args.apply:
                        url = matches[0]['url']
                        updates = apply_updates(conn, cur, tables, name, url, dry_run=dry_run)
                        logger.info(f"Applied updates for '{name}': {updates} rows modified")
                    elif len(matches) == 1:
                        logger.info(f"Single match for '{name}': {matches[0]['name']} ({matches[0]['url']}) (run with --apply to commit updates)")
                    else:
                        logger.debug(f"Multiple/no matches for '{name}'; skipping auto-update")
                        
                    results.append((name, matches))
    except psycopg2.Error as e:
        logger.error(f"Database error occurred: {e}")
        sys.exit(1)

    save_summary(results)


if __name__ == '__main__':
    main()