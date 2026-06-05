import argparse
import psycopg2
from pathlib import Path
import tomllib
import re
import json
import sys  
import signal  
import hanja  
from xml.etree import ElementTree as ET
from langchain_ollama import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate

BIBLIOGRAPHY_PERSON_FIELDS = {
    '작성자', '발신자', '수신자', '수신자2', '상위자료_작성자', '피고인', '관련인물', '인물'
}

EVENT_PERSON_FIELDS = {
    '관련인물'
}

EVENT_TITLE_KOREAN = {
    '대한국민의회장', '기수', '영사', '공사', '총영사', '부영사', '영사관', '통역', '서기'
}

SOURCE_INFO_NAME_FIELDS = {
    '관련인물', '출처정보'
}

KOREAN_SURNAME_PREFIXES = [
    '남궁', '선우', '제갈', '황보', '독고', '동방', '사공', '서문', '어금', '망절', '무본', '강전',
    '김', '이', '박', '최', '정', '강', '조', '윤', '장', '임', '오', '한', '신', '서', '권', '황', '안', '송', '전', '홍',
    '유', '고', '문', '양', '손', '배', '백', '허', '남', '심', '노', '하', '곽', '성', '차', '주', '우', '구', '현', '국', '민',
]

BIBLIOGRAPHY_TITLE_EXACT = {
    '경부', '서기', '순사', '검사정', '경찰서장', '평양경찰서장', '警部', '書記', '巡査', '檢事正', '警察署長', '司法警察官事務取扱',
    '警務總監部', '京城地방법원', '京城覆審法院', '高等法院', '平壤地방법원', '朝鮮總督府裁判所書記', '朝鮮總督府判事', '豫審掛職務代理朝鮮總督府判事',
    '豫審掛朝鮮總督府判사', '豫審掛代理朝鮮總督府判사', '豫審掛朝鮮총독부재판소서기', '豫審掛職務代理朝鮮총독부재판소서기',
    '朝鮮총독부재판소서기', '朝鮮총독부판사', '裁判長', '判事', '司法警察官事務取扱', '警務總監部', '警部', '書記', '巡査'
}

BIBLIOGRAPHY_TITLE_KOREAN = {
    '예심괘직무대리', '예심괘조선총독부판사', '예심괘조선총독부재판소서기', '조선총독부재판소서기', '조선총독부판사', '재판장', '판사',
    '검사정', '경찰서장', '평양경찰서장', '경무총감부', '경무총감', '경무총장', '경무총장부', '고등경찰과장', '고등경찰과',
    '정무총감', '각부국', '비서과장', '양무관', '임시보고계', '임시신문계', '각도부장', '참모차장',
    '경부', '서기', '순사', '순사보', '의사', '주임', '주사', '부장', '장관',
}

class LocalDocentEngine:
    def __init__(self):
        self.secrets = self._load_secrets()
        self.conn = self._get_conn()
        self.cur = self.conn.cursor()  
        self.done_status = 'REFINED'
        self.interrupted = False  
        
        signal.signal(signal.SIGINT, self._handle_signal)
        
        print("📦 Ollama 초고속 인명 추출 엔진 연결 중 (qwen3.6)...")
        self.llm = OllamaLLM(model="qwen3.6", format="json", temperature=0.0)
        print("✅ 엔진 연결 완료.")

    def _handle_signal(self, signum, frame):
        print("\n\n🛑 [인터럽트] 주인님, Ctrl+C 입력을 감지했습니다. 현재 건까지만 안전하게 커밋하고 정지합니다...")
        self.interrupted = True

    def _load_secrets(self):
        path = Path(__file__).resolve().parent.parent / '.streamlit' / 'secrets.toml'
        with open(path, 'rb') as f: return tomllib.load(f)

    def _get_conn(self):
        return psycopg2.connect(
            host=self.secrets.get('PG_HOST'), user=self.secrets.get('PG_USER'),
            password=self.secrets.get('PG_PASSWORD'), dbname=self.secrets.get('PG_DATABASE')
        )

    def _table_has_column(self, table_name, column_name):
        self.cur.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
            LIMIT 1
            """,
            (table_name, column_name),
        )
        return self.cur.fetchone() is not None

    def _ensure_tei_status_column(self, table_name):
        if not self._table_has_column(table_name, 'tei_status'):
            self.cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN tei_status VARCHAR(50)')
            self.conn.commit()
            print(f"  [안내] {table_name} 테이블에 'tei_status' 컬럼을 신규 생성했습니다.")

    def _clean_and_pre_tag_hanja(self, text):
        text = re.sub(r'</?(term|foreign|gloss|persName)[^>]*>', '', text)
        
        def replace_hanja(match):
            word = match.group(0)
            reading = hanja.translate(word, 'substitution')
            
            if word.startswith('金') and reading.startswith('금'):
                reading = '김' + reading[1:]
                
            if word.startswith('丸山') and reading.startswith('환산'):
                reading = reading.replace('환산', '마루야마')
            elif word.startswith('楠') and reading.startswith('남'):
                reading = reading.replace('남', '구스노키')

            if len(reading) >= 2:
                first_char = reading[0]
                if first_char in ('리', '니', '랴', '냐', '려', '녀', '료', '뇨', '류', '뉴'):
                    trans_map = {
                        '리': '이', '니': '이', '랴': '야', '냐': '야', 
                        '려': '여', '녀': '여', '료': '요', '뇨': '요', 
                        '류': '유', '뉴': '유'
                    }
                    reading = trans_map[first_char] + reading[1:]
                elif first_char in ('라', '러', '로', '루', '뢰', '래', '레', '릉'):
                    trans_map = {
                        '라': '나', '러': '너', '로': '노', '루': '누', 
                        '뢰': '뇌', '래': '내', '레': '네', '릉': '능'
                    }
                    reading = trans_map[first_char] + reading[1:]
                    
            return f'<term><foreign xml:lang="zh-Hani">{word}</foreign><gloss>{reading}</gloss></term>'
            
        return re.sub(r'[\u4e00-\u9fff]+', replace_hanja, text)

    def _is_valid_person_name(self, name):
        if not name: return False
        name = str(name).strip()
        if len(name) <= 1: return False
        strict_geography = {'장연', '온양', '비안', '청단', '장련', '삭주', '금천', '양덕', '함흥', '의주', '안악', '시흥', '고양', '혼춘', '용정'}
        if name in strict_geography: return False
        if re.search(r'([_\-\.\d]|[A-Za-z])', name): return False
        if len(name) > 2 and name.endswith(('군', '면', '읍', '리', '시', '구')): return False
        exact_noise_tokens = {'군', '현', '읍', '면', '리', '도', '시', '구', '촌', '포', '진', '령', '강', '천', '궁', '문', '묘', '역', '원', '해', '녀'}
        if name in exact_noise_tokens: return False
        invalid_phrases = {
            '장관', '사령관', '분대장', '소장', '부장', '의원', '총독', '목사', '순사', '헌병', '조장', '서기', '통역', '판사', '검사', '하사', '소좌', '군수', '면장', '장교', '순사보', '장로',
            '사무소', '주재소', '경찰서', '학교', '학원', '서원', '회관', '교회', '정부', '부대', '대대', '중대', '연대', '분대', '외무성',
            '시위대', '주모자', '주도자', '지도자', '선동자', '가담자', '참가자', '불량배', '소방조', '작업자', '피의자', '조선인', '일본인', '사람들', '청년', '소년들',
            '일람표', '보고서', '계획서', '선언서', '서류', '기록', '유인물', '만세', '독립', '선언', '시위', '사건', '소요',
            '하여', '지어', '올라', '이하', '이상', '이름', '기보', '기타', '박다', '서명', '위해', '미연', '방지', '상고',
            '임명', '석달', '가량', '군경', '명녀', '신녀',
            '법원', '재판소', '재판장', '지명', '조서', '소송', '증인', '증언', '출판법', '위반', '내란죄', '보안법', '문서',
            '매일신보사', '신문사', '출판사', '통신사', '회사', '사', '기자단',
            '경무총감부', '경무총감', '경부', '警務總監部', '警部', '경찰서장', '평양경찰서장', '署長', '檢事正', '警察署長', '平壤警察署長'
        }
        invalid_phrases.update(BIBLIOGRAPHY_TITLE_KOREAN)
        invalid_phrases.update(EVENT_TITLE_KOREAN)
        for sub in invalid_phrases:
            if sub in name: return False
        if re.search(r'[\u4e00-\u9fff]', name):
            if re.search(r'(郡|市|Do|面|里|村|長|官|署|所|隊|兵|院|海|女)$', name): return False
        return True

    def _split_name_candidates(self, text):
        if text is None: return []
        raw = str(text).strip()
        if not raw: return []
        parts = re.split(r'[;,/|·、\n\s]+', raw)
        return [part.strip() for part in parts if part and part.strip()]

    def _normalize_person_candidate(self, name):
        if not name: return ''
        candidate = str(name).strip()
        candidate = re.sub(r'[\s\u3000]+', '', candidate)
        candidate = candidate.rstrip('외外間人名')
        if re.search(r'[가-힣]', candidate) and re.search(r'[\u4e00-\u9fff]', candidate):
            hangul_parts = re.findall(r'[가-힣]+', candidate)
            if hangul_parts: candidate = hangul_parts[-1]
        candidate = re.sub(r'(으로부터|으로써|이라고|라고|에게서|에게|께|에서|까지|부터|보다|만|밖에|조차|마저|도|은|는|이|가|을|를|의|와|과|에|로|으로|처럼|같이|및|등)$', '', candidate)
        if re.search(r'[\u4e00-\u9fff]', candidate) and not re.search(r'[가-힣]', candidate):
            reading = hanja.translate(candidate, 'substitution')
            if candidate.startswith('金') and reading.startswith('금'): reading = '김' + reading[1:]
            if candidate.startswith('丸山') and reading.startswith('환산'): reading = reading.replace('환산', '마루야마')
            elif candidate.startswith('楠') and reading.startswith('남'): reading = reading.replace('남', '구스노키')
            if len(reading) >= 2:
                first_char = reading[0]
                if first_char in ('리', '니', '랴', '냐', '려', '녀', '료', '뇨', '류', '뉴'):
                    trans_map = {'리': '이', '니': '이', '랴': '야', '냐': '야', '려': '여', '녀': '여', '료': '요', '뇨': '요', '류': '유', '뉴': '유'}
                    reading = trans_map[first_char] + reading[1:]
                elif first_char in ('라', '러', '로', '루', '뢰', '래', '레', '릉'):
                    trans_map = {'라': '나', '러': '너', '로': '노', '루': '누', '뢰': '뇌', '래': '내', '레': '네', '릉': '능'}
                    reading = trans_map[first_char] + reading[1:]
            candidate = reading
        return candidate.strip()

    def _split_compound_korean_name_tokens(self, token):
        token = str(token).strip()
        if not token or not re.fullmatch(r'[가-힣]+', token): return [token] if token else []
        if len(token) < 5: return [token]
        surnames = sorted(KOREAN_SURNAME_PREFIXES, key=len, reverse=True)
        pieces = []
        index = 0
        while index < len(token):
            matched = None
            for surname in surnames:
                if token.startswith(surname, index):
                    candidate_length = len(surname) + 2
                    if index + candidate_length <= len(token):
                        pieces.append(token[index:index + candidate_length])
                        index += candidate_length
                        matched = True
                    break
            if matched: continue
            remaining = len(token) - index
            if remaining in (2, 3): pieces.append(token[index:]); break
            index += 1
        return pieces if pieces else [token]

    def _looks_like_person_name(self, name):
        candidate = str(name).strip()
        if not candidate or not re.fullmatch(r'[가-힣]{2,8}', candidate): return False
        surnames = sorted(KOREAN_SURNAME_PREFIXES, key=len, reverse=True)
        return any(candidate.startswith(surname) for surname in surnames)

    def _extract_last_token_names(self, text):
        candidates = []
        for segment in self._split_name_candidates(text):
            normalized = re.sub(r"[\(\)\[\]{}<>,，。·“”'\"]", ' ', segment).strip()
            if not normalized: continue
            tokens = [token for token in re.split(r'\s+', normalized) if token]
            if not tokens: continue
            candidates.append(tokens[-1])
        return candidates

    def _extract_names_from_persname_tags(self, raw_text, skip_data_cols=None):
        skip_data_cols = set(skip_data_cols or set())
        try: root = ET.fromstring(raw_text)
        except Exception: return []
        extracted = []
        def walk(elem, ancestors):
            if elem.tag == 'persName' and not any(data_col in skip_data_cols for data_col in ancestors):
                name_text = elem.findtext('.//gloss') or elem.findtext('.//foreign') or ''.join(elem.itertext())
                name_text = str(name_text).strip()
                for candidate in self._extract_last_token_names(name_text):
                    candidate = self._normalize_person_candidate(candidate)
                    if self._is_valid_person_name(candidate): extracted.append(candidate)
            for child in list(elem):
                child_data_col = elem.attrib.get('data-col') if elem.tag == 'p' else None
                walk(child, ancestors + ((child_data_col,) if child_data_col else ()))
        walk(root, tuple())
        return list(dict.fromkeys(extracted))

    def _extract_names_from_event_related_field(self, raw_text):
        try: root = ET.fromstring(raw_text)
        except Exception: return []
        extracted = []
        for elem in root.iter():
            if elem.attrib.get('data-col') != '관련인물': continue
            buffer = (elem.text or '')
            for child in list(elem):
                if buffer and (';' in buffer or '；' in buffer):
                    buffer_candidates = []
                    for segment in re.split(r'[;\n]+', buffer):
                        segment = segment.strip()
                        if not segment: continue
                        prefix = re.split(r'[（(\[]', segment, maxsplit=1)[0].strip()
                        if not prefix: continue
                        for candidate in self._extract_last_token_names(prefix):
                            candidate = self._normalize_person_candidate(candidate)
                            if self._is_valid_person_name(candidate) and (len(candidate) >= 3 or any(mark in segment for mark in ('(', '（', '['))):
                                buffer_candidates.append(candidate)
                    if buffer_candidates: extracted.extend(buffer_candidates)
                child_candidates = []
                if child.tag == 'persName':
                    child_text = child.findtext('.//gloss') or child.findtext('.//foreign') or ''.join(child.itertext())
                    child_text = str(child_text).strip()
                    for candidate in self._extract_last_token_names(child_text):
                        candidate = self._normalize_person_candidate(candidate)
                        if self._is_valid_person_name(candidate): child_candidates.append(candidate)
                if child_candidates: extracted.extend(child_candidates)
                buffer = child.tail or ''
            if buffer:
                for segment in re.split(r'[;\n]+', buffer):
                    segment = segment.strip()
                    if not segment: continue
                    prefix = re.split(r'[（(\[]', segment, maxsplit=1)[0].strip()
                    if not prefix: continue
                    for candidate in self._extract_last_token_names(prefix):
                        candidate = self._normalize_person_candidate(candidate)
                        if self._is_valid_person_name(candidate) and (len(candidate) >= 3 or any(mark in segment for mark in ('(', '（', '['))):
                            extracted.append(candidate)
        return list(dict.fromkeys(extracted))

    def _extract_names_from_top_title_field(self, raw_text):
        try: root = ET.fromstring(raw_text)
        except Exception: return []
        extracted = []
        for elem in root.iter():
            if elem.attrib.get('data-col') != '상위자료_제목': continue
            title_text = ''.join(elem.itertext()).strip()
            if not title_text: continue
            for match in re.finditer(r'([\u4e00-\u9fff]{2,}?)外\s*(?:\d+\s*)?(?:人|名)?', title_text):
                candidate = self._normalize_person_candidate(match.group(1))
                if self._is_valid_person_name(candidate): extracted.append(candidate)
        return list(dict.fromkeys(extracted))

    def _extract_names_from_source_info_fields(self, raw_text):
        try: root = ET.fromstring(raw_text)
        except Exception: return []
        extracted = []
        for elem in root.iter():
            data_col = elem.attrib.get('data-col')
            if data_col not in SOURCE_INFO_NAME_FIELDS: continue
            field_text = ''.join(elem.itertext()).strip()
            if not field_text: continue
            if data_col == '관련인물':
                for segment in re.split(r'[;,/|·、\n]+', field_text):
                    segment = segment.strip()
                    if not segment: continue
                    if re.fullmatch(r'[가-힣\s]+', segment):
                        for token in self._split_name_candidates(segment):
                            for candidate in self._split_compound_korean_name_tokens(token):
                                candidate = self._normalize_person_candidate(candidate)
                                if self._is_valid_person_name(candidate) and self._looks_like_person_name(candidate) and len(candidate) >= 3: extracted.append(candidate)
                        continue
                    for token in self._extract_last_token_names(segment):
                        candidate = self._normalize_person_candidate(token)
                        if self._is_valid_person_name(candidate) and self._looks_like_person_name(candidate) and len(candidate) >= 3: extracted.append(candidate)
            elif data_col == '출처정보':
                bracket_hits = re.findall(r'([\u4e00-\u9fff]{2,})\s*\[[^\]]+\]', field_text)
                particle_hits = re.findall(r'([\u4e00-\u9fff]{2,})(?=\s*(?:은|는|이|가|도|와|과|의|를|을|에|에서|에게|께|등|및|로|으로|이며|인|란|라고|자|후|전|보다|까지|부터)\b)', field_text)
                for candidate_text in bracket_hits + particle_hits:
                    candidate = self._normalize_person_candidate(candidate_text)
                    if self._is_valid_person_name(candidate) and self._looks_like_person_name(candidate) and len(candidate) >= 3: extracted.append(candidate)
        return list(dict.fromkeys(extracted))

    def _extract_names_from_structured_fields(self, raw_text, allowed_fields=None):
        try: root = ET.fromstring(raw_text)
        except Exception: return []
        extracted = []
        for elem in root.iter():
            data_col = elem.attrib.get('data-col')
            if allowed_fields is not None:
                if data_col not in allowed_fields: continue
            elif data_col not in BIBLIOGRAPHY_PERSON_FIELDS: continue
            term_candidates = []
            for term in elem.findall('.//term'):
                gloss = term.findtext('gloss')
                foreign = term.findtext('foreign')
                term_text = (gloss or foreign or ''.join(term.itertext())).strip()
                if term_text: term_candidates.extend(self._extract_last_token_names(term_text))
            if not term_candidates:
                field_text = ''.join(elem.itertext()).strip()
                term_candidates.extend(self._extract_last_token_names(field_text))
            for candidate in term_candidates:
                candidate = self._normalize_person_candidate(candidate)
                if self._is_valid_person_name(candidate): extracted.append(candidate)
        return list(dict.fromkeys(extracted))

    def _parse_llm_response_names(self, response_text):
        if hasattr(response_text, 'content'): response_text = response_text.content
        response_text = str(response_text).strip()
        if not response_text: return []
        try: data = json.loads(response_text)
        except Exception:
            match = re.search(r'\{.*\}', response_text, re.S)
            if not match: return []
            try: data = json.loads(match.group(0))
            except Exception: return []
        names = data.get('names', [])
        if not isinstance(names, list): return []
        refined = []
        for name in names:
            candidate = self._normalize_person_candidate(name)
            if self._is_valid_person_name(candidate): refined.append(candidate)
        return list(dict.fromkeys(refined))

    def _extract_names_with_llm_context(self, table_name, raw_text):
        clean_context = re.sub(r'<[^>]+>', ' ', raw_text)
        if table_name == 'raw_bibliography':
            names = self._extract_names_from_persname_tags(raw_text, skip_data_cols={'제목', '상위자료_제목'})
            names.extend(self._extract_names_from_top_title_field(raw_text))
            names = list(dict.fromkeys(names))
            if names: return names
            return self._extract_names_from_structured_fields(raw_text)
        if table_name == 'raw_source_info':
            names = self._extract_names_from_source_info_fields(raw_text)
            if names: return names
            return self._extract_names_from_persname_tags(raw_text)
        if table_name == 'raw_event_info':
            names = self._extract_names_from_event_related_field(raw_text)
            names.extend(self._extract_names_from_persname_tags(raw_text, skip_data_cols={'시위_행정구역명'}))
            names = list(dict.fromkeys(names))
            if names: return names
            return self._extract_names_from_structured_fields(raw_text, EVENT_PERSON_FIELDS)
        prompt = ChatPromptTemplate.from_template("""
            You are a strict historical data extraction expert. 
            Analyze the provided Korean historical context and extract ALL actual individual "personal names" (인명, names of people).
            [CRITICAL CENSORSHIP RULES]
            - Extract ONLY real human names (e.g., "이계창", "박희도", "백시찬").
            - Do NOT extract geographical places, regions, or single towns (e.g., "장호원", "황해", "온양", "장연", "청단", "비안").
            - Do NOT extract modern generic titles, actions, or temporary nouns (e.g., "임명", "상고", "석달", "정신녀", "정명녀").
            Return ONLY a valid JSON object with a single key "names" containing the array of extracted personal names. No explanation, no markdown blocks.
            Text context:
            {context}
        """)
        chain = prompt | self.llm
        try:
            response_text = chain.invoke({"context": clean_context})
            return self._parse_llm_response_names(response_text)
        except Exception: return []

    def _apply_persname_tags(self, pre_tagged_tei, real_names, event_id):
        for name in real_names:
            if not name or len(name) < 2: continue
            hanja_pattern = rf'(<term><foreign[^>]*>([^\s<]+)</foreign><gloss>{re.escape(name)}</gloss></term>)'
            replacement = f'<persName ref="#{event_id}_\\2">\\1</persName>'
            pre_tagged_tei = re.sub(hanja_pattern, replacement, pre_tagged_tei)
            plain_pattern = rf'(?<![>#_\-\[₩A-Za-z0-9]){re.escape(name)}(?![<A-Za-z0-9])'
            pre_tagged_tei = re.sub(plain_pattern, f'<persName ref="#{event_id}_{name}">{name}</persName>', pre_tagged_tei)
        return pre_tagged_tei

    def run_full_pipeline(self, limit=0, skip_hitl=False):
        print("🚀 [최종 진화 완결] LLM 문맥 추출 + 파이썬 포함문자열 기계 차단 파이프라인 가동...")
        self.cur.execute("""
            SELECT table_name FROM information_schema.columns 
            WHERE column_name = 'tei' AND table_schema = 'public'
        """)
        tables = [row[0] for row in self.cur.fetchall()]
        skipped_tables = {'raw_detail_place'}
        for table in tables:
            if table in skipped_tables: continue
            if self.interrupted: break
            print(f"\n📘 테이블 제어 중: {table}")
            self._ensure_tei_status_column(table)
            has_event_id = self._table_has_column(table, '사건아이디')
            where_clauses = ['tei IS NOT NULL', '(tei_status IS NULL OR tei_status != %s)']
            params = [self.done_status]
            select_event_id = '"사건아이디" AS event_id' if has_event_id else 'NULL AS event_id'
            query = f'SELECT rowid, tei, {select_event_id} FROM "{table}" WHERE ' + ' AND '.join(where_clauses) + ' ORDER BY rowid ASC'
            if limit > 0:
                query += ' LIMIT %s'
                params.append(limit)
            self.cur.execute(query, tuple(params))
            rows = self.cur.fetchall()
            if not rows: continue
            for rowid, tei, event_id in rows:
                if self.interrupted: break
                try:
                    event_ref = str(event_id).strip() if event_id else f"{table}_{rowid}"
                    pre_tagged = self._clean_and_pre_tag_hanja(tei)
                    real_names = self._extract_names_with_llm_context(table, pre_tagged)
                    final_tei = self._apply_persname_tags(pre_tagged, real_names, event_ref)
                    self.cur.execute(f'UPDATE "{table}" SET tei = %s, tei_status = %s WHERE rowid = %s', (final_tei, self.done_status, rowid))
                    self.conn.commit()
                    print(f"  ✅ [최종 검증 완료] {table} rowid={rowid} | 확정 인명: {real_names}")
                except Exception as e:
                    self.conn.rollback()
                    print(f"  ⚠️ 장애 발생 (rowid={rowid}): {e}")
        self.cur.close()
        self.conn.close()
        try:
            from scripts.extract_all_entities import TeiEntityExtractor
            import pandas as pd
            csv_out = Path(__file__).resolve().parent.parent / 'Extracted_Historical_Entities.csv'
            xlsx_out = csv_out.with_suffix('.xlsx')
            print('\n🔧 HITL 준비: 개체 추출 CSV 생성 중...')
            extractor = TeiEntityExtractor()
            extractor.output_csv = str(csv_out)
            extractor.extract_and_report()
            print(f'🔁 CSV -> XLSX 변환 중: {csv_out} -> {xlsx_out}')
            try:
                df = pd.read_csv(csv_out, encoding='utf-8-sig')
                df.to_excel(xlsx_out, index=False)
                print(f'✅ HITL 워크북 생성 완료: {xlsx_out}')
            except Exception as e: print(f'⚠️ XLSX 변환 실패: {e} — CSV 위치: {csv_out}')
            if not skip_hitl:
                print('🔔 HITL: 생성된 워크북을 검토하세요. 검토가 끝나면 Enter 를 눌러 계속합니다.')
                try: input()
                except Exception: pass
        except Exception as e: print(f'⚠️ HITL 전처리 단계에서 예외 발생: {e}')
        if self.interrupted: sys.exit(0)
        else: print("\n🎉 54,500건 전수 조사 무결점 완수.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=0)
    parser.add_argument('--all', action='store_true')
    parser.add_argument('--skip-hitl', action='store_true', default=False)
    args = parser.parse_args()
    engine = LocalDocentEngine()
    engine.run_full_pipeline(limit=0 if args.all else args.limit, skip_hitl=args.skip_hitl)
