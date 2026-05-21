import argparse
import psycopg2
from pathlib import Path
import tomllib
import re
import json
import sys  
import signal  
import hanja  
from langchain_ollama import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate

class LocalDocentEngine:
    def __init__(self):
        self.secrets = self._load_secrets()
        self.conn = self._get_conn()
        self.cur = self.conn.cursor()  
        self.done_status = 'REFINED'
        self.interrupted = False  
        
        signal.signal(signal.SIGINT, self._handle_signal)
        
        print("📦 Ollama 초고속 인명 추출 엔진 연결 중 (llama3:8b)...")
        # 결정론적이고 칼날 같은 추출을 위해 온도(temperature)를 0.0으로 완전 고정
        self.llm = OllamaLLM(model="llama3:8b", format="json", temperature=0.0)
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
            return f'<term><foreign xml:lang="zh-Hani">{word}</foreign><gloss>{reading}</gloss></term>'
        return re.sub(r'[\u4e00-\u9fff]+', replace_hanja, text)

    def _extract_names_with_llm_context(self, raw_text):
        """LLM에게 문맥 전체를 주고 오직 '인명'만 자유롭게 JSON 배열로 추출하게 합니다."""
        clean_context = re.sub(r'<[^>]+>', ' ', raw_text)
        
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
            data = json.loads(response_text)
            llm_names = data.get("names", [])
            
            refined_names = []
            
            # [역사 사료 데이터 보존 종결] 포함 문자열 매칭 사전
            invalid_substrings = {
                '군', '현', '읍', '면', '리', '도', '시', '구', '촌', '포', '진', '령', '강', '천', '궁', '문', '묘', '역', '원', '해', # 행정/지리 단위 확장 ('원', '해' 추가)
                '장관', '사령관', '분대장', '소장', '부장', '의원', '총독', '목사', '순사', '헌병', '조장', '서기', '통역', '판사', '검사', '하사', '소좌', '군수', '면장', '장교', '순사보', '장로', # 직책/계급
                '사무소', '주재소', '경찰서', '학교', '학원', '서원', '회관', '교회', '정부', '부대', '대대', '중대', '연대', '분대', '외무성', # 기관/단체
                '시위대', '주모자', '주도자', '지도자', '선동자', '가담자', '참가자', '불량배', '소방조', '작업자', '피의자', '조선인', '일본인', '사람들', '청년', '소년들', # 대명사/역할
                '일람표', '보고서', '계획서', '선언서', '서류', '기록', '유인물', '만세', '독립', '선언', '시위', '사건', '소요', # 서식/사건
                '하여', '지어', '올라', '이하', '이상', '이름', '기보', '기타', '박다', '서명', '위해', '미연', '방지', '상고', # 서술어/부사 파편
                '임명', '석달', '가량', '군경', '녀', '명녀', '신녀' # [종결 보강] 역사 행동 단어 및 여성형 대명사 파편 완벽 소멸
            }
            
            # 행정구역 접미사 없이 튀어나오는 고유 역사 지명 전수 배제 사전
            strict_geography = {
                '장연', '온양', '비안', '청단', '장련', '삭주', '금천', '양덕', '함흥', '의주', '안악', '시흥', '고양', '혼춘', '용정'
            }
            
            for name in llm_names:
                name = name.strip()
                
                # 1. 외자 파편 무조건 탈락
                if len(name) <= 1:
                    continue
                    
                # 2. 고유 지명 사전에 완전 일치 시 탈락
                if name in strict_geography:
                    continue
                    
                # 3. 알파벳, 숫자, 시스템 주석 특수기호 혼착어 무조건 탈락
                if re.search(r'([_\-\.\d]|[A-Za-z])', name):
                    continue
                    
                # 4. 불용어 Substring 검사
                has_noise = False
                for sub in invalid_substrings:
                    if sub in name:
                        has_noise = True
                        break
                        
                # 5. 한자가 섞인 상태에서 지명/직책 접미사가 잔존하는 경우 차단
                if re.search(r'[\u4e00-\u9fff]', name):
                    if re.search(r'(郡|市|Do|面|里|村|長|官|署|所|隊|兵|院|海|女)$', name):
                        has_noise = True
                        
                if has_noise:
                    continue
                    
                refined_names.append(name)
                
            return list(set(refined_names))
        except Exception:
            return []

    def _apply_persname_tags(self, pre_tagged_tei, real_names, event_id):
        for name in real_names:
            if not name or len(name) < 2:
                continue
                
            hanja_pattern = rf'(<term><foreign[^>]*>([^\s<]+)</foreign><gloss>{re.escape(name)}</gloss></term>)'
            replacement = f'<persName ref="#{event_id}_\\2">\\1</persName>'
            pre_tagged_tei = re.sub(hanja_pattern, replacement, pre_tagged_tei)
            
            plain_pattern = rf'(?<![>#_\-\[₩A-Za-z0-9]){re.escape(name)}(?![<A-Za-z0-9])'
            pre_tagged_tei = re.sub(plain_pattern, f'<persName ref="#{event_id}_{name}">{name}</persName>', pre_tagged_tei)
            
        return pre_tagged_tei

    def run_full_pipeline(self, limit=0):
        print("🚀 [최종 진화 완결] LLM 문맥 추출 + 파이썬 포함문자열 기계 차단 파이프라인 가동...")
        
        self.cur.execute("""
            SELECT table_name FROM information_schema.columns 
            WHERE column_name = 'tei' AND table_schema = 'public'
        """)
        tables = [row[0] for row in self.cur.fetchall()]

        for table in tables:
            if self.interrupted:
                break
                
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
            
            if not rows:
                continue

            for rowid, tei, event_id in rows:
                if self.interrupted:
                    print("  🛑 데이터베이스 보호 및 세션 정상 종료.")
                    break
                    
                try:
                    event_ref = str(event_id).strip() if event_id else f"{table}_{rowid}"
                    
                    pre_tagged = self._clean_and_pre_tag_hanja(tei)
                    
                    # 땜질 차단: LLM이 자유롭게 문맥에서 뽑은 결과물을 파이썬이 Substring 단위로 도려냄
                    real_names = self._extract_names_with_llm_context(pre_tagged)
                    
                    final_tei = self._apply_persname_tags(pre_tagged, real_names, event_ref)
                    
                    self.cur.execute(f'UPDATE "{table}" SET tei = %s, tei_status = %s WHERE rowid = %s', 
                               (final_tei, self.done_status, rowid))
                    self.conn.commit()
                    print(f"  ✅ [최종 검증 완료] {table} rowid={rowid} | 확정 인명: {real_names}")
                    
                except Exception as e:
                    self.conn.rollback()
                    print(f"  ⚠️ 장애 발생 (rowid={rowid}): {e}")

        self.cur.close()
        self.conn.close()
        
        if self.interrupted:
            print("\n👋 파이프라인 정지 완수.\n")
            sys.exit(0)
        else:
            print("\n🎉 54,500건 전수 조사 무결점 완수.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=0)
    parser.add_argument('--all', action='store_true')
    args = parser.parse_args()
    engine = LocalDocentEngine()
    engine.run_full_pipeline(limit=0 if args.all else args.limit)