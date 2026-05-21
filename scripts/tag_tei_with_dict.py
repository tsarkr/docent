import argparse
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from pathlib import Path
import tomllib
import re
import hanja  # 한자-한글 변환 라이브러리
from langchain_ollama import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate

class LocalDocentEngine:
    def __init__(self):
        self.secrets = self._load_secrets()
        self.conn = self._get_conn()
        self.llm = OllamaLLM(model="gemma4:31b-cloud")
        self.done_status = 'REFINED'

    def _load_secrets(self):
        path = Path(__file__).resolve().parent.parent / '.streamlit' / 'secrets.toml'
        with open(path, 'rb') as f: return tomllib.load(f)

    def _get_conn(self):
        return psycopg2.connect(
            host=self.secrets.get('PG_HOST'), user=self.secrets.get('PG_USER'),
            password=self.secrets.get('PG_PASSWORD'), dbname=self.secrets.get('PG_DATABASE')
        )

    def _table_has_column(self, cur, table_name, column_name):
        cur.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
            LIMIT 1
            """,
            (table_name, column_name),
        )
        return cur.fetchone() is not None

    def _ensure_tei_status_column(self, cur, table_name):
        """tei_status 컬럼이 없으면 강제로 생성합니다."""
        if not self._table_has_column(cur, table_name, 'tei_status'):
            cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN tei_status VARCHAR(50)')
            self.conn.commit()
            print(f"  [안내] {table_name} 테이블에 'tei_status' 컬럼을 신규 생성했습니다.")

    def _pre_tag_hanja(self, text):
        """이전 작업의 찌꺼기를 청소한 후, 한자 태깅을 100% 완벽하게 적용합니다."""
        # 1. 이전 작업에서 발생한 중복/오염된 term, foreign, gloss 태그 강제 삭제 (세탁)
        # 주의: <p>, <div>, <placeName> 등 다른 TEI 태그는 건드리지 않습니다.
        text = re.sub(r'</?(term|foreign|gloss)[^>]*>', '', text)
        
        # 2. 깨끗해진 텍스트에 한자 사전 태깅 적용
        def replace_hanja(match):
            word = match.group(0)
            reading = hanja.translate(word, 'substitution')
            return f'<term><foreign xml:lang="zh-Hani">{word}</foreign><gloss>{reading}</gloss></term>'
        
        # 한자(기본 영역)가 연속된 구간을 찾아 강제 변환
        return re.sub(r'[\u4e00-\u9fff]+', replace_hanja, text)

    def _sanitize_tei(self, xml_string):
        """LLM이 임의로 생성한 마크다운 껍데기와 환각 로그 방어"""
        xml_string = re.sub(r'^```[a-zA-Z]*\s*', '', xml_string.strip())
        xml_string = re.sub(r'\s*```$', '', xml_string)
        xml_string = re.sub(r'\$\.\.\.\s*\(wait,[^\)]+\)\s*\.\.\.', '', xml_string)
        
        # 잘못된 태그 닫힘 교정
        xml_string = xml_string.replace('</template>', '</term>')
        
        return xml_string

    def _ask_llm(self, item):
        table, rowid, tei, event_id = item
        try:
            # 1단계: 파이썬 정규식을 통한 오염 태그 세탁 및 한자 100% 완벽 태깅
            pre_tagged_tei = self._pre_tag_hanja(tei)

            # 2단계: LLM은 오직 인명(<persName>)만 찾도록 지시
            prompt = ChatPromptTemplate.from_template("""
                당신은 역사 문서 TEI 마크업 전문가입니다. 주어진 텍스트는 이미 한자 태깅이 완료된 상태입니다.
                당신의 유일한 임무는 원본 구조를 100% 유지하면서, 텍스트 내의 '사람 이름(인명)'에만 다음 태그를 추가하는 것입니다.
                
                인명 태그 규칙: <persName ref="#{event_id}_{{name}}">{{name}}</persName>
                
                [금지사항]
                - 기존에 있는 <term>, <foreign>, <gloss>, <p>, <placeName> 등의 태그를 절대 건드리지 마세요.
                - 마크다운이나 부가 설명을 넣지 말고 오직 완벽한 XML 결과만 출력하세요.
                
                원본 텍스트:
                {text}
            """)
            chain = prompt | self.llm
            result = chain.invoke({"text": pre_tagged_tei, "event_id": event_id})
            sanitized = self._sanitize_tei(result)
            return (item, sanitized, None)
        except Exception as e:
            return (item, None, str(e))

    def run_full_pipeline(self, limit=0):
        print("🚀 로컬 엔진 가동: 하이브리드(태그 세탁 + Hanja + LLM) 파이프라인 시작...")
        cur = self.conn.cursor()
        
        cur.execute("""
            SELECT table_name FROM information_schema.columns 
            WHERE column_name = 'tei' AND table_schema = 'public'
        """)
        tables = [row[0] for row in cur.fetchall()]

        for table in tables:
            print(f"\n📘 테이블 처리 중: {table}")
            
            # tei_status 컬럼 강제 생성
            self._ensure_tei_status_column(cur, table)
            
            has_event_id = self._table_has_column(cur, table, '사건아이디')

            where_clauses = ['tei IS NOT NULL', '(tei_status IS NULL OR tei_status != %s)']
            params = [self.done_status]

            select_event_id = '"사건아이디" AS event_id' if has_event_id else 'NULL AS event_id'
            
            query = f'SELECT rowid, tei, {select_event_id} FROM "{table}" WHERE ' + ' AND '.join(where_clauses) + ' ORDER BY rowid ASC'
            
            if limit > 0:
                query += ' LIMIT %s'
                params.append(limit)
            
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
            
            if not rows:
                continue

            tasks = []
            for rowid, tei, event_id in rows:
                event_ref = str(event_id).strip() if event_id else f"{table}_{rowid}"
                tasks.append((table, rowid, tei, event_ref))

            # 클라우드 연산을 활용하여 5배속 병렬 처리, 저장은 순차적으로 수행
            with ThreadPoolExecutor(max_workers=5) as executor:
                for original_item, new_tei, error in executor.map(self._ask_llm, tasks):
                    table, rowid, tei, event_ref = original_item
                    
                    if error:
                        print(f"  ⚠️ 에러 ({table} rowid={rowid}): {error}")
                        continue
                        
                    try:
                        cur.execute(f'UPDATE "{table}" SET tei = %s, tei_status = %s WHERE rowid = %s', 
                                   (new_tei, self.done_status, rowid))
                        self.conn.commit()
                        print(f"  ✅ 완료 및 저장됨: {table} rowid={rowid}")
                    except Exception as e:
                        self.conn.rollback()
                        print(f"  ⚠️ DB 저장 에러 ({table} rowid={rowid}): {e}")

        cur.close()
        self.conn.close()
        print("\n🎉 모든 작업 완료.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=0)
    parser.add_argument('--all', action='store_true')
    args = parser.parse_args()
    engine = LocalDocentEngine()
    engine.run_full_pipeline(limit=0 if args.all else args.limit)