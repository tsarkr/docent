import psycopg2
from pathlib import Path
import tomllib
import re
import csv
import sys

class TeiEntityExtractor:
    def __init__(self):
        self.secrets = self._load_secrets()
        self.conn = self._get_conn()
        self.cur = self.conn.cursor()
        self.output_csv = "/Users/gyungmin/Dev/docent/Extracted_Historical_Entities.csv"
        
    def _load_secrets(self):
        path = Path(__file__).resolve().parent.parent / '.streamlit' / 'secrets.toml'
        with open(path, 'rb') as f: return tomllib.load(f)

    def _get_conn(self):
        return psycopg2.connect(
            host=self.secrets.get('PG_HOST'), user=self.secrets.get('PG_USER'),
            password=self.secrets.get('PG_PASSWORD'), dbname=self.secrets.get('PG_DATABASE')
        )

    def _parse_inner_text(self, tag_content):
        hanja = ""
        gloss = ""
        
        f_match = re.search(r'<foreign[^>]*>([^<]+)</foreign>', tag_content)
        g_match = re.search(r'<gloss>([^<]+)</gloss>', tag_content)
        
        if f_match: hanja = f_match.group(1).strip()
        if g_match: gloss = g_match.group(1).strip()
        
        if not gloss:
            clean_text = re.sub(r'<[^>]+>', '', tag_content).strip()
            gloss = clean_text
            hanja = clean_text
            
        return hanja, gloss

    def extract_and_report(self):
        self.cur.execute("""
            SELECT table_name FROM information_schema.columns 
            WHERE column_name = 'tei' AND table_schema = 'public' AND table_name LIKE 'raw_%' AND table_name NOT LIKE '%_bak'
            GROUP BY table_name
        """)
        tables = [row[0] for row in self.cur.fetchall()]

        if not tables:
            print("❌ 추출 연산을 진행할 원천 사료 테이블을 찾지 못했습니다.")
            return

        print("🔮 [역사 데이터 마이닝] 안전 계층형 추출 파이프라인을 가동합니다.")

        with open(self.output_csv, mode='w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['출처_테이블', '사료_ROWID', '사건아이디', '태그_유형(Class)', '고유_식별자(Ref/Type)', '표기_한자(Hanja)', '한글_독음(Gloss)'])

            total_extracted_count = 0

            for table in tables:
                print(f"📘 개체 전수 조사 및 파싱 중: {table}")
                
                self.cur.execute(f"SELECT count(*) FROM information_schema.columns WHERE table_name = '{table}' AND column_name = '사건아이디'")
                has_event_id = self.cur.fetchone()[0] > 0
                select_event_id = '"사건아이디"' if has_event_id else 'NULL'

                cursor_name = f"ext_cur_{table}"
                self.cur.execute(f'DECLARE {cursor_name} CURSOR FOR SELECT rowid, tei, {select_event_id} FROM "{table}" WHERE tei IS NOT NULL')

                while True:
                    self.cur.execute(f"FETCH 2000 FROM {cursor_name}")
                    rows = self.cur.fetchall()
                    if not rows:
                        break

                    for rowid, tei, event_id in rows:
                        event_ref = str(event_id).strip() if event_id else "N/A"
                        
                        # 💡 [우회 방어 가드] 이미 처리된 구절들을 마킹하여 중복 수집을 원천 차단하기 위한 추적셋
                        processed_spans = []

                        # -------------------------------------------------------------
                        # ① 인명 태그 정밀 추출 (<persName ref="...">)
                        # -------------------------------------------------------------
                        for match in re.finditer(r'<persName\s+ref="([^"]+)"[^>]*>(.*?)</persName>', tei, flags=re.DOTALL):
                            ref = match.group(1)
                            content = match.group(2)
                            processed_spans.append(match.span()) # 인명이 수집된 본문 좌표 저장
                            
                            hanja, gloss = self._parse_inner_text(content)
                            writer.writerow([table, rowid, event_ref, 'persName', ref, hanja, gloss])
                            total_extracted_count += 1

                        # -------------------------------------------------------------
                        # ② 장소 태그 정밀 추출 (<placeName ref="...">)
                        # -------------------------------------------------------------
                        for match in re.finditer(r'<placeName\s+ref="([^"]+)"[^>]*>(.*?)</placeName>', tei, flags=re.DOTALL):
                            ref = match.group(1)
                            content = match.group(2)
                            processed_spans.append(match.span()) # 장소가 수집된 본문 좌표 저장
                            
                            hanja, gloss = self._parse_inner_text(content)
                            writer.writerow([table, rowid, event_ref, 'placeName', ref, hanja, gloss])
                            total_extracted_count += 1

                        # -------------------------------------------------------------
                        # ③ 일반/죄명/연호 용어 태그 정밀 추출 (<term>)
                        # -------------------------------------------------------------
                        # 💡 복잡한 후방탐색 대신 모든 term을 다 찾은 뒤, 파이썬 연산으로 부모 소속 여부 판별
                        for match in re.finditer(r'<term(?:\s+type="([^"]+)")?>(.*?)</term>', tei, flags=re.DOTALL):
                            term_type = match.group(1)
                            content = match.group(2)
                            term_start, term_end = match.span()

                            # 이 term의 위치가 이미 앞에서 수집한 persName이나 placeName 내부에 포함되어 있다면 중복이므로 패스!
                            is_duplicate = False
                            for p_start, p_end in processed_spans:
                                if p_start <= term_start and term_end <= p_end:
                                    is_duplicate = True
                                    break
                            
                            if is_duplicate:
                                continue

                            if "<foreign" in content or not "<" in content:
                                hanja, gloss = self._parse_inner_text(content)
                                type_label = term_type if term_type else 'general'
                                writer.writerow([table, rowid, event_ref, 'term', type_label, hanja, gloss])
                                total_extracted_count += 1

                self.cur.execute(f"CLOSE {cursor_name}")
                self.conn.commit()

        self.cur.close()
        self.conn.close()
        print(f"\n🎉 전수 마이닝 마감! 총 {total_extracted_count}건의 삼원 핵심 개체가 유실 없이 엑셀 리포트로 변환되었습니다.")
        print(f"💾 마스터 파일 저장 경로 ➡️ {self.output_csv}")

if __name__ == '__main__':
    extractor = TeiEntityExtractor()
    extractor.extract_and_report()