import psycopg2
from pathlib import Path
import tomllib
import re
import csv
import sys
import signal

class TeiPersNameExtractor:
    def __init__(self):
        self.secrets = self._load_secrets()
        self.conn = self._get_conn()
        self.cur = self.conn.cursor()
        self.interrupted = False
        self.output_file = "extracted_persnames.csv"
        
        # 중간 정지 안전 장치
        signal.signal(signal.SIGINT, self._handle_signal)
        print("🚀 데이터베이스 전체 <persName> 태그 영역 정밀 추출 가동...")

    def _handle_signal(self, signum, frame):
        print("\n\n🛑 [인터럽트] 주인님, 추출 도중 Ctrl+C가 감지되었습니다. 현재까지의 데이터를 안전하게 저장하고 종료합니다...")
        self.interrupted = True

    def _load_secrets(self):
        path = Path(__file__).resolve().parent.parent / '.streamlit' / 'secrets.toml'
        with open(path, 'rb') as f: 
            return tomllib.load(f)

    def _get_conn(self):
        return psycopg2.connect(
            host=self.secrets.get('PG_HOST'),
            user=self.secrets.get('PG_USER'),
            password=self.secrets.get('PG_PASSWORD'),
            dbname=self.secrets.get('PG_DATABASE')
        )

    def execute_extraction(self):
        # 1. 'tei' 컬럼이 존재하는 public 스키마 내의 모든 테이블 수집
        self.cur.execute("""
            SELECT table_name FROM information_schema.columns 
            WHERE column_name = 'tei' AND table_schema = 'public'
        """)
        tables = [row[0] for row in self.cur.fetchall()]

        # 2. CSV 파일 오픈 (UTF-8-SIG 설정으로 엑셀 한글 깨짐 원천 방지)
        with open(self.output_file, mode='w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            # 주인님이 요청하신 컬럼 헤더 규격 작성
            writer.writerow(['테이블명', 'rowid', 'persname'])

            total_extracted = 0

            for table in tables:
                if self.interrupted:
                    break
                    
                print(f"📘 현재 추출 중인 테이블: {table}")
                
                # 대량 데이터 메모리 폭발 방지를 위해 서버 사이드 커서 선언 (버퍼 2000건 단위 추출)
                cursor_name = f"srv_cur_{table}"
                self.cur.execute(f'DECLARE {cursor_name} CURSOR FOR SELECT rowid, tei FROM "{table}" WHERE tei IS NOT NULL')
                
                while True:
                    if self.interrupted:
                        break
                        
                    self.cur.execute(f"FETCH 2000 FROM {cursor_name}")
                    rows = self.cur.fetchall()
                    if not rows:
                        break

                    for rowid, tei in rows:
                        # 본문 전체에서 <persName> 태그 영역만 글로벌 정규식으로 정밀 타격
                        # 예: <persName ref="#id_金">...</persName> 구조 전체 추출
                        matches = re.findall(r'<persName[^>]*>.*?</persName>', tei, flags=re.DOTALL)
                        
                        for match in matches:
                            # 다른 서술어나 태그 밖의 텍스트는 전부 제외하고 요청 포맷만 단정하게 누적
                            writer.writerow([table, rowid, match.strip()])
                            total_extracted += 1

                self.cur.execute(f"CLOSE {cursor_name}")
                self.conn.commit()

        self.cur.close()
        self.conn.close()
        
        if self.interrupted:
            print(f"\n👋 중간 정지 완료. 현재까지 {total_extracted}건의 인명 태그가 '{self.output_file}'에 저장되었습니다.\n")
        else:
            print(f"\n🎉 전수 정밀 추출 완수! 총 {total_extracted}건의 순수 <persName> 구역이 '{self.output_file}' 파일로 출력되었습니다.")

if __name__ == '__main__':
    extractor = TeiPersNameExtractor()
    extractor.execute_extraction()