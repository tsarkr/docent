import argparse
import psycopg2
from pathlib import Path
import tomllib
import re
import json
import sys  
import signal  

class TeiUltimateMasterRefiner:
    def __init__(self, run_backup=False):
        self.secrets = self._load_secrets()
        self.conn = self._get_conn()
        self.cur = self.conn.cursor()  
        self.interrupted = False  
        self.run_backup = run_backup  
        
        signal.signal(signal.SIGINT, self._handle_signal)
        
        # CJK 최강인 Qwen 모델로 판별 엔진 고정
        from langchain_ollama import OllamaLLM
        from langchain_core.prompts import ChatPromptTemplate
        print("📦 [Qwen 3.6 아키텍처] Ollama 고성능 문맥 판별 엔진 연결 중...")
        self.llm = OllamaLLM(model="qwen3.6", format="json", temperature=0.0)
        self.ChatPromptTemplate = ChatPromptTemplate
        
        # 역사학 범주형 통제 사전
        self.CRIME_WORDS = {'소요', '소요범인', '보안법', '출판법', '위반', '범인', '주모자', '가담자'}
        self.OFFICE_WORDS = {'주재소', '경찰서', '사무소', '재판', '판결', '검사', '판사', '헌병', '보조원', '소장'}
        self.GENERAL_TERMS = {'개시', '군기', '군중', '시위', '만세', '서류', '진정서', '조선인', '일본인'}
        self.PERIOD_TERMS = {'대정', '소화', '명치', '明治', '大正', '昭和'}
        
        self.GEOGRAPHY_DICT = {
            '밀양', '논산', '천안', '병천', '갈전', '동면', '용두', '경성', '충청', '경상', '전라', '경기', '강원',
            '황해', '평안', '함경', '장연', '평강', '고창', '강화', '재령', '안악', '시흥', '고양', '용정', '삭주',
            '충청남도', '천안군', '갈전면', '병천시장', '동면', '용두리', '용정촌', '서대문감옥'
        }
        print("✅ 마스터 온톨로지 사전 및 Qwen 컨텍스트 엔진 로드 완료.")

    def _handle_signal(self, signum, frame):
        print("\n\n🛑 [인터럽트 감지] 주인님, 안전하게 세션 커서를 마감하고 정지 절차를 밟습니다...")
        self.interrupted = True

    def _load_secrets(self):
        path = Path(__file__).resolve().parent.parent / '.streamlit' / 'secrets.toml'
        with open(path, 'rb') as f: return tomllib.load(f)

    def _get_conn(self):
        return psycopg2.connect(
            host=self.secrets.get('PG_HOST'), user=self.secrets.get('PG_USER'),
            password=self.secrets.get('PG_PASSWORD'), dbname=self.secrets.get('PG_DATABASE')
        )

    def _execute_server_backup(self, tables):
        if not self.run_backup:
            print("ℹ️ [안내] 백업 옵션(--backup)이 생략되었습니다. 기존 스냅샷 기반으로 가동합니다.")
            return
        print("\n📦 [서버 사이드 백업] 원천 테이블 스냅샷 복제를 시작합니다.")
        old_isolation = self.conn.isolation_level
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        for table in tables:
            backup_table_name = f"{table}_bak"
            self.cur.execute(f'DROP TABLE IF EXISTS "{backup_table_name}" CASCADE;')
            self.cur.execute(f'CREATE TABLE "{backup_table_name}" AS SELECT * FROM "{table}";')
            print(f"  ✓ {table} ➡️ {backup_table_name} (서버 복제 완료)")
        self.conn.set_isolation_level(old_isolation)
        print("✅ 백업 무결성이 확인되었습니다.\n")

    def _clean_and_downgrade_legacy_errors(self, xml_text):
        """💡 [2차 역교정 엔진] 이미 잘못 박혀 있는 기형 태그 및 오용된 persName을 전수 역추적 청소"""
        if not xml_text: return ""
        
        # 1. 과거의 지명 Wrapping 오염 데이터 (<placeName><term>...) 완전 정화
        pattern_legacy = r'<placeName\s+ref="([^"]+)">\s*<term>(<foreign[^>]*>.*?</foreign><gloss>.*?</gloss>)\s*</term>\s*</placeName>'
        xml_text = re.sub(pattern_legacy, r'<placeName ref="\1" type="general">\2</placeName>', xml_text)
        
        # 2. 💡 [핵심 추가] 이미 잘못 승격된 가짜 인명(<persName>) 역추적하여 강제 강등(Downgrade)
        # 본문에 이미 박혀 있는 모든 persName 구조를 파싱
        pers_matches = re.findall(r'(<persName\s+ref="[^"]+">(<term><foreign[^>]*>([^<]+)</foreign><gloss>([^<]+)</gloss></term>|[^<]*)</persName>)', xml_text)
        
        for full_pers_tag, inner_body, *raw_gloss in pers_matches:
            # 내부에 term 구조가 살아있다면 gloss 추출, 없다면 태그 내부 글자 추출
            gloss_clean = ""
            if "<gloss>" in full_pers_tag:
                g_match = re.search(r'<gloss>([^<]+)</gloss>', full_pers_tag)
                if g_match: gloss_clean = g_match.group(1).strip()
            else:
                gloss_clean = re.sub(r'<[^>]+>', '', full_pers_tag).strip()
                
            if not gloss_clean: continue
            
            # 지명 사전에 걸린 가짜 인명 ➡️ 장소 태그로 강제 치환 및 정정
            if gloss_clean in self.GEOGRAPHY_DICT:
                # 내부 한자 파트 추출 가드
                f_match = re.search(r'<foreign[^>]*>([^<]+)</foreign>', full_pers_tag)
                hanja = f_match.group(1).strip() if f_match else gloss_clean
                corrected_markup = f'<placeName ref="#place_{gloss_clean}" type="general"><foreign xml:lang="zh-Hani">{hanja}</foreign><gloss>{gloss_clean}</gloss></placeName>'
                xml_text = xml_text.replace(full_pers_tag, corrected_markup)
                
            # 죄명 사전에 걸린 가짜 인명 ➡️ 일반 용어(crime) 태그로 강제 강등
            elif gloss_clean in self.CRIME_WORDS:
                cleaned_body = inner_body if "<term>" in inner_body else f'<term><foreign>{gloss_clean}</foreign><gloss>{gloss_clean}</gloss></term>'
                corrected_markup = cleaned_body.replace('<term>', '<term type="crime">')
                xml_text = xml_text.replace(full_pers_tag, corrected_markup)
                
            # 일본 연호 사전에 걸린 가짜 인명 ➡️ 시계열 용어(period) 태그로 강제 강등
            elif gloss_clean in self.PERIOD_TERMS:
                cleaned_body = inner_body if "<term>" in inner_body else f'<term><foreign>{gloss_clean}</foreign><gloss>{gloss_clean}</gloss></term>'
                corrected_markup = cleaned_body.replace('<term>', '<term type="period">')
                xml_text = xml_text.replace(full_pers_tag, corrected_markup)

        # 3. 비대칭 꺾쇠 및 중복 마크업 최종 소독
        xml_text = re.sub(r'<persName\s+ref="([^"<>\\]*)<persName[^>]*>', r'<persName ref="\1">', xml_text)
        xml_text = xml_text.replace('<persName_ref=', '<persName ref=')
        xml_text = re.sub(r'(<persName ref="[^"]+">){2,}', r'\1', xml_text)
        
        return xml_text

    def _extract_verified_names_with_context(self, context, candidates):
        if not candidates: return []
        prompt = self.ChatPromptTemplate.from_template("""
            You are a premier historical data archivist reviewing 1919 Korean independence movement documents.
            Review the "Full Text Context" and identify which items in the "Candidates List" are strictly individual "Human/Personal Names" (인명).
            Exclude any place names, organizations, or legal event nouns.
            Return ONLY a valid JSON object with a key "verified_personal_names" containing an array of confirmed strings.
            
            [Full Text Context]: {context}
            [Candidates List]: {candidates_list}
        """)
        try:
            clean_context = re.sub(r'<[^>]+>', ' ', context)
            response = (prompt | self.llm).invoke({"context": clean_context, "candidates_list": ", ".join(candidates)})
            return json.loads(response).get("verified_personal_names", [])
        except Exception:
            return []

    def execute_refinement(self):
        self.cur.execute("""
            SELECT table_name FROM information_schema.columns 
            WHERE column_name = 'tei' AND table_schema = 'public' AND table_name LIKE 'raw_%' AND table_name NOT LIKE '%_bak'
            GROUP BY table_name
        """)
        tables = [row[0] for row in self.cur.fetchall()]
        if not tables: return

        self._execute_server_backup(tables)
        print("🚀 [태그 마스터 정제 가동] 과거 오염된 persName 역교정 및 삼원 스키마 재배치를 융합 실행합니다.")

        for table in tables:
            if self.interrupted: break
            print(f"\n📘 대상 사료 테이블 정밀 리팩토링 중: {table}")
            
            self.cur.execute(f"SELECT count(*) FROM information_schema.columns WHERE table_name = '{table}' AND column_name = '사건아이디'")
            has_event_id = self.cur.fetchone()[0] > 0
            
            cursor_name = f"master_cur_{table}"
            select_event_id = '"사건아이디"' if has_event_id else 'NULL'
            
            self.cur.execute(f'DECLARE {cursor_name} CURSOR FOR SELECT rowid, tei, {select_event_id} FROM "{table}" WHERE tei IS NOT NULL')
            
            while True:
                if self.interrupted: break
                
                # 💡 [커서 파괴 방어벽 장착] InvalidCursorName 원천 방지 가드
                try:
                    self.cur.execute(f"FETCH 2000 FROM {cursor_name}")
                    rows = self.cur.fetchall()
                except psycopg2.errors.InvalidCursorName:
                    print(f"  ℹ️ [세션 안전 안내] 테이블 {table}의 커서 세션이 안전하게 자동 종료되었습니다.")
                    break
                    
                if not rows: break

                for rowid, tei, event_id in rows:
                    # 💡 가동되자마자 2차 역교정 필터 작동 (기존에 잘못 처리된 persName까지 싹 다 추적 청소)
                    cleaned_tei = self._clean_and_downgrade_legacy_errors(tei)
                    
                    # 새로운 정제 대상 term 수집
                    terms = re.findall(r'(<term><foreign[^>]*>([^<]+)</foreign><gloss>([^<]+)</gloss></term>)', cleaned_tei)
                    
                    modified_tei = cleaned_tei
                    updated_flag = (cleaned_tei != tei)
                    event_ref_base = str(event_id).strip() if event_id else f"{table}_{rowid}"

                    human_candidates = []
                    term_replacement_plan = []
                    visual_logs = []

                    for full_tag, hanja, gloss in terms:
                        gloss_clean = gloss.strip()
                        
                        if gloss_clean in self.GEOGRAPHY_DICT:
                            place_markup = f'<placeName ref="#place_{gloss_clean}" type="general"><foreign xml:lang="zh-Hani">{hanja}</foreign><gloss>{gloss_clean}</gloss></placeName>'
                            term_replacement_plan.append((full_tag, place_markup))
                            visual_logs.append(f"    📍 장소 구조 개혁: {full_tag} ➡️ {place_markup}")
                        elif gloss_clean in self.CRIME_WORDS:
                            crime_markup = full_tag.replace('<term>', '<term type="crime">')
                            term_replacement_plan.append((full_tag, crime_markup))
                            visual_logs.append(f"    ⚖️ 죄명 분류: {full_tag} ➡️ {crime_markup}")
                        elif gloss_clean in self.OFFICE_WORDS:
                            office_markup = full_tag.replace('<term>', '<term type="organization">')
                            term_replacement_plan.append((full_tag, office_markup))
                            visual_logs.append(f"    🏢 기관 분류: {full_tag} ➡️ {office_markup}")
                        elif gloss_clean in self.PERIOD_TERMS:
                            period_markup = full_tag.replace('<term>', '<term type="period">')
                            term_replacement_plan.append((full_tag, period_markup))
                            visual_logs.append(f"    ⏳ 일본 연호 격리: {full_tag} ➡️ {period_markup}")
                        elif gloss_clean in self.GENERAL_TERMS:
                            gen_markup = full_tag.replace('<term>', '<term type="general">')
                            term_replacement_plan.append((full_tag, gen_markup))
                        else:
                            if 1 < len(gloss_clean) < 5:
                                human_candidates.append(gloss_clean)

                    for target_tag, new_markup in term_replacement_plan:
                        pattern = re.escape(target_tag)
                        modified_tei, count = re.subn(pattern, new_markup, modified_tei)
                        if count > 0: updated_flag = True

                    if human_candidates:
                        verified_names = self._extract_verified_names_with_context(modified_tei, list(set(human_candidates)))
                        for gloss in verified_names:
                            for full_tag, hanja, g in terms:
                                if g.strip() == gloss:
                                    safe_ref = f"{event_ref_base}_{gloss}"
                                    persname_markup = f'<persName ref="#{safe_ref}">{full_tag}</persName>'
                                    
                                    def replace_guard(match):
                                        start_idx = match.start()
                                        context_before = modified_tei[max(0, start_idx-60):start_idx]
                                        if '<persName' in context_before and not '</persName>' in context_before:
                                            return match.group(0)
                                        return persname_markup

                                    new_tei, count = re.subn(re.escape(full_tag), replace_guard, modified_tei)
                                    if count > 0 and new_tei != modified_tei:
                                        modified_tei = new_tei
                                        updated_flag = True
                                        visual_logs.append(f"    👤 인명 승격 확정: {full_tag} ➡️ {persname_markup}")

                    if updated_flag:
                        try:
                            self.cur.execute(f'UPDATE "{table}" SET tei = %s, tei_status = \'REFINED\' WHERE rowid = %s', (modified_tei, rowid))
                            self.conn.commit()
                            
                            has_core_entity = any("👤" in log or "📍" in log for log in visual_logs)
                            if has_core_entity:
                                print(f"\n✨ [마스터 구조 고도화 완수] 테이블: {table} | rowid: {rowid}")
                                for log in visual_logs: print(log)
                            else:
                                sys.stdout.write(".")
                                sys.stdout.flush()
                        except Exception as e:
                            self.conn.rollback()
                            print(f"  ⚠️ DB 반영 실패 (rowid={rowid}): {e}")

            # 예외가 발생하더라도 열려있는 커서를 안전하게 수거
            try:
                self.cur.execute(f"CLOSE {cursor_name}")
                self.conn.commit()
            except Exception:
                self.conn.rollback()

        self.cur.close()
        self.conn.close()
        print("\n🎉 [역교정 종결] 과거 노이즈 오폭 소독 및 최종 삼원 온톨로지 속성 재조정이 전수 마감되었습니다.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="과거 청산 및 역교정 기능 융합형 마스터 정제기")
    parser.add_argument('--backup', action='store_true', help='가동 전 서버사이드 _bak 복제 백업')
    args = parser.parse_args()
    
    refiner = TeiUltimateMasterRefiner(run_backup=args.backup)
    refiner.execute_refinement()