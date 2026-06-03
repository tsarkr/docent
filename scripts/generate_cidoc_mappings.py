import psycopg2
from pathlib import Path
import tomllib
import re
from rdflib import Graph, Literal, RDF, URIRef, Namespace
from rdflib.namespace import RDFS, XSD, OWL

class CidocTimelineGenerator:
    def __init__(self):
        self.secrets = self._load_secrets()
        self.conn = self._get_conn()
        self.cur = self.conn.cursor()
        self.output_ttl = "/Users/gyungmin/Dev/docent/CIDOC_Timeline_Mappings.ttl"
        
        self.g = Graph()
        self.CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
        self.EX = Namespace("http://example.org/historical-event/")
        
        self.g.bind("crm", self.CRM)
        self.g.bind("ex", self.EX)
        self.g.bind("rdfs", RDFS)
        self.g.bind("owl", OWL)

    def _load_secrets(self):
        path = Path(__file__).resolve().parent.parent / '.streamlit' / 'secrets.toml'
        with open(path, 'rb') as f: return tomllib.load(f)

    def _get_conn(self):
        return psycopg2.connect(
            host=self.secrets.get('PG_HOST'), user=self.secrets.get('PG_USER'),
            password=self.secrets.get('PG_PASSWORD'), dbname=self.secrets.get('PG_DATABASE')
        )

    def _ensure_mapping_table(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS tei_cidoc_mappings (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(100),
                rowid BIGINT,
                mapping_label TEXT,
                cidoc_ttl TEXT,
                UNIQUE (table_name, rowid)
            );
        """)
        self.conn.commit()

    def _sanitize_uri_fragment(self, text):
        if not text: return "unknown"
        text = text.replace('#', '').strip().replace(' ', '_')
        text = re.sub(r'[<>"\'\\]', '', text)
        return text

    def _table_has_column(self, table, column_name):
        self.cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_name = %s
            LIMIT 1
            """,
            (table, column_name),
        )
        return self.cur.fetchone() is not None

    def _build_sequential_timeline(self, target_graph, table, rowid, tei, event_uri):
        pers_blocks = re.findall(r'<persName\s+ref="([^"]+)"[^>]*>(.*?)</persName>', tei, flags=re.DOTALL)
        if not pers_blocks:
            return 0

        act_count = 0
        prev_activity_uri = None

        for idx, (ref, inner_content) in enumerate(pers_blocks):
            gloss_match = re.search(r'<gloss>([^<]+)</gloss>', inner_content)
            if gloss_match:
                pure_name = gloss_match.group(1).strip()
            else:
                pure_name = re.sub(r'<[^>]+>', '', inner_content).strip()

            if not pure_name:
                continue

            clean_ref = self._sanitize_uri_fragment(ref)
            
            act_id = f"activity_{table}_{rowid}_{idx}"
            actor_id = f"actor_{clean_ref}"

            act_uri = URIRef(self.EX[act_id])
            actor_uri = URIRef(self.EX[actor_id])

            # 개별 인스턴스 관계 선언
            target_graph.add((act_uri, RDF.type, self.CRM.E7_Activity))
            target_graph.add((act_uri, RDFS.label, Literal(f"{pure_name} 관련 하위 액티비티 ({idx+1})", lang="ko")))
            
            target_graph.add((event_uri, self.CRM.P9_consists_of, act_uri))

            target_graph.add((act_uri, self.CRM.P14_carried_out_by, actor_uri))
            target_graph.add((actor_uri, RDF.type, self.CRM.E39_Actor))
            target_graph.add((actor_uri, RDFS.label, Literal(pure_name, lang="ko")))

            if prev_activity_uri:
                target_graph.add((prev_activity_uri, self.CRM.P134_was_continued_by, act_uri))
            
            prev_activity_uri = act_uri
            act_count += 1

        return act_count

    def generate_and_save(self):
        self._ensure_mapping_table()
        
        # 1. 메인 온톨로지 선언 구조 구축
        ontology_uri = URIRef("http://example.org/historical-event-ontology")
        self.g.add((ontology_uri, RDF.type, OWL.Ontology))
        self.g.add((ontology_uri, RDFS.label, Literal("역사 사료 독립운동 CIDOC-CRM 통합 온톨로지", lang="ko")))
        
        # 2. 💡 [도구 인식 대수술] 도구가 클래스 계층을 100% 인지하도록 메타 스키마 주입
        self.g.add((self.CRM.E5_Event, RDF.type, OWL.Class))
        self.g.add((self.CRM.E5_Event, RDFS.label, Literal("Event (사건 통합체)", lang="ko")))
        
        self.g.add((self.CRM.E7_Activity, RDF.type, OWL.Class))
        self.g.add((self.CRM.E7_Activity, RDFS.label, Literal("Activity (하위 액티비티)", lang="ko")))
        self.g.add((self.CRM.E7_Activity, RDFS.subClassOf, self.CRM.E5_Event))  # 상속 관계 명시
        
        self.g.add((self.CRM.E39_Actor, RDF.type, OWL.Class))
        self.g.add((self.CRM.E39_Actor, RDFS.label, Literal("Actor (행위자)", lang="ko")))

        # 오브젝트 프로퍼티 명시적 선언
        self.g.add((self.CRM.P9_consists_of, RDF.type, OWL.ObjectProperty))
        self.g.add((self.CRM.P14_carried_out_by, RDF.type, OWL.ObjectProperty))
        self.g.add((self.CRM.P134_was_continued_by, RDF.type, OWL.ObjectProperty))

        self.cur.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name LIKE 'raw_%' AND table_name NOT LIKE '%_bak'
        """)
        tables = [t[0] for t in self.cur.fetchall()]

        total_events_processed = 0
        total_activities_mapped = 0

        print("🔮 [클래스 구조 명시적 주입판] CIDOC-CRM 타임라인 변환을 가동합니다.")

        for table in tables:
            if not self._table_has_column(table, 'tei'):
                continue

            if self._table_has_column(table, 'tei_status'):
                query = f'SELECT rowid, tei FROM "{table}" WHERE tei_status = \'REFINED\' ORDER BY rowid'
            else:
                query = f'SELECT rowid, tei FROM "{table}" ORDER BY rowid'

            self.cur.execute(query)
            rows = self.cur.fetchall()
            
            if not rows: continue
                
            print(f"📘 테이블 연산 및 클래스 매핑 중: {table} ({len(rows)}건 발견)")
            
            for rowid, tei in rows:
                event_id = f"{table}_{rowid}"
                safe_event_id = event_id.replace(' ', '_')
                event_uri = URIRef(self.EX[safe_event_id])
                
                row_sub_graph = Graph()
                row_sub_graph.bind("crm", self.CRM)
                row_sub_graph.bind("ex", self.EX)
                row_sub_graph.bind("rdfs", RDFS)
                row_sub_graph.bind("owl", OWL)

                # 개별 로우 덤프용 서브 그래프 스키마 선언
                row_sub_graph.add((ontology_uri, RDF.type, OWL.Ontology))
                row_sub_graph.add((self.CRM.E5_Event, RDF.type, OWL.Class))
                row_sub_graph.add((self.CRM.E7_Activity, RDF.type, OWL.Class))
                row_sub_graph.add((self.CRM.E7_Activity, RDFS.subClassOf, self.CRM.E5_Event))
                row_sub_graph.add((self.CRM.E39_Actor, RDF.type, OWL.Class))

                self.g.add((event_uri, RDF.type, self.CRM.E5_Event))
                self.g.add((event_uri, RDFS.label, Literal(f"역사적 사건 통합체 {event_id}", lang="ko")))
                row_sub_graph.add((event_uri, RDF.type, self.CRM.E5_Event))
                row_sub_graph.add((event_uri, RDFS.label, Literal(f"역사적 사건 통합체 {event_id}", lang="ko")))
                
                self._build_sequential_timeline(self.g, table, rowid, tei, event_uri)
                act_count = self._build_sequential_timeline(row_sub_graph, table, rowid, tei, event_uri)
                
                if act_count > 0:
                    total_events_processed += 1
                    total_activities_mapped += act_count
                    
                    cidoc_ttl_text = row_sub_graph.serialize(format="turtle")
                    mapping_label = f"Timeline Mapping for {event_id}"

                    self.cur.execute("""
                        INSERT INTO tei_cidoc_mappings (table_name, rowid, mapping_label, cidoc_ttl)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (table_name, rowid) 
                        DO UPDATE SET mapping_label = EXCLUDED.mapping_label, cidoc_ttl = EXCLUDED.cidoc_ttl;
                    """, (table, rowid, mapping_label, cidoc_ttl_text))

            self.conn.commit()

        if total_activities_mapped == 0:
            print("\n⚠️ [경고] 추출된 인명 마크업 태그가 없습니다.")
            return

        print(f"\n💾 통합 온톨로지 파일 디스크 저장 중...")
        self.g.serialize(destination=self.output_ttl, format="turtle")
        print(f"🎉 클래스 계층 선언 완료 ➡️ {self.output_ttl}")

        self.cur.close()
        self.conn.close()

if __name__ == "__main__":
    generator = CidocTimelineGenerator()
    generator.generate_and_save()