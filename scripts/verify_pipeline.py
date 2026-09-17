#!/usr/bin/env python3
"""Pipeline Self-Verification & Integration Test Suite.

Verifies:
1. Module imports and common configuration integration (config.py).
2. Individual entity recognition and Hanja / Initial-sound law conversion (hanja_utils.py).
3. Person name validation & compound token splitting (tag_tei_with_dict.py).
4. Integrated sentence recognition (TEI tagging & false-positive prevention).
5. CIDOC-CRM ontology mapping generation (generate_cidoc_mappings.py).
6. Pipeline orchestration dry-run integrity.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
from xml.etree import ElementTree as ET

# Ensure workspace root is in sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rdflib import Graph, Literal, RDF, URIRef
from rdflib.namespace import RDFS

from scripts.config import ROOT as CONFIG_ROOT, get_neo4j_config, get_pg_config, load_secrets, setting
from scripts.hanja_utils import apply_initial_sound_law, translate_hanja_name
from scripts.tag_tei_with_dict import LocalDocentEngine
from scripts.generate_cidoc_mappings import CidocTimelineGenerator


class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0

    def assert_true(self, condition: bool, description: str):
        if condition:
            print(f"  ✅ [PASS] {description}")
            self.passed += 1
        else:
            print(f"  ❌ [FAIL] {description}")
            self.failed += 1

    def assert_equal(self, actual, expected, description: str):
        if actual == expected:
            print(f"  ✅ [PASS] {description}")
            self.passed += 1
        else:
            print(f"  ❌ [FAIL] {description} (Expected: {expected!r}, Got: {actual!r})")
            self.failed += 1


def test_config_and_environment(runner: TestRunner):
    print("\n[1/5] 검증: 공통 설정(config.py) 및 환경 연동")
    runner.assert_equal(CONFIG_ROOT, ROOT, "config.ROOT 경로 일치")

    secrets = load_secrets()
    runner.assert_true(isinstance(secrets, dict), "secrets 정상 로드(dict 반환)")

    pg_cfg = get_pg_config()
    runner.assert_true(all(k in pg_cfg for k in ("host", "port", "user", "dbname")), "PG 설정 기본 키 구성 확인")

    neo4j_cfg = get_neo4j_config()
    runner.assert_true(all(k in neo4j_cfg for k in ("uri", "user", "password")), "Neo4j 설정 기본 키 구성 확인")

    fallback_val = setting("NON_EXISTENT_KEY_XYZ", default="fallback_ok")
    runner.assert_equal(fallback_val, "fallback_ok", "setting() 기본값 fallback 정상 작동")


def test_individual_term_recognition(runner: TestRunner):
    print("\n[2/5] 검증: 개별 용어 한자 변환 및 두성법칙 (hanja_utils)")

    # 1. 성씨 예외 처리
    runner.assert_equal(translate_hanja_name("金容圭"), "김용규", "金(금->김) 성씨 예외 변환")
    runner.assert_equal(translate_hanja_name("金基鉉"), "김기현", "金(금->김) 복수 글자 변환")

    # 2. 두성법칙 (ㄹ/ㄴ -> ㅇ/ㄴ 등)
    runner.assert_equal(translate_hanja_name("柳寬順"), "유관순", "두성법칙: 류(柳) -> 유관순")
    runner.assert_equal(translate_hanja_name("李承晩"), "이승만", "두성법칙: 리(李) -> 이승만")
    runner.assert_equal(translate_hanja_name("羅錫疇"), "나석주", "두성법칙: 라(羅) -> 나석주")

    # 3. 단위 두성법칙 함수 단독 테스트
    runner.assert_equal(apply_initial_sound_law("로동"), "노동", "두성법칙: 로 -> 노")
    runner.assert_equal(apply_initial_sound_law("량심"), "양심", "두성법칙: 량 -> 양")
    runner.assert_equal(apply_initial_sound_law("력사"), "역사", "두성법칙: 력 -> 역")
    runner.assert_equal(apply_initial_sound_law("박희도"), "박희도", "비해당 음운 불변")


def test_person_name_validation_and_splitting(runner: TestRunner):
    print("\n[3/5] 검증: 인명 유효성 판별 및 복합 토큰 분리 (tag_tei_with_dict)")

    engine = LocalDocentEngine.__new__(LocalDocentEngine)

    # 1. 유효 인명 검증
    valid_names = ["김용규", "이계창", "박희도", "손병희", "유관순", "나석주", "안중근"]
    for name in valid_names:
        runner.assert_true(engine._is_valid_person_name(name), f"유효 인명 판별: {name}")

    # 2. 무효/노이즈 토큰 필터링
    invalid_names = [
        "경찰서", "주재소", "사무소", "총독", "순사", "판사", "의원",
        "장연", "장연군", "평양", "시흥", "용정", "함흥",
        "김", "a", "123", "test-user",
    ]
    for name in invalid_names:
        runner.assert_true(not engine._is_valid_person_name(name), f"노이즈/지명/직책 차단: {name}")

    # 3. 복합 인명 토큰 분리 (예: '김용규이계창박희도' -> 개별 인명)
    compound = "김용규이계창박희도"
    split_result = engine._split_compound_korean_name_tokens(compound)
    runner.assert_equal(split_result, ["김용규", "이계창", "박희도"], f"연속 복합 토큰 분리: '{compound}'")

    # 4. 조사/접미사 정규화
    normalized = engine._normalize_person_candidate("손병희로부터")
    runner.assert_equal(normalized, "손병희", "조사('로부터') 제거 정규화")
    normalized_hanja = engine._normalize_person_candidate("金容圭외")
    runner.assert_equal(normalized_hanja, "김용규", "한자+접미사('외') 정규화")


def test_integrated_sentence_recognition_and_cidoc(runner: TestRunner):
    print("\n[4/5] 검증: 통합 문장 인식 (TEI 태깅 & CIDOC-CRM 온톨로지 매핑)")

    engine = LocalDocentEngine.__new__(LocalDocentEngine)

    # 1. 문장 내 한자 사전 태깅 + 인명 태깅 통합 테스트
    sentence = "金容圭의 부친인 金基鉉과 동지인 이계창은 장연군에서 만세운동을 모의하였다."
    
    # Step 1: 한자 사전 태깅
    pre_tagged = engine._clean_and_pre_tag_hanja(sentence)
    runner.assert_true("<term><foreign xml:lang=\"zh-Hani\">金容圭</foreign><gloss>김용규</gloss></term>" in pre_tagged,
                     "한자(金容圭) 사전 태그 정상 삽입")
    runner.assert_true("<term><foreign xml:lang=\"zh-Hani\">金基鉉</foreign><gloss>김기현</gloss></term>" in pre_tagged,
                     "한자(金基鉉) 사전 태그 정상 삽입")

    # Step 2: 식별된 유효 인명 적용
    real_names = ["김용규", "김기현", "이계창"]
    tagged_tei = engine._apply_persname_tags(pre_tagged, real_names, "EV_1919")

    runner.assert_true('<persName ref="#EV_1919_金容圭">' in tagged_tei, "인명(金容圭) persName 태그 정상 적용")
    runner.assert_true('<persName ref="#EV_1919_金基鉉">' in tagged_tei, "인명(金基鉉) persName 태그 정상 적용")
    runner.assert_true('<persName ref="#EV_1919_이계창">이계창</persName>' in tagged_tei, "한글 인명(이계창) persName 태그 정상 적용")
    runner.assert_true("장연군" in tagged_tei and "<persName" not in tagged_tei.split("장연군")[0].split(">")[-1],
                     "지명(장연군) 오인식 persName 방지 확인")

    # 2. CIDOC-CRM 온톨로지 생성 검증
    cidoc_gen = CidocTimelineGenerator.__new__(CidocTimelineGenerator)
    from rdflib import Namespace
    cidoc_gen.CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
    cidoc_gen.EX = Namespace("http://example.org/docent/")
    cidoc_gen._sanitize_uri_fragment = lambda text: re.sub(r'[<>"\'\\]', '', text.replace('#', '').strip().replace(' ', '_'))

    test_graph = Graph()
    event_uri = URIRef("http://example.org/docent/event_101")
    
    act_count = cidoc_gen._build_sequential_timeline(
        target_graph=test_graph,
        table="test_table",
        rowid=1,
        tei=tagged_tei,
        event_uri=event_uri,
    )

    runner.assert_equal(act_count, 3, "CIDOC 활동(Activity) 추출 개수 3건 일치")

    # RDF 그래프 내 E7_Activity 및 E39_Actor 검증
    activities = list(test_graph.subjects(RDF.type, cidoc_gen.CRM.E7_Activity))
    actors = list(test_graph.subjects(RDF.type, cidoc_gen.CRM.E39_Actor))
    runner.assert_equal(len(activities), 3, "생성된 E7_Activity 인스턴스 3개 일치")
    runner.assert_equal(len(actors), 3, "생성된 E39_Actor 인스턴스 3개 일치")

    # Actor 레이블 확인
    actor_labels = {str(lbl) for s, _, lbl in test_graph.triples((None, RDFS.label, None)) if s in actors}
    runner.assert_true("김용규" in actor_labels and "김기현" in actor_labels and "이계창" in actor_labels,
                     f"CIDOC Actor 레이블 일치 확인: {actor_labels}")


def test_pipeline_dry_run_and_integrity(runner: TestRunner):
    print("\n[5/5] 검증: 파이프라인 오케스트레이션 및 레거시 파일 정리 확인")

    # 1. 삭제된 불필요 파일이 없는지 확인
    quick_apply = ROOT / "scripts" / "quick_apply_relations.py"
    neo4j_clean = ROOT / "scripts" / "neo4j_cleanup.py"
    runner.assert_true(not quick_apply.exists(), "quick_apply_relations.py 완전 삭제 확인")
    runner.assert_true(not neo4j_clean.exists(), "neo4j_cleanup.py 완전 삭제 확인")

    # 2. run_pipeline.py --dry-run 서브프로세스 실행 검증
    res = subprocess.run(
        [sys.executable, str(ROOT / "run_pipeline.py"), "--dry-run"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )
    runner.assert_equal(res.returncode, 0, "run_pipeline.py --dry-run 종료코드 0")
    runner.assert_true("Pipeline orchestrator starting" in res.stdout, "오케스트레이터 시작 로그 확인")
    runner.assert_true(res.stdout.count("[OK]") >= 10, "10개 파이프라인 단계 모두 [OK] 확인")


def main():
    print("=" * 60)
    print("🔎 Docent 파이프라인 리팩토링 자체 종합 검증 시작")
    print("=" * 60)

    runner = TestRunner()

    test_config_and_environment(runner)
    test_individual_term_recognition(runner)
    test_person_name_validation_and_splitting(runner)
    test_integrated_sentence_recognition_and_cidoc(runner)
    test_pipeline_dry_run_and_integrity(runner)

    print("\n" + "=" * 60)
    print(f"📊 검증 결과: 통과 {runner.passed}개 / 실패 {runner.failed}개")
    print("=" * 60)

    if runner.failed > 0:
        sys.exit(1)
    print("\n🎉 모든 자체 검증 항목을 성공적으로 통과했습니다!")


if __name__ == "__main__":
    main()
