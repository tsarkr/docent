<?php
/**
 * scripts/run_docent_pipeline.php — 3·1운동 역사 AI 도슨트 전체 프롬프트 자동화 파이프라인 실행기
 *
 * 사용법:
 *   php scripts/run_docent_pipeline.php "질의어" [언어: ko|en|ja|zh]
 *
 * 실행 단계:
 *   1단계: 질의 의도 분석 (Query Analyzer)
 *   2단계: Text-to-Cypher 동적 그래프 질의 생성 및 실행
 *   3단계: PostgreSQL 1차 사료 연동
 *   4단계: 사료 비판 기반 정밀 팩트 표 추출 (Stage 1 Fact Table)
 *   5단계: 4대 원칙 기반 역사 도슨트 해설 완성 (Stage 2 Historical Docent Commentary)
 */

declare(strict_types=1);

require_once __DIR__ . '/../includes/config.php';
require_once __DIR__ . '/../includes/security.php';
require_once __DIR__ . '/../includes/database.php';
require_once __DIR__ . '/../includes/gemini.php';
require_once __DIR__ . '/../includes/helpers.php';
require_once __DIR__ . '/../includes/prompts.php';
require_once __DIR__ . '/../includes/actions.php';

$query = $argv[1] ?? '화성 제암리 3·1 만세운동과 일제의 학살';
$target_lang = $argv[2] ?? 'ko';

echo "======================================================================\n";
echo "  3·1운동 역사 AI 도슨트 전체 프롬프트 자동화 파이프라인\n";
echo "======================================================================\n";
echo "▶ 질의: {$query}\n";
echo "▶ 요청 언어: {$target_lang}\n";
echo "----------------------------------------------------------------------\n\n";

// ── 1. 질의 의도 분석 (Query Analyzer) ──
echo "[1/5] 다국어 질의 의도 분석 및 DB 키워드 매핑 수행 중...\n";
$analyzer_system = "# Role\n"
                 . "당신은 '3.1 운동 역사 도슨트 시스템'의 다국어 질의 분석기(Query Analyzer)입니다.\n"
                 . "사용자의 질문이 어떤 언어로 들어오든 그 역사적 의도를 파악하고, 한국어 지식그래프(Graph DB) 검색에 필요한 핵심 데이터를 추출해야 합니다.\n\n"
                 . "# Objective\n"
                 . "입력된 [사용자 질의]를 분석하여 지정된 JSON 형식으로만 결과를 출력하십시오.\n"
                 . "{\n"
                 . "  \"intent_type\": \"ENTITY_SEARCH | EVENT_RELATION | GENERAL_INFO\",\n"
                 . "  \"focus\": \"인물 | 장소 | 사건 | 문헌 | 종합\",\n"
                 . "  \"analyzed_intent_ko\": \"문장 형태의 한국어 요약 질의\",\n"
                 . "  \"search_keywords\": [\"keyword1\", \"keyword2\"],\n"
                 . "  \"response_language\": \"ko\"\n"
                 . "}";

$analysis_raw = call_gemini([
    ["role" => "system", "content" => $analyzer_system],
    ["role" => "user", "content" => $query]
], true, null, 0.0, 0.8);

$analysis = json_decode($analysis_raw, true) ?: [];
$intent_ko = $analysis['analyzed_intent_ko'] ?? $query;
$keywords = $analysis['search_keywords'] ?? [$query];
$lang = $target_lang ?: ($analysis['response_language'] ?? 'ko');
$focus = $analysis['focus'] ?? '종합';

echo "  ✔ 의도 요약: {$intent_ko}\n";
echo "  ✔ 검색 키워드: " . implode(', ', $keywords) . "\n";
echo "  ✔ 감지 언어: {$lang}, 초점: {$focus}\n\n";

// ── 2. Text-to-Cypher 생성 및 그래프/사료 검색 ──
echo "[2/5] Text-to-Cypher 동적 그래프 쿼리 생성 및 지식그래프 탐색...\n";
$cypher = generate_text_to_cypher($intent_ko);
echo "  ✔ 생성된 Cypher: " . ($cypher ?: "(생성 생략 또는 Fulltext 대체)") . "\n";

$nodes = [];
$edges = [];
$evidences = [];
$prefetch_names = [];
$found_candidates = [];

$client = get_neo4j();
$cypher_count = 0;
if ($cypher !== '') {
    $cypher_count = execute_dynamic_cypher($client, $cypher, $nodes, $edges, $evidences, $prefetch_names, $found_candidates);
    echo "  ✔ 동적 Cypher 실행 결과: {$cypher_count}개 레코드 매칭\n";
}

// Fulltext fallback/보완 검색
$search_query = "
    CALL db.index.fulltext.queryNodes('namesIndex', \$term) YIELD node, score
    WHERE score >= 2.0
    RETURN DISTINCT node as n, labels(node) as labels, score
    ORDER BY score DESC
    LIMIT 10
";
foreach ($keywords as $kw) {
    if (empty($kw)) continue;
    $escaped_kw = docent_escape_lucene($kw);
    if ($escaped_kw === '') continue;
    try {
        $res = $client->run($search_query, ['term' => $escaped_kw]);
        foreach ($res as $record) {
            $node = $record->get('n');
            $labels_iterable = $record->get('labels');
            $nid = add_node_to_map($nodes, $node, $labels_iterable);
            if ($nid) {
                $raw_id = $nodes[$nid]['raw_id'] ?? $nid;
                $props = $nodes[$nid]['props'] ?? [];
                $labels = $nodes[$nid]['labels'] ?? [];
                $name = (string)($props['명칭'] ?? $props['name'] ?? $props['사건명'] ?? $props['title'] ?? $raw_id);
                $desc = (string)($props['설명'] ?? $props['description'] ?? '');
                add_fact_evidence($evidences, 'node', [(string)$raw_id, $name, $desc], [
                    "doc" => "[" . implode(',', $labels) . "] {$name}",
                    "quote" => mb_substr($desc, 0, 300),
                    "concept" => (string)$raw_id,
                    "text" => "명칭: {$name}\n설명: {$desc}"
                ], 40);
                $prefetch_names[] = (string)$raw_id;
            }
        }
    } catch (\Throwable $e) {
        // Fulltext index 실패 시 pass
    }
}

echo "  ✔ 탐색된 노드 수: " . count($nodes) . "개, 엣지 수: " . count($edges) . "개, 사료 건수: " . count($evidences) . "건\n\n";

// ── 3. PostgreSQL 원천 사료 연동 ──
echo "[3/5] PostgreSQL 원천 사료 연동 (Prefetch)...\n";
$pg_texts = [];
$pdo = get_pg();
$pn_unique = array_values(array_unique($prefetch_names));
if ($pdo && !empty($pn_unique)) {
    $filtered_names = array_slice($pn_unique, 0, 15);
    $tables_meta = get_pg_tables_metadata($pdo);
    $rows = fetch_pg_rows_for_names($pdo, $filtered_names, $tables_meta);
    foreach ($rows as $r) {
        $snippets = [];
        if (!empty($r['snippets']) && is_array($r['snippets'])) {
            foreach ($r['snippets'] as $k => $v) {
                if ($v) $snippets[] = "{$k}: {$v}";
            }
        }
        $pg_texts[] = "[{$r['schema']}.{$r['table']}] node={$r['node_id']} rowid={$r['rowid']} :: " . implode('; ', $snippets);
    }
}
echo "  ✔ PostgreSQL 원천 사료: " . count($pg_texts) . "건 연동 완료\n\n";

// ── 4. 사료 컨텍스트 조립 및 1단계 팩트 테이블 추출 ──
echo "[4/5] 사료 컨텍스트 조립 및 [1단계: 사료 비판 기반 팩트 테이블 추출] 프롬프트 실행...\n";
$evidence_context = build_budgeted_evidence_context($evidences, $lang, 20, 250, 600, 5000, $query, $focus);
$pg_context = build_budgeted_pg_context($pg_texts, $lang, 25, 180, 450, 6000, $query, $focus);
$context_str = trim($evidence_context . ($evidence_context && $pg_context ? "\n\n" : "") . $pg_context);

$fact_table = '';
if (!empty($context_str)) {
    $fact_prompts = get_fact_extraction_prompts($lang, $context_str);
    try {
        $fact_table = call_gemini([
            ["role" => "system", "content" => $fact_prompts['system']],
            ["role" => "user", "content" => $fact_prompts['user']]
        ], false, 1500, 0.0, 0.8, 50);
        echo "  ✔ 1단계 팩트 테이블 추출 성공 (" . mb_strlen($fact_table) . "자)\n";
    } catch (\Throwable $e) {
        echo "  ⚠ 1단계 팩트 테이블 추출 실패: " . $e->getMessage() . "\n";
    }
} else {
    echo "  ⚠ 사료 컨텍스트가 비어 있어 팩트 추출을 건너뜁니다.\n";
}
echo "\n";

// ── 5. 2단계: 신규 4대 원칙 AI 도슨트 역사 해설 생성 ──
echo "[5/5] [2단계: 국사편찬위원회/독립기념관 수준 AI 도슨트 전문 해설] 프롬프트 실행...\n";
$explain_prompts = get_explain_prompts($lang, $query, $context_str, $fact_table);

$commentary = call_gemini([
    ["role" => "system", "content" => $explain_prompts['system']],
    ["role" => "user", "content" => $explain_prompts['user']]
], false, 8192, 0.1, 0.85, 90);

echo "\n======================================================================\n";
echo "  [최종 생성된 AI 도슨트 역사 해설]\n";
echo "======================================================================\n\n";
echo $commentary . "\n\n";
echo "======================================================================\n";
echo "  자동화 파이프라인 실행 완료!\n";
echo "======================================================================\n";
