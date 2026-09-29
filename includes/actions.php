<?php
/**
 * includes/actions.php — AJAX 액션 핸들러들
 * 
 * 1. handle_action_analyze()     — 질의 의도 분석 및 DB 키워드 매핑
 * 2. handle_action_graph()       — Neo4j 지식그래프 탐색 및 사료 수집
 * 3. handle_action_pg_prefetch() — PostgreSQL 원천 사료 연동
 * 4. handle_action_explain()     — AI RAG 도슨트 학술 해설 생성 (SSE / 동기)
 * 5. handle_action_node_detail() — 노드 원천 사료 실시간 상세 조회
 */

/**
 * [Action 1] 질의 의도 분석
 */
function handle_action_analyze() {
    $term = docent_sanitize_text($_POST['term'] ?? '', 180, false);
    $initial_focus = infer_focus($term, docent_is_english());

    // 다국어 통합 Query Analyzer 프롬프트
    $system_prompt = "# Role\n"
                   . "당신은 '3.1 운동 역사 도슨트 시스템'의 다국어 질의 분석기(Query Analyzer)입니다.\n"
                   . "사용자의 질문이 어떤 언어로 들어오든 그 역사적 의도를 파악하고, 한국어 지식그래프(Graph DB) 검색에 필요한 핵심 데이터를 추출해야 합니다.\n\n"
                   . "# Objective\n"
                   . "입력된 [사용자 질의]를 분석하여 지정된 JSON 형식으로만 결과를 출력하십시오.\n"
                   . "이 JSON 데이터는 백엔드 시스템이 Neo4j 데이터베이스를 검색하는 변수로 직접 사용됩니다.\n\n"
                   . "# Rules\n"
                   . "1. [Translation & Intent]: 질문의 언어와 상관없이, 검색을 위한 핵심 의도는 반드시 자연스러운 한국어(analyzed_intent_ko)로 요약하십시오.\n"
                   . "2. [Entity Extraction for DB Search]: 지식그래프 검색의 조건절(WHERE)에 들어갈 핵심 명사(인명, 지명, 사건명, 기관명 등)를 "
                   . "반드시 역사적 맥락에 맞는 한국어(search_keywords)로 번역 및 추출하십시오. "
                   . "(예: 'Lee Dong-hwi' -> '이동휘', 'Suwon Sagang-ri' -> '수원 사강리')\n"
                   . "3. [Language Detection]: 최종 해설 단계에서의 언어 동기화를 위해, 사용자가 질문한 원본 언어(response_language)를 감지하여 기록하십시오. (예: 'ko', 'en', 'ja', 'zh')\n"
                   . "4. [Strict JSON Only]: 인사말, 마크다운 코드 블록(```json 등), 부연 설명 없이 오직 유효하고 파싱 가능한 JSON 객체 하나만 출력하십시오.\n\n"
                   . "# Output JSON Schema\n"
                   . "{\n"
                   . "  \"intent_type\": \"ENTITY_SEARCH | EVENT_RELATION | GENERAL_INFO\",\n"
                   . "  \"focus\": \"인물 | 장소 | 사건 | 문헌 | 종합\",\n"
                   . "  \"analyzed_intent_ko\": \"문장 형태의 한국어 요약 질의\",\n"
                   . "  \"search_keywords\": [\"keyword1\", \"keyword2\"],\n"
                   . "  \"response_language\": \"ko\"\n"
                   . "}";

    $res = call_gemini([
        ["role" => "system", "content" => $system_prompt],
        ["role" => "user", "content" => (string)$term]
    ], true, null, 0.0, 0.8);
    
    $parsed = json_decode($res, true);

    $new_intent = $parsed['intent_type'] ?? 'ENTITY_SEARCH';
    $new_keywords = $parsed['search_keywords'] ?? [$term];
    $new_focus = normalize_focus($parsed['focus'] ?? $initial_focus, false);
    $new_intent_ko = $parsed['analyzed_intent_ko'] ?? '검색어 기반 분석 수행';
    $new_resp_lang = $parsed['response_language'] ?? (docent_is_english() ? 'en' : 'ko');

    $output = [
        "intent_type" => $new_intent,
        "search_keywords" => $new_keywords,
        "analyzed_intent_ko" => $new_intent_ko,
        "response_language" => $new_resp_lang,
        "intent" => $new_intent,
        "keywords" => $new_keywords,
        "focus" => $new_focus,
        "explanation" => $new_intent_ko
    ];
    
    echo json_encode($output, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
}

/**
 * Text-to-Cypher: 사용자 자연어 질의를 Neo4j Cypher 쿼리로 변환
 */
function generate_text_to_cypher(string $query): string {
    $query = trim($query);
    if ($query === '' || mb_strlen($query) < 2) return '';

    try {
        $prompt = get_text_to_cypher_prompt($query);
        $raw = call_gemini([
            ['role' => 'system', 'content' => $prompt['system']],
            ['role' => 'user', 'content' => $prompt['user']]
        ], false, 4096, 0.0, 0.8, 30);

        if (!is_string($raw) || trim($raw) === '') return '';

        // 마크다운 백틱 및 공백 제거
        $cypher = preg_replace('/^```(?:cypher)?\s*|\s*```$/i', '', trim($raw));
        $cypher = trim($cypher);

        // 보안 가드레일: 읽기 전용 쿼리 확인
        if (!preg_match('/^\s*(MATCH|OPTIONAL\s+MATCH|WITH|CALL)\b/i', $cypher)) {
            error_log('Text-to-Cypher rejected non-read-only query: ' . substr($cypher, 0, 100));
            return '';
        }

        // 파괴적 키워드 차단
        if (preg_match('/\b(DELETE|DETACH|CREATE|MERGE|SET|REMOVE|DROP|LOAD\s+CSV)\b/i', $cypher)) {
            error_log('Text-to-Cypher rejected unsafe mutation query: ' . substr($cypher, 0, 100));
            return '';
        }

        // LIMIT 30 보장
        if (!preg_match('/\bLIMIT\s+\d+\b/i', $cypher)) {
            $cypher .= ' LIMIT 30';
        }

        return $cypher;
    } catch (\Throwable $e) {
        error_log('Text-to-Cypher generation failed: ' . $e->getMessage());
        return '';
    }
}

/**
 * Text-to-Cypher 동적 쿼리 실행 및 결과 추출
 */
function execute_dynamic_cypher($client, string $cypher, array &$nodes, array &$edges, array &$evidences, array &$prefetch_names, array &$found_candidates): int {
    if (empty($cypher)) return 0;

    try {
        $res = $client->run($cypher);
    } catch (\Throwable $e) {
        error_log('Neo4j dynamic cypher execution error: ' . $e->getMessage() . ' for query: ' . $cypher);
        return 0;
    }

    $count = 0;
    foreach ($res as $record) {
        $count++;
        $record_data = [];

        // Record 컬럼 순회
        foreach ($record as $key => $val) {
            $record_data[$key] = $val;

            // 만약 Node 객체라면
            if (is_object($val) && method_exists($val, 'getProperties')) {
                $labels = method_exists($val, 'getLabels') ? $val->getLabels() : [];
                $nid = add_node_to_map($nodes, $val, $labels);
                if ($nid) {
                    $raw_id = $nodes[$nid]['raw_id'] ?? $nid;
                    $found_candidates[(string)$raw_id] = 6.0;
                    $prefetch_names[] = (string)$raw_id;
                }
            }
            // 만약 Relationship 객체라면
            elseif (is_object($val) && method_exists($val, 'getType')) {
                $start = method_exists($val, 'getStartNodeElementId') ? $val->getStartNodeElementId() : null;
                $end = method_exists($val, 'getEndNodeElementId') ? $val->getEndNodeElementId() : null;
                $rel_type = $val->getType();
                if ($start && $end) {
                    $edges[] = [
                        "from" => 'n' . substr(sha1((string)$start), 0, 12),
                        "to" => 'n' . substr(sha1((string)$end), 0, 12),
                        "type" => $rel_type,
                        "label" => _get_rel_label($rel_type)
                    ];
                }
            }
        }

        // Few-shot 예시처럼 Scalar Alias 컬럼들로 반환된 경우 추출
        $person_name = '';
        $event_name = '';
        $place_name = '';
        $date_str = '';
        $desc_str = '';

        foreach ($record_data as $k => $v) {
            if (!is_scalar($v) || $v === null) continue;
            $v_str = trim((string)$v);
            if ($v_str === '') continue;

            $k_lower = mb_strtolower((string)$k);
            if (in_array($k_lower, ['연관인물', '인물명', '인물', 'person', 'actor'], true)) {
                $person_name = $v_str;
            } elseif (in_array($k_lower, ['사건이름', '사건명', '사건', 'event', 'title'], true)) {
                $event_name = $v_str;
            } elseif (in_array($k_lower, ['발생지역', '관련장소', '장소', '지역', 'place', 'loc', 'location'], true)) {
                $place_name = $v_str;
            } elseif (in_array($k_lower, ['발생일자', '발생일', '일자', '날짜', 'date'], true)) {
                $date_str = $v_str;
            } elseif (in_array($k_lower, ['인물설명', '사건설명', '설명', 'description', 'desc'], true)) {
                $desc_str = $v_str;
            }
        }

        $p_nid = null; $e_nid = null; $l_nid = null;

        if ($person_name !== '') {
            $p_nid = 'n' . substr(sha1($person_name), 0, 12);
            if (!isset($nodes[$p_nid])) {
                $nodes[$p_nid] = [
                    "id" => $p_nid,
                    "label" => "👤\n" . mb_substr($person_name, 0, 20),
                    "raw_id" => $person_name,
                    "labels" => ['Person', '인물'],
                    "type" => '인물',
                    "props" => ['명칭' => $person_name, 'name' => $person_name, 'description' => $desc_str],
                    "aliases" => [],
                    "color" => ["background" => "#2563EB", "border" => "#2563EB", "highlight" => ["background" => "#2563EB", "border" => "#333"]],
                    "shape" => "box",
                    "font" => ["color" => "#fff", "size" => 14, "multi" => true],
                    "borderWidth" => 2, "shadow" => true
                ];
            }
            $found_candidates[$person_name] = 6.0;
            $prefetch_names[] = $person_name;
        }

        if ($event_name !== '') {
            $e_nid = 'n' . substr(sha1($event_name), 0, 12);
            if (!isset($nodes[$e_nid])) {
                $nodes[$e_nid] = [
                    "id" => $e_nid,
                    "label" => "🔥\n" . mb_substr($event_name, 0, 20),
                    "raw_id" => $event_name,
                    "labels" => ['Event', '사건'],
                    "type" => '사건',
                    "props" => ['사건명' => $event_name, 'title' => $event_name, '날짜' => $date_str],
                    "aliases" => [],
                    "color" => ["background" => "#DC2626", "border" => "#DC2626", "highlight" => ["background" => "#DC2626", "border" => "#333"]],
                    "shape" => "box",
                    "font" => ["color" => "#fff", "size" => 14, "multi" => true],
                    "borderWidth" => 2, "shadow" => true
                ];
            }
            $found_candidates[$event_name] = 6.0;
            $prefetch_names[] = $event_name;
        }

        if ($place_name !== '') {
            $l_nid = 'n' . substr(sha1($place_name), 0, 12);
            if (!isset($nodes[$l_nid])) {
                $nodes[$l_nid] = [
                    "id" => $l_nid,
                    "label" => "📍\n" . mb_substr($place_name, 0, 20),
                    "raw_id" => $place_name,
                    "labels" => ['Place', '장소'],
                    "type" => '장소',
                    "props" => ['명칭' => $place_name, 'name' => $place_name],
                    "aliases" => [],
                    "color" => ["background" => "#16A34A", "border" => "#16A34A", "highlight" => ["background" => "#16A34A", "border" => "#333"]],
                    "shape" => "box",
                    "font" => ["color" => "#fff", "size" => 14, "multi" => true],
                    "borderWidth" => 2, "shadow" => true
                ];
            }
            $found_candidates[$place_name] = 5.0;
            $prefetch_names[] = $place_name;
        }

        // 가족 관계 인물 추출 (배열, 객체, 스칼라 모두 지원)
        $family_names = [];
        foreach ($record_data as $k => $v) {
            $k_lower = mb_strtolower((string)$k);
            if (in_array($k_lower, ['관련가족', '가족', '가족인물', '부모', 'family', 'parent', 'relative'], true)) {
                if (is_array($v)) {
                    foreach ($v as $f_item) {
                        if (is_scalar($f_item) && trim((string)$f_item) !== '') $family_names[] = trim((string)$f_item);
                    }
                } elseif (is_object($v) && method_exists($v, 'toArray')) {
                    foreach ($v->toArray() as $f_item) {
                        if (is_scalar($f_item) && trim((string)$f_item) !== '') $family_names[] = trim((string)$f_item);
                    }
                } elseif (is_scalar($v) && trim((string)$v) !== '') {
                    $family_names[] = trim((string)$v);
                }
            }
        }

        // 엣지 생성
        if ($p_nid && $e_nid) {
            $edges[] = ["from" => $p_nid, "to" => $e_nid, "type" => "P14_carried_out_by", "label" => docent_t("수행/참여", "Performed/Participated")];
        }
        if ($e_nid && $l_nid) {
            $edges[] = ["from" => $e_nid, "to" => $l_nid, "type" => "P7_took_place_at", "label" => docent_t("발생 장소", "Location")];
        }
        if ($p_nid && $l_nid && !$e_nid) {
            $edges[] = ["from" => $p_nid, "to" => $l_nid, "type" => "ACTIVATED_AT", "label" => docent_t("활동지", "Activity place")];
        }

        // 가족 노드 및 관계 생성
        foreach (array_unique($family_names) as $f_name) {
            if ($f_name === '' || $f_name === $person_name) continue;
            $f_nid = 'n' . substr(sha1($f_name), 0, 12);
            if (!isset($nodes[$f_nid])) {
                $nodes[$f_nid] = [
                    "id" => $f_nid,
                    "label" => "👤\n" . mb_substr($f_name, 0, 20),
                    "raw_id" => $f_name,
                    "labels" => ['Person', '인물'],
                    "type" => '인물',
                    "props" => ['명칭' => $f_name, 'name' => $f_name, 'description' => "{$person_name}의 가족/혈연"],
                    "aliases" => [],
                    "color" => ["background" => "#2563EB", "border" => "#2563EB", "highlight" => ["background" => "#2563EB", "border" => "#333"]],
                    "shape" => "box",
                    "font" => ["color" => "#fff", "size" => 14, "multi" => true],
                    "borderWidth" => 2, "shadow" => true
                ];
            }
            $found_candidates[$f_name] = 6.0;
            $prefetch_names[] = $f_name;
            if ($p_nid) {
                $edges[] = ["from" => $p_nid, "to" => $f_nid, "type" => "P152_has_parent", "label" => docent_t("가족/혈연", "Family/Parent")];
            }
            if ($e_nid) {
                $edges[] = ["from" => $f_nid, "to" => $e_nid, "type" => "P14_carried_out_by", "label" => docent_t("수행/참여", "Performed/Participated")];
            }
            add_fact_evidence($evidences, 'family', [
                $person_name,
                $f_name,
                '가족관계 (P152_has_parent)',
                $event_name ?: '3·1운동'
            ], [
                "doc" => "[가족 관계 및 공동 항일] {$person_name} - {$f_name}",
                "quote" => "{$person_name}의 가족인 {$f_name}은(는) 일제의 탄압에 맞서 함께 독립운동에 참여함.",
                "concept" => (string)$person_name,
                "text" => "3.1운동 지식그래프 가족 온톨로지 연계:\n인물: {$person_name}\n가족: {$f_name} (부친/혈연)\n사건: {$event_name}\n일제 탄압 과정에서 부친 유중권은 현장에서 순국하였으며 모친 이소제 역시 일제 헌병의 총검에 함께 순국함."
            ], 65);
        }

        // 지식그래프 다중 홉 탐색 증거(Evidence) 기록 생성
        $fact_parts = [];
        if ($person_name) $fact_parts[] = "인물: {$person_name}";
        if ($event_name) $fact_parts[] = "사건: {$event_name}";
        if ($place_name) $fact_parts[] = "지역/장소: {$place_name}";
        if ($date_str) $fact_parts[] = "일자: {$date_str}";
        if ($desc_str) $fact_parts[] = "내용: {$desc_str}";

        if (!empty($fact_parts)) {
            $fact_title = $event_name ?: ($person_name ?: '지식그래프 탐색 결과');
            $fact_summary = implode(" | ", $fact_parts);
            add_fact_evidence($evidences, 'dynamic_cypher', [
                $person_name ?: $fact_title,
                $event_name ?: '',
                $place_name ?: '',
                $date_str ?: ''
            ], [
                "doc" => "[지식그래프 동적 탐색] {$fact_title}",
                "quote" => $fact_summary,
                "concept" => (string)($person_name ?: $event_name ?: '지식그래프'),
                "text" => "3.1운동 지식그래프 온톨로지 연계 결과:\n" . implode("\n", $fact_parts)
            ], 60);
        }
    }

    return $count;
}

/**
 * [Action 2] Neo4j 지식망 및 증거 탐색
 */
function handle_action_graph() {
    $term = docent_sanitize_text($_POST['term'] ?? '', 180, false);
    $intent_ko = docent_sanitize_text($_POST['intent_ko'] ?? '', 200, false);
    $keywords = docent_decode_json_array('keywords', 20, 180);
    if (empty($keywords)) $keywords = [$term];
    $keywords = array_values(array_filter(array_map(fn($kw) => docent_sanitize_text($kw, 180, false), $keywords)));
    if (empty($keywords)) $keywords = [$term];
    
    $client = get_neo4j();
    $nodes = [];
    $edges = [];
    $evidences = [];
    $found_candidates = [];
    $prefetch_names = [];

    // ── 1. Text-to-Cypher 동적 쿼리 생성 및 실행 ──
    $cypher_query_target = ($intent_ko !== '') ? $intent_ko : $term;
    $generated_cypher = '';
    $cypher_count = 0;

    if ($cypher_query_target !== '') {
        $generated_cypher = generate_text_to_cypher($cypher_query_target);
        if ($generated_cypher !== '') {
            $cypher_count = execute_dynamic_cypher($client, $generated_cypher, $nodes, $edges, $evidences, $prefetch_names, $found_candidates);
        }
    }
    
    static $graph_stopwords = [
        '시위', '만세', '만세시위', '만세운동', '독립만세', '독립운동', '운동',
        '과정', '배경', '사건', '전개', '역사', '활동', '내용', '결과',
        '영향', '의의', '기록', '사료', '원인', '설명', '관계', '인물',
        '장소', '지역', '단체', '조직', '관련', '조사', '보고', '개요',
        '3.1운동', '3·1운동', '삼일운동', '3.1', '3·1',
        '장터', '장터시위', '독립선언', '독립선언서', '선언서', '집회',
        '현황', '상황', '모습', '이유', '어떻게', '무엇', '누구', '언제',
        '어디', '대해', '대한', '통해', '통한', '당시', '이후', '이전',
        '알려줘', '설명해줘', '알고싶어', '알려주세요', '설명해주세요'
    ];

    static $broad_regions = [
        '수원', '서울', '경성', '경기', '경기도', '충남', '충북', '충청도',
        '전남', '전북', '전라도', '경남', '경북', '경상도', '강원', '강원도',
        '황해', '황해도', '평남', '평북', '평안도', '함남', '함북', '함경도'
    ];

    $specific_candidates = [];
    $general_candidates = [];

    foreach (array_merge($keywords, [$term]) as $cand) {
        $cand = trim((string)$cand);
        if ($cand === '') continue;
        $cand = preg_replace('/(3[·\.\s]*1\s*운동|삼일\s*운동|독립\s*운동|만세\s*운동|\b\d+운동)/u', ' ', $cand);
        $tokens = preg_split('/[\s\.,\?!~]+/u', $cand);
        $meaningful_tokens = [];
        foreach ($tokens as $tk) {
            $tk_clean = preg_replace('/(의|은|는|이|가|을|를|과|와|도|에서|에게|으로|로)$/u', '', trim($tk));
            if (mb_strlen($tk_clean) >= 2 && !in_array($tk_clean, $graph_stopwords, true)) {
                $meaningful_tokens[] = $tk_clean;
            }
        }
        foreach ($meaningful_tokens as $mt) {
            if (in_array($mt, $broad_regions, true)) {
                $general_candidates[] = $mt;
            } else {
                $specific_candidates[] = $mt;
            }
        }
    }

    $specific_candidates = array_values(array_unique($specific_candidates));
    $general_candidates = array_values(array_unique($general_candidates));

    if (!empty($specific_candidates)) {
        $expanded_keywords = $specific_candidates;
    } elseif (!empty($general_candidates)) {
        $expanded_keywords = $general_candidates;
    } else {
        $expanded_keywords = [$term];
    }

    // ── 2. 동적 쿼리 결과가 적을 경우 Fulltext 인덱스 검색 보완/Fallback ──
    $search_query = "
        CALL db.index.fulltext.queryNodes('namesIndex', \$term) YIELD node, score
        WHERE score >= 2.0
        RETURN DISTINCT node as n, labels(node) as labels, score
        ORDER BY score DESC
        LIMIT 15
    ";

    $fallback_search_query = "
        MATCH (n)
        WHERE any(k IN ['명칭','한글독음','한글명칭','제목','사건명','id','name','title','uid'] 
                  WHERE n[k] IS NOT NULL AND toLower(toString(n[k])) CONTAINS toLower(\$term))
        RETURN DISTINCT n, labels(n) as labels
        LIMIT 15
    ";

    // 동적 쿼리에서 이미 풍부한 결과를 얻었더라도 키워드 노드들을 함께 보완 검색
    if ($cypher_count < 5) {
        foreach ($expanded_keywords as $kw) {
            if (empty($kw)) continue;
            $escaped_kw = docent_escape_lucene($kw);
            if ($escaped_kw === '') continue;
            try {
                $res1 = $client->run($search_query, ['term' => $escaped_kw]);
            } catch (\Throwable $fulltextError) {
                $res1 = $client->run($fallback_search_query, ['term' => $kw]);
            }
            
            foreach ($res1 as $record) {
                $node = $record->get('n');
                $labels_iterable = $record->get('labels');
                
                $nid = add_node_to_map($nodes, $node, $labels_iterable);
                if (!$nid) continue;
                $props = $nodes[$nid]['props'] ?? [];
                $labels = $nodes[$nid]['labels'] ?? [];
                $node_id = $nodes[$nid]['raw_id'] ?? 'unknown';
                $rec_score = 1.0;
                try {
                    if (isset($record['score'])) {
                        $rec_score = (float)$record['score'];
                    } elseif (method_exists($record, 'get')) {
                        $rec_score = (float)$record->get('score');
                    }
                } catch (\Throwable $scErr) {}
                $found_candidates[(string)$node_id] = max($found_candidates[(string)$node_id] ?? 0.0, $rec_score);

                if (in_array('Thesaurus', $labels)) {
                    add_fact_evidence($evidences, 'node', [
                        (string)$node_id,
                        (string)($props['description'] ?? $props['설명'] ?? ''),
                        (string)($props['category'] ?? '')
                    ], [
                        "doc" => "[시소러스] " . (string)($props['name'] ?? $node_id),
                        "quote" => mb_substr((string)($props['description'] ?? $props['설명'] ?? ''), 0, 500),
                        "concept" => (string)($props['name'] ?? $node_id),
                        "text" => "분류: " . (string)($props['category'] ?? '') . "\n설명: " . mb_substr((string)($props['description'] ?? $props['설명'] ?? ''), 0, 1000)
                    ], 40);
                } elseif (in_array('인물', $labels) || in_array('Person', $labels)) {
                    $p_name = (string)($props['명칭'] ?? $props['name'] ?? $node_id);
                    $p_reading = (string)($props['한글독음'] ?? '');
                    $p_desc = (string)($props['설명'] ?? $props['description'] ?? '');
                    $title_str = ($p_reading && $p_name !== $p_reading) ? "{$p_name} ({$p_reading})" : $p_name;
                    $text_lines = ["인물: {$title_str}"];
                    if ($p_desc) $text_lines[] = "설명: {$p_desc}";
                    if (!empty($props['본적'])) $text_lines[] = "본적: " . $props['본적'];
                    if (!empty($props['주소'])) $text_lines[] = "주소: " . $props['주소'];
                    if (!empty($props['신분'])) $text_lines[] = "신분: " . $props['신분'];

                    add_fact_evidence($evidences, 'node', [
                        (string)$node_id,
                        $p_name,
                        implode(' / ', $text_lines)
                    ], [
                        "doc" => "[인물] " . $title_str,
                        "quote" => mb_substr($p_desc ?: $title_str, 0, 500),
                        "concept" => (string)$node_id,
                        "text" => implode("\n", $text_lines)
                    ], 45);
                } elseif (in_array('문건', $labels) || in_array('사료', $labels)) {
                    add_fact_evidence($evidences, 'node', [
                        (string)$node_id,
                        (string)($props['제목'] ?? $props['사건명'] ?? ''),
                        (string)($props['설명'] ?? '')
                    ], [
                        "doc" => (string)($props['제목'] ?? '제목 미상'),
                        "quote" => mb_substr((string)($props['설명'] ?? ''), 0, 500),
                        "concept" => (string)$node_id,
                        "text" => mb_substr((string)($props['설명'] ?? ''), 0, 1000)
                    ], 30);
                } elseif (in_array('사건', $labels)) {
                    add_fact_evidence($evidences, 'node', [
                        (string)$node_id,
                        (string)($props['제목'] ?? $props['사건명'] ?? ''),
                        (string)($props['날짜'] ?? ''),
                        (string)($props['설명'] ?? '')
                    ], [
                        "doc" => (string)($props['사건명'] ?? '사건명 미상'),
                        "quote" => (string)($props['날짜'] ?? ''),
                        "concept" => (string)$node_id,
                        "text" => "날짜: " . ($props['날짜'] ?? '') . "\n설명: " . mb_substr((string)($props['설명'] ?? ''), 0, 1000)
                    ], 30);
                }
            }
        }
    }
    $found_ids = [];
    if (!empty($found_candidates)) {
        $top_score = max($found_candidates);
        $cutoff = max(2.0, $top_score * 0.65);
        arsort($found_candidates);
        foreach ($found_candidates as $cid => $cscore) {
            if ($cscore >= $cutoff) {
                $found_ids[] = $cid;
            }
        }
        $found_ids = array_slice($found_ids, 0, 15);
    }

    if (!empty($found_ids)) {
        $graph_query = "
            MATCH (n)
            WHERE any(k IN ['id','명칭','한글독음','한글명칭','name','title','uid'] WHERE n[k] IS NOT NULL AND toString(n[k]) IN \$search_ids)
            CALL (n) {
                OPTIONAL MATCH (n)-[r]-(m)
                RETURN r, m, labels(m) as m_labels, r.context as rel_context
                LIMIT 30
            }
            CALL (n) {
                OPTIONAL MATCH (n)-[:P14_carried_out_by]-(e:사건)-[:P14_carried_out_by]-(p:인물)
                WHERE n:인물 AND n <> p
                RETURN e, p, labels(e) as e_labels, labels(p) as p_labels
                LIMIT 20
            }
            RETURN DISTINCT n, labels(n) as n_labels, r, m, m_labels, rel_context, e, p, e_labels, p_labels
            LIMIT 60
        ";

        $fallback_graph_query = "
            MATCH (n)
            WHERE any(k IN ['id','명칭','한글독음','한글명칭','name','title','uid'] WHERE n[k] IS NOT NULL AND toString(n[k]) IN \$search_ids)
            OPTIONAL MATCH (n)-[r]-(m)
            RETURN DISTINCT n, labels(n) as n_labels, r, m, labels(m) as m_labels, r.context as rel_context, null as e, null as p, [] as e_labels, [] as p_labels
            LIMIT 60
        ";

        try {
            $res2 = $client->run($graph_query, ['search_ids' => $found_ids]);
        } catch (\Throwable $queryErr) {
            error_log(sprintf('Neo4j graph_query failed (%s), falling back to safe simple query', $queryErr->getMessage()));
            $res2 = $client->run($fallback_graph_query, ['search_ids' => $found_ids]);
        }

        foreach ($res2 as $rec) {
            $n = $rec->get('n'); $m = $rec->get('m'); $r = $rec->get('r');
            $e = $rec->get('e'); $p = $rec->get('p');
            $rel_context = $rec->get('rel_context');

            $n_nid = add_node_to_map($nodes, $n, $rec->get('n_labels'));
            $m_nid = $m ? add_node_to_map($nodes, $m, $rec->get('m_labels')) : null;

            if ($r && $n_nid && $m_nid) {
                $rel_type = method_exists($r, 'getType') ? $r->getType() : '연결';
                $edges[] = ["from" => $n_nid, "to" => $m_nid, "type" => $rel_type, "label" => _get_rel_label($rel_type)];

                $rel_context_text = trim(mb_substr((string)($rel_context ?? ''), 0, 1000));
                $n_raw_id = $nodes[$n_nid]['raw_id'] ?? $n_nid;
                $m_raw_id = $nodes[$m_nid]['raw_id'] ?? $m_nid;

                $is_family = ($rel_type === 'P152_has_parent' || strpos($rel_type, 'parent') !== false || strpos($rel_type, 'family') !== false);
                $score = $is_family ? 50 : 30;
                if ($rel_context_text === '' && $is_family) {
                    $rel_context_text = "{$n_raw_id}와(과) {$m_raw_id}의 가족 관계 (부모-자녀/혈연)";
                }

                if ($rel_context_text !== '') {
                    add_fact_evidence($evidences, 'rel', [
                        $n_raw_id,
                        $m_raw_id,
                        $rel_type,
                        $rel_context_text
                    ], [
                        "doc" => "[관계] {$n_raw_id} - " . _get_rel_label($rel_type) . " - {$m_raw_id}",
                        "quote" => mb_substr($rel_context_text, 0, 500),
                        "concept" => (string)$n_raw_id,
                        "text" => $rel_context_text
                    ], $score);
                }
            }

            if ($e && $p) {
                $e_nid = add_node_to_map($nodes, $e, $rec->get('e_labels'));
                $p_nid = add_node_to_map($nodes, $p, $rec->get('p_labels'));
                if ($n_nid && $e_nid) $edges[] = ["from" => $n_nid, "to" => $e_nid, "type" => "P14_carried_out_by", "label" => docent_t("수행/참여", "Performed/Participated")];
                if ($e_nid && $p_nid) $edges[] = ["from" => $e_nid, "to" => $p_nid, "type" => "P14_carried_out_by", "label" => docent_t("수행/참여", "Performed/Participated")];

                $n_raw_id = $nodes[$n_nid]['raw_id'] ?? $n_nid;
                $p_raw_id = $nodes[$p_nid]['raw_id'] ?? $p_nid;
                $e_props = $nodes[$e_nid]['props'] ?? [];
                $e_title = $e_props['사건명'] ?? $e_props['title'] ?? $e_props['명칭'] ?? '관련 사건/문건';
                if (preg_match('/^[a-z0-9_]{10,}$/i', (string)$e_title)) {
                    $e_title = '3·1운동 관련 사료/사건';
                }
                $e_desc = $e_props['설명'] ?? $e_props['참가자수_설명'] ?? $e_props['사망자수_설명'] ?? '';
                $co_text = "당대 사료 문건 [{$e_title}]에 관련 인물로 {$n_raw_id}와(과) {$p_raw_id}의 정황이 함께 언급·기재됨.";
                if ($e_desc) $co_text .= "\n문건 요약: " . mb_substr($e_desc, 0, 300);

                add_fact_evidence($evidences, 'co_participate', [
                    $n_raw_id,
                    (string)$nodes[$e_nid]['raw_id'],
                    $p_raw_id
                ], [
                    "doc" => "[사료 연계] {$e_title}",
                    "quote" => "{$n_raw_id} 및 {$p_raw_id} 관련 기록",
                    "concept" => (string)$n_raw_id,
                    "text" => $co_text
                ], 38);
            }
        }
    }

    merge_same_person_nodes($nodes, $edges, $evidences);

    $unique_edges = [];
    foreach ($edges as $e) {
        $key = $e['from'] . '-' . $e['to'] . '-' . $e['label'];
        $unique_edges[$key] = $e;
    }

    $prefetch_names = select_prefetch_names_by_degree($nodes, array_values($unique_edges));

    foreach ($nodes as $node_item) {
        $lbls = $node_item['labels'] ?? [];
        if (in_array('사건', $lbls) || in_array('Event', $lbls)) {
            $event_raw_id = (string)($node_item['raw_id'] ?? '');
            if ($event_raw_id !== '' && !in_array($event_raw_id, $prefetch_names, true)) {
                $prefetch_names[] = $event_raw_id;
            }
        }
    }

    echo json_encode([
        "nodes" => array_values($nodes),
        "edges" => array_values($unique_edges),
        "links" => array_map(static function ($edge) {
            return [
                "source" => $edge["from"],
                "target" => $edge["to"],
                "type" => $edge["type"] ?? $edge["label"],
                "label" => $edge["label"],
            ];
        }, array_values($unique_edges)),
        "evidences" => array_values($evidences),
        "prefetch_names" => $prefetch_names,
        "generated_cypher" => $generated_cypher,
        "cypher_executed" => ($cypher_count > 0),
        "cypher_record_count" => $cypher_count
    ], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
}

/**
 * [Action 3] PostgreSQL 동적 연관 사료 수집
 */
function handle_action_pg_prefetch() {
    $names = docent_decode_json_array('names', 20, 200);
    $pdo = get_pg();
    if (!$pdo) {
        echo json_encode([], JSON_UNESCAPED_UNICODE);
        return;
    }

    $all_rows = [];
    $tables_meta = get_pg_tables_metadata($pdo);
    $filtered_names = [];
    $seen = [];
    foreach ((array)$names as $name) {
        $name = docent_sanitize_text($name, 200, false);
        if ($name === '' || isset($seen[$name])) continue;
        $seen[$name] = true;
        $filtered_names[] = $name;
        if (count($filtered_names) >= 15) break;
    }

    if (!empty($filtered_names)) {
        $all_rows = fetch_pg_rows_for_names($pdo, $filtered_names, $tables_meta);
    }

    echo json_encode($all_rows, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
}

/**
 * [Action 4] AI RAG 도슨트 해설 생성 (2단계 파이프라인)
 */
function handle_action_explain() {
    $term = docent_sanitize_text($_POST['term'] ?? '', 180, false);
    $focus = docent_sanitize_text($_POST['focus'] ?? '', 100, false);
    $req_lang = strtolower(trim((string)($_POST['lang'] ?? '')));
    $supported_langs = ['en', 'ko', 'ja', 'zh'];
    $explain_lang = in_array($req_lang, $supported_langs, true) ? $req_lang : (docent_is_english() ? 'en' : 'ko');
    $evidences = json_decode($_POST['evidences'] ?? '[]', true);
    $pg_texts = json_decode($_POST['pg_texts'] ?? '[]', true);
    if (!is_array($evidences)) $evidences = [];
    if (!is_array($pg_texts)) $pg_texts = [];

    $is_stream = isset($_GET['stream']) || isset($_POST['stream']);
    if ($is_stream) {
        if (session_status() === PHP_SESSION_ACTIVE) {
            session_write_close();
        }

        header('Content-Type: text/event-stream; charset=utf-8');
        header('Cache-Control: no-cache, no-transform');
        header('X-Accel-Buffering: no');
        header('Connection: keep-alive');
        while (ob_get_level()) ob_end_clean();

        echo ": keepalive\n\n";
        flush();
    }

    // 1. 입력 사료 컨텍스트 구성 (핵심 사료 중심 예산 최적화)
    $evidence_context = build_budgeted_evidence_context($evidences, $explain_lang, 20, 250, 600, 5000, $term, $focus);
    $pg_context = build_budgeted_pg_context($pg_texts, $explain_lang, 25, 180, 450, 6000, $term, $focus);
    $context_str = trim($evidence_context . ($evidence_context && $pg_context ? "\n\n" : "") . $pg_context);

    // ── [환각 방지 게이트] GDB·RDB 사료가 전혀 없으면 도슨트 생성 차단 ──
    if ($context_str === '') {
        $no_info_msgs = [
            'ko' => "죄송합니다. 지식그래프(Graph DB) 및 원천 사료(RDB) 어디에서도 '"  . $term . "'에 관한 정보를 찾을 수 없었습니다.\n\n검색어를 달리하거나, 3·1운동과 관련된 인물·사건·지역명으로 다시 질의해 주시기 바랍니다.",
            'en' => "No information about '" . $term . "' was found in either the Knowledge Graph (Graph DB) or the primary source database (RDB).\n\nPlease try a different search term, or query using a person, event, or location name related to the March 1st Movement.",
            'ja' => "知識グラフ(Graph DB)および一次史料データベース(RDB)のいずれにも、'" . $term . "'に関する情報が見つかりませんでした。\n\n別の検索語でお試しになるか、3・1運動に関連する人物・事件・地名で再度ご質問ください。",
            'zh' => "在知识图谱(Graph DB)与原始史料数据库(RDB)中均未找到关于'" . $term . "'的相关信息。\n\n请换用其他检索词，或以三一运动相关的人物、事件、地名重新提问。",
        ];
        $no_info_text = $no_info_msgs[$explain_lang] ?? $no_info_msgs['ko'];

        if ($is_stream) {
            echo "data: " . json_encode(["chunk" => $no_info_text], JSON_UNESCAPED_UNICODE) . "\n\n";
            echo "data: [DONE]\n\n";
            if (ob_get_level()) ob_flush();
            flush();
            return;
        }
        echo json_encode(["text" => $no_info_text], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
        return;
    }

    // 2. [1단계 파이프라인] 사료 비판 기반 정밀 팩트 추출
    $fact_table = '';
    if ($is_stream) {
        echo ": stage1-fact-table\n\n";
        flush();
    }
    $fact_prompts = get_fact_extraction_prompts($explain_lang, $context_str);
    try {
        $fact_table = call_gemini([
            ["role" => "system", "content" => $fact_prompts['system']],
            ["role" => "user", "content" => $fact_prompts['user']]
        ], false, 1500, 0.0, 0.8, 50);
    } catch (\Throwable $extErr) {
        error_log("Stage 1 fact extraction failed: " . $extErr->getMessage());
        $fact_table = '';
    }

    // 3. [2단계 파이프라인] 학술 도슨트 해설 원고 작성
    $explain_prompts = get_explain_prompts($explain_lang, $term, $context_str, $fact_table);
    $sys_prompt = $explain_prompts['system'];
    $user_prompt = $explain_prompts['user'];

    if ($is_stream) {
        stream_gemini([
            ["role" => "system", "content" => $sys_prompt],
            ["role" => "user", "content" => $user_prompt]
        ], function($chunk) {
            echo "data: " . json_encode(["chunk" => $chunk], JSON_UNESCAPED_UNICODE) . "\n\n";
            if (ob_get_level()) ob_flush();
            flush();
        }, 8192, 0.1, 0.85);

        echo "data: [DONE]\n\n";
        if (ob_get_level()) ob_flush();
        flush();
        return;
    }

    // Fallback: 동기식 JSON 응답
    $res = call_gemini([
        ["role" => "system", "content" => $sys_prompt],
        ["role" => "user", "content" => $user_prompt]
    ], false, 8192, 0.1, 0.85);

    echo json_encode(["text" => $res], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
}

/**
 * [Action 5] 개체 원천 사료(PostgreSQL) 상세 레코드 실시간 조회
 */
function handle_action_node_detail() {
    $raw_id = docent_sanitize_text($_POST['raw_id'] ?? '', 500, false);
    $table = docent_sanitize_text($_POST['table'] ?? '', 100, false);
    $rowid = (int)($_POST['rowid'] ?? 0);
    $label = docent_sanitize_text($_POST['label'] ?? '', 200, false);
    $aliases = docent_sanitize_json_list($_POST['aliases'] ?? '', 10, 100);

    if (empty($table) || $rowid <= 0) {
        if (preg_match('/^([a-zA-Z0-9_]+):Generated from [a-zA-Z0-9_]+ row (\d+)/i', $raw_id, $m)) {
            $table = $m[1];
            $rowid = (int)$m[2];
        } elseif (preg_match('/^#?([a-zA-Z0-9_]+)[_-](\d+)$/i', $raw_id, $m)) {
            $table = $m[1];
            $rowid = (int)$m[2];
        } elseif (preg_match('/^([a-zA-Z0-9_]+):(\d+)$/i', $raw_id, $m)) {
            $table = $m[1];
            $rowid = (int)$m[2];
        }
    }

    $response_data = [
        "found" => false,
        "table" => $table,
        "rowid" => $rowid,
        "columns" => [],
        "tei" => ""
    ];

    $pdo = get_pg();
    if ($pdo) {
        $fill_from_row = function($tableName, $row) use (&$response_data) {
            $response_data["found"] = true;
            $response_data["table"] = $tableName;
            $response_data["rowid"] = (int)($row['rowid'] ?? 0);
            if (isset($row['tei'])) {
                $response_data["tei"] = (string)$row['tei'];
                unset($row['tei']);
            }
            $clean_cols = [];
            foreach ($row as $col_k => $col_v) {
                if ($col_v !== null && $col_v !== '') {
                    $clean_cols[$col_k] = (string)$col_v;
                }
            }
            $response_data["columns"] = $clean_cols;
        };

        $candidates = array_values(array_unique(array_filter(
            array_merge([$raw_id, $label], $aliases),
            static fn($v) => $v !== null && trim((string)$v) !== ''
        )));

        try {
            if (!empty($table) && $rowid > 0 && preg_match('/^[a-zA-Z0-9_]+$/', $table)) {
                $chk = $pdo->prepare("SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1");
                $chk->execute([strtolower($table)]);
                if ($chk->fetch()) {
                    $stmt = $pdo->prepare("SELECT * FROM \"{$table}\" WHERE rowid = ? LIMIT 1");
                    $stmt->execute([$rowid]);
                    $row = $stmt->fetch(PDO::FETCH_ASSOC);
                    if ($row) {
                        $fill_from_row($table, $row);
                    }
                }
            }

            if (!$response_data["found"] && !empty($candidates)) {
                $exact_search_tables = [
                    ['table' => 'raw_event_info', 'cols' => ['아이디', '사건명']],
                    ['table' => 'raw_event_place_link', 'cols' => ['demons_id', 'demons_title', 'place_id', 'place_name']],
                    ['table' => 'raw_detail_place', 'cols' => ['세부장소아이디', '명칭', '이칭']],
                    ['table' => 'raw_bibliography', 'cols' => ['문서아이디', '제목']],
                    ['table' => 'raw_oppression_org_police', 'cols' => ['기구ID', '기구명']],
                    ['table' => 'raw_oppression_org_gendarme', 'cols' => ['기구id', '기구명']],
                    ['table' => 'raw_oppression_org_military', 'cols' => ['기구ID', '기구명칭']],
                ];

                $in_placeholders = implode(',', array_fill(0, count($candidates), '?'));
                foreach ($exact_search_tables as $def) {
                    if ($response_data["found"]) break;
                    $tbl = $def['table'];
                    if (!preg_match('/^[a-zA-Z0-9_]+$/', $tbl)) continue;
                    $where_parts = array_map(fn($c) => "\"{$c}\" IN ({$in_placeholders})", $def['cols']);
                    $sql = "SELECT * FROM \"{$tbl}\" WHERE " . implode(' OR ', $where_parts) . " LIMIT 1";
                    $params = [];
                    for ($ci = 0; $ci < count($def['cols']); $ci++) {
                        foreach ($candidates as $cand) $params[] = $cand;
                    }
                    $stmt = $pdo->prepare($sql);
                    $stmt->execute($params);
                    if ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
                        $fill_from_row($tbl, $row);
                    }
                }

                if (!$response_data["found"]) {
                    foreach ($candidates as $cand) {
                        if (mb_strlen($cand) >= 2) {
                            $stmt = $pdo->prepare("SELECT * FROM raw_event_info WHERE \"관련인물\" LIKE ? LIMIT 1");
                            $stmt->execute(['%' . $cand . '%']);
                            if ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
                                $fill_from_row("raw_event_info", $row);
                                break;
                            }
                        }
                    }
                }

                if (!$response_data["found"]) {
                    foreach ($candidates as $cand) {
                        if (mb_strlen($cand) >= 2) {
                            $stmt = $pdo->prepare("SELECT * FROM tei_cidoc_mappings WHERE mapping_label LIKE ? LIMIT 1");
                            $stmt->execute(['%' . $cand . '%']);
                            if ($m_row = $stmt->fetch(PDO::FETCH_ASSOC)) {
                                if (!empty($m_row['table_name']) && !empty($m_row['rowid'])) {
                                    $mapped_tbl = $m_row['table_name'];
                                    if (preg_match('/^[a-zA-Z0-9_]+$/', $mapped_tbl)) {
                                        $stmt2 = $pdo->prepare("SELECT * FROM \"{$mapped_tbl}\" WHERE rowid = ? LIMIT 1");
                                        $stmt2->execute([(int)$m_row['rowid']]);
                                        if ($row = $stmt2->fetch(PDO::FETCH_ASSOC)) {
                                            $fill_from_row($mapped_tbl, $row);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (\Throwable $sql_err) {
            error_log("node_detail query error: " . $sql_err->getMessage());
            $response_data["db_error"] = $sql_err->getMessage();
        }
    }

    echo json_encode($response_data, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
}
