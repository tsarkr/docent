<?php
/**
 * 3.1 운동 역사 도슨트 - Advanced Robust RAG Version (Node Click & Info Panel Edition)
 * Handles Interactive Graph node clicking to show localized metadata and evidences.
 */

require_once __DIR__ . '/vendor/autoload.php';

use Dotenv\Dotenv;
use Laudis\Neo4j\ClientBuilder;
use Laudis\Neo4j\Authentication\Authenticate;

if (session_status() === PHP_SESSION_NONE) {
    $forwarded_proto = trim(explode(',', (string)($_SERVER['HTTP_X_FORWARDED_PROTO'] ?? ''))[0]);
    $is_https = (!empty($_SERVER['HTTPS']) && $_SERVER['HTTPS'] !== 'off') || strtolower($forwarded_proto) === 'https';
    ini_set('session.use_strict_mode', '1');
    session_cache_limiter('nocache');
    session_set_cookie_params([
        'lifetime' => 0,
        'path' => '/',
        'httponly' => true,
        'secure' => $is_https,
        'samesite' => $is_https ? 'None' : 'Lax'
    ]);
    session_start();
}
if (empty($_SESSION['docent_csrf'])) {
    $_SESSION['docent_csrf'] = bin2hex(random_bytes(32));
}
header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');
header('Pragma: no-cache');

// 1. Load Environment Variables
if (file_exists(__DIR__ . '/.env')) {
    $dotenv = Dotenv::createImmutable(__DIR__);
    $dotenv->load();
}

// Utility to get config safely
function get_cfg($key, $default = '') {
    $val = getenv($key);
    if ($val !== false) return trim($val, " \t\n\r\0\x0B\"'");
    if (isset($_ENV[$key])) return trim($_ENV[$key], " \t\n\r\0\x0B\"'");
    if (isset($_SERVER[$key])) return trim($_SERVER[$key], " \t\n\r\0\x0B\"'");
    return $default;
}

function docent_lang() {
    return defined('DOCENT_LANG') ? DOCENT_LANG : 'ko';
}

function docent_is_english() {
    return docent_lang() === 'en';
}

function docent_t($ko, $en) {
    return docent_is_english() ? $en : $ko;
}

function docent_sanitize_text($value, $max_len = 255, $allow_newlines = false) {
    $text = trim((string)($value ?? ''));
    if ($text === '') return '';
    $text = preg_replace('/[\x00-\x1F\x7F]+/u', ' ', $text);
    if (!$allow_newlines) {
        $text = preg_replace('/\s+/u', ' ', $text);
    }
    $text = str_replace(["\r", "\n"], ' ', $text);
    if ($max_len > 0) {
        $text = mb_substr($text, 0, $max_len, 'UTF-8');
    }
    return trim($text);
}

function docent_decode_json_array($key, $max_items = 50, $max_len = 500) {
    $raw = $_POST[$key] ?? '[]';
    if (!is_string($raw)) {
        return [];
    }

    $decoded = json_decode($raw, true);
    if (!is_array($decoded)) {
        return [];
    }

    $items = [];
    foreach ($decoded as $item) {
        if (is_array($item)) {
            $item = json_encode($item, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
        }
        $val = docent_sanitize_text((string)$item, $max_len, false);
        if ($val !== '') {
            $items[] = $val;
        }
        if (count($items) >= $max_items) {
            break;
        }
    }
    return $items;
}

function docent_valid_identifier($value, $default = '') {
    $sanitized = preg_replace('/[^A-Za-z0-9_]/', '', (string)($value ?? $default));
    return $sanitized !== '' ? $sanitized : $default;
}

function docent_check_same_origin() {
    $origin = trim((string)($_SERVER['HTTP_ORIGIN'] ?? ''));
    if ($origin === '') return true;

    $allowed_origins = array_filter(array_map('trim', explode(',', get_cfg('DOCENT_ALLOWED_ORIGINS', 'https://gyungmin.tsar.kr'))));
    foreach ($allowed_origins as $allowed_origin) {
        if (strcasecmp($origin, rtrim($allowed_origin, '/')) === 0) {
            return true;
        }
    }

    $origin_parts = parse_url($origin);
    if (!is_array($origin_parts) || !isset($origin_parts['host'])) {
        return false;
    }

    $origin_scheme = strtolower((string)($origin_parts['scheme'] ?? ''));
    $origin_host = strtolower((string)$origin_parts['host']);
    $origin_port = isset($origin_parts['port']) ? (int)$origin_parts['port'] : null;

    $forwarded_host = trim((string)($_SERVER['HTTP_X_FORWARDED_HOST'] ?? ''));
    if ($forwarded_host !== '' && str_contains($forwarded_host, ',')) {
        $forwarded_host = trim(explode(',', $forwarded_host)[0]);
    }

    $host_candidates = [];
    foreach ([$forwarded_host, $_SERVER['HTTP_HOST'] ?? '', $_SERVER['SERVER_NAME'] ?? ''] as $candidate) {
        $value = strtolower(trim((string)$candidate));
        if ($value !== '') {
            $host_candidates[] = preg_replace('/:\d+$/', '', $value);
        }
    }
    $host_candidates = array_values(array_unique(array_filter($host_candidates, static fn($v) => $v !== null && $v !== '')));
    $proto_header = trim((string)($_SERVER['HTTP_X_FORWARDED_PROTO'] ?? ''));
    if ($proto_header !== '' && str_contains($proto_header, ',')) {
        $proto_header = trim(explode(',', $proto_header)[0]);
    }
    $https = strtolower((string)($_SERVER['HTTPS'] ?? ''));
    $request_scheme = strtolower((string)($_SERVER['REQUEST_SCHEME'] ?? ''));
    if ($request_scheme === '') {
        $request_scheme = ($https !== '' && $https !== 'off') ? 'https' : 'http';
    }
    if ($proto_header !== '') {
        $request_scheme = strtolower($proto_header);
    }

    $request_port = isset($_SERVER['SERVER_PORT']) ? (int)$_SERVER['SERVER_PORT'] : null;
    if (isset($_SERVER['HTTP_X_FORWARDED_PORT']) && is_numeric($_SERVER['HTTP_X_FORWARDED_PORT'])) {
        $request_port = (int)$_SERVER['HTTP_X_FORWARDED_PORT'];
    }

    $is_local_origin = in_array($origin_host, ['localhost', '127.0.0.1', '::1'], true);
    $is_local_host = in_array('localhost', $host_candidates, true) || in_array('127.0.0.1', $host_candidates, true) || in_array('::1', $host_candidates, true);
    if ($is_local_origin && $is_local_host) {
        return true;
    }

    if (!in_array($origin_host, $host_candidates, true)) {
        return false;
    }

    if ($origin_scheme !== '' && $request_scheme !== '' && $origin_scheme !== $request_scheme) {
        return false;
    }

    if ($origin_port !== null && $request_port !== null && $origin_port !== $request_port) {
        return false;
    }

    return true;
}

function docent_check_csrf() {
    $request_token = $_SERVER['HTTP_X_CSRF_TOKEN'] ?? ($_POST['csrf_token'] ?? '');
    return is_string($request_token)
        && isset($_SESSION['docent_csrf'])
        && hash_equals((string)$_SESSION['docent_csrf'], $request_token);
}

function docent_rate_limit($action) {
    $ip = (string)($_SERVER['REMOTE_ADDR'] ?? 'unknown');
    $key = hash('sha256', $ip . '|' . $action);
    $limit = $action === 'explain' ? 5 : 30;
    $file = rtrim(sys_get_temp_dir(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . 'docent-rate-' . $key;
    $handle = @fopen($file, 'c+');
    if (!$handle) return true;

    $allowed = true;
    if (flock($handle, LOCK_EX)) {
        $timestamps = json_decode(stream_get_contents($handle) ?: '[]', true);
        if (!is_array($timestamps)) $timestamps = [];
        $now = time();
        $timestamps = array_values(array_filter($timestamps, static fn($timestamp) => is_int($timestamp) && $timestamp > ($now - 60)));
        if (count($timestamps) >= $limit) {
            $allowed = false;
        } else {
            $timestamps[] = $now;
            ftruncate($handle, 0);
            rewind($handle);
            fwrite($handle, json_encode($timestamps));
        }
        flock($handle, LOCK_UN);
    }
    fclose($handle);
    return $allowed;
}

function infer_focus($term, $is_english = false) {
    $text = trim((string)$term);
    if ($text === '') {
        return $is_english ? 'Person' : '인물';
    }

    $normalized = strtolower($text);
    $person_markers = ['인물','사람','인명','주인공','영웅','독립운동가','지도자','장군','장수','leader','person','hero','revolutionary','activist','politician','general'];
    $event_markers = ['사건','시위','운동','봉기','폭동','재판','전쟁','전투','혁명','incident','event','protest','movement','uprising','trial','war','battle'];
    $place_markers = ['장소','지역','도시','마을','곳','서울','부산','대구','인천','광주','대전','울산','경기','강원','충청','전라','경상','제주','만주','한양','평양','place','city','town','region','location','province','capital'];
    $org_markers = ['기관','단체','정부','청','학교','협회','조직','군대','경찰','헌병','academy','company','organization','government','police','army','military','association'];

    foreach ($person_markers as $marker) {
        if (strpos($normalized, $marker) !== false) {
            return $is_english ? 'Person' : '인물';
        }
    }
    foreach ($event_markers as $marker) {
        if (strpos($normalized, $marker) !== false) {
            return $is_english ? 'Event' : '사건';
        }
    }
    foreach ($place_markers as $marker) {
        if (strpos($normalized, $marker) !== false) {
            return $is_english ? 'Place' : '장소';
        }
    }
    foreach ($org_markers as $marker) {
        if (strpos($normalized, $marker) !== false) {
            return $is_english ? 'Organization' : '기관';
        }
    }

    if (preg_match('/^[\p{Hangul}]{2,4}$/u', $text) || preg_match('/^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*$/', $text)) {
        return $is_english ? 'Person' : '인물';
    }

    if (preg_match('/\b(운동|시위|사건|전쟁|봉기|재판)\b/u', $text)) {
        return $is_english ? 'Event' : '사건';
    }

    if (preg_match('/\b(서울|부산|대구|인천|광주|대전|울산|경기|강원|충청|전라|경상|제주|만주|한양|평양)\b/u', $text)) {
        return $is_english ? 'Place' : '장소';
    }

    return $is_english ? 'Person' : '인물';
}

function normalize_focus($focus, $is_english = false) {
    $value = trim((string)($focus ?? ''));
    if ($value === '') {
        return infer_focus('', $is_english);
    }

    $normalized = strtolower($value);
    $map = [
        'person' => 'Person',
        'people' => 'Person',
        '인물' => '인물',
        'event' => 'Event',
        '사건' => '사건',
        'place' => 'Place',
        '장소' => '장소',
        'organization' => 'Organization',
        'org' => 'Organization',
        '기관' => '기관',
    ];

    if (isset($map[$normalized])) {
        return $map[$normalized];
    }

    return $is_english ? 'Person' : '인물';
}

// 2. API Logic (AJAX Handlers)
if (isset($_GET['ajax'])) {
    header('Content-Type: application/json; charset=utf-8');
    header('X-Content-Type-Options: nosniff');
    header('X-Frame-Options: SAMEORIGIN');

    $action = strtolower((string)($_GET['ajax'] ?? ''));
    $allowed_actions = ['analyze', 'graph', 'pg_prefetch', 'explain'];
    if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
        http_response_code(405);
        echo json_encode(['error' => 'Method not allowed.'], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
        exit;
    }
    if (!in_array($action, $allowed_actions, true)) {
        http_response_code(400);
        echo json_encode(['error' => '허용되지 않은 요청입니다.'], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
        exit;
    }
    if (!docent_check_same_origin()) {
        error_log(sprintf('Docent request rejected: invalid origin for action %s', $action));
        http_response_code(403);
        echo json_encode(['error' => '접속 출처가 올바르지 않습니다. 페이지를 새로고침해 주세요.'], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
        exit;
    }
    if (!docent_check_csrf()) {
        error_log(sprintf('Docent request rejected: invalid CSRF token for action %s', $action));
        http_response_code(403);
        echo json_encode(['error' => '보안 토큰이 만료되었습니다. 페이지를 새로고침해 주세요.'], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
        exit;
    }
    if (!docent_rate_limit($action)) {
        http_response_code(429);
        header('Retry-After: 60');
        echo json_encode(['error' => '요청이 너무 많습니다. 잠시 후 다시 시도하세요.'], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
        exit;
    }
    
    try {
        // [Action 1] 의도 분석
        if ($action === 'analyze') {
            $term = docent_sanitize_text($_POST['term'] ?? '', 180, false);
            $initial_focus = infer_focus($term, docent_is_english());

            if (docent_is_english()) {
                $system_prompt = "You are a specialist analyst for historical knowledge search. Analyze the user's query and reply only in the following JSON format.\n"
                               . "{\n"
                               . "  \"intent\": \"ENTITY_SEARCH\",\n"
                               . "  \"keywords\": [\"" . addslashes($term) . "\"],\n"
                               . "  \"focus\": \"" . addslashes($initial_focus) . "\",\n"
                               . "  \"explanation\": \"Keyword-based analysis completed\"\n"
                               . "}\n"
                               . "Choose the most relevant focus from Person, Event, Place, or Organization based on the question. Return JSON only, without additional explanation.";
            } else {
                $system_prompt = "당신은 역사 지식 검색을 위한 전문 분석가입니다. 사용자의 질문을 분석하여 다음 JSON 형식으로만 답하십시오.\n"
                               . "{\n"
                               . "  \"intent\": \"ENTITY_SEARCH\",\n"
                               . "  \"keywords\": [\"" . addslashes($term) . "\"],\n"
                               . "  \"focus\": \"" . addslashes($initial_focus) . "\",\n"
                               . "  \"explanation\": \"검색어 기반 분석 수행\"\n"
                               . "}\n"
                               . "focus는 질문 내용에 가장 적합한 값인 '인물', '사건', '장소', '기관' 중 하나로 결정하십시오. 부가적인 설명 없이 JSON만 반환하십시오.";
            }

            $res = call_gemini([
                ["role" => "system", "content" => $system_prompt],
                ["role" => "user", "content" => (string)$term]
            ], true);
            
            $parsed = json_decode($res, true);
            $output = [
                "intent" => $parsed['intent'] ?? 'ENTITY_SEARCH',
                "keywords" => $parsed['keywords'] ?? [$term],
                "focus" => normalize_focus($parsed['focus'] ?? $initial_focus, docent_is_english()),
                "explanation" => $parsed['explanation'] ?? (docent_is_english() ? 'Analysis complete' : '분석 완료')
            ];
            
            echo json_encode($output, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
            exit;
        }

        // [Action 2] Neo4j 지식망 및 증거 탐색 (Closure Safe & Edge Fix)
        if ($action === 'graph') {
            $term = docent_sanitize_text($_POST['term'] ?? '', 180, false);
            $keywords = docent_decode_json_array('keywords', 20, 180);
            if (empty($keywords)) $keywords = [$term];
            $keywords = array_values(array_filter(array_map(fn($kw) => docent_sanitize_text($kw, 180, false), $keywords)));
            if (empty($keywords)) $keywords = [$term];
            
            $client = get_neo4j();
            $nodes = [];
            $edges = [];
            $evidences = [];
            $found_ids = [];

            $search_query = "
                CALL db.index.fulltext.queryNodes('namesIndex', \$term) YIELD node, score
                RETURN DISTINCT node as n, labels(node) as labels, score
                ORDER BY score DESC
                LIMIT 50
            ";

            $fallback_search_query = "
                MATCH (n)
                WHERE any(k IN ['명칭','한글독음','한글명칭','제목','사건명','id','name','title','uid'] 
                          WHERE n[k] IS NOT NULL AND toLower(toString(n[k])) CONTAINS toLower(\$term))
                RETURN DISTINCT n, labels(n) as labels
                LIMIT 50
            ";
            
            foreach ($keywords as $kw) {
                if (empty($kw)) continue;
                try {
                    $res1 = $client->run($search_query, ['term' => $kw]);
                } catch (Throwable $fulltextError) {
                    $res1 = $client->run($fallback_search_query, ['term' => $kw]);
                }
                
                foreach ($res1 as $record) {
                    $node = $record->get('n');
                    $labels_iterable = $record->get('labels');
                    
                    // 안전한 프로퍼티 및 라벨 추출
                    $props = [];
                    if ($node && method_exists($node, 'getProperties')) {
                        foreach ($node->getProperties() as $k => $v) {
                            $props[$k] = is_scalar($v) ? $v : (string)$v;
                        }
                    }
                    
                    $labels = [];
                    if (is_iterable($labels_iterable)) {
                        foreach ($labels_iterable as $l) $labels[] = (string)$l;
                    }
                    
                    $node_id = $props['uid'] ?? $props['id'] ?? $props['명칭'] ?? $props['name'] ?? $props['title'] ?? 'unknown';
                    $found_ids[] = (string)$node_id;
                    
                    add_node_to_map($nodes, $node, $labels_iterable);

                    if (in_array('문건', $labels) || in_array('사료', $labels)) {
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

            if (!empty($found_ids)) {
                $found_ids = array_unique($found_ids);
                $graph_query = "
                    MATCH (n)
                    WHERE any(k IN ['id','명칭','한글독음','한글명칭','name','title','uid'] WHERE n[k] IS NOT NULL AND toString(n[k]) IN \$search_ids)
                    OPTIONAL MATCH (n)-[r]-(m)
                    WITH n, r, m, labels(n) as n_labels, labels(m) as m_labels
                    OPTIONAL MATCH (n)-[:P14_carried_out_by|P11_had_participant]-(e:사건)-[:P14_carried_out_by|P11_had_participant]-(p:인물)
                    WHERE n:인물 AND n <> p
                    RETURN DISTINCT n, r, m, n_labels, m_labels, e, p, labels(e) as e_labels, labels(p) as p_labels, r.context as rel_context
                    LIMIT 100
                ";

                $res2 = $client->run($graph_query, ['search_ids' => $found_ids]);
                foreach ($res2 as $rec) {
                    $n = $rec->get('n'); $m = $rec->get('m'); $r = $rec->get('r');
                    $e = $rec->get('e'); $p = $rec->get('p');
                    $rel_context = $rec->get('rel_context');

                    $n_nid = add_node_to_map($nodes, $n, $rec->get('n_labels'));
                    $m_nid = $m ? add_node_to_map($nodes, $m, $rec->get('m_labels')) : null;

                    if ($r && $n_nid && $m_nid) {
                        $rel_type = method_exists($r, 'getType') ? $r->getType() : '연결';
                        $edges[] = ["from" => $n_nid, "to" => $m_nid, "label" => _get_rel_label($rel_type)];

                        $rel_context_text = trim(mb_substr((string)($rel_context ?? ''), 0, 1000));
                        if ($rel_context_text !== '') {
                            $n_raw_id = $nodes[$n_nid]['raw_id'] ?? $n_nid;
                            $m_raw_id = $nodes[$m_nid]['raw_id'] ?? $m_nid;
                            add_fact_evidence($evidences, 'rel', [
                                $n_raw_id,
                                $m_raw_id,
                                $rel_type,
                                $rel_context_text
                            ], [
                                "doc" => "[EDGE] {$n_raw_id} - {$rel_type} - {$m_raw_id}",
                                "quote" => mb_substr($rel_context_text, 0, 500),
                                "concept" => (string)$n_raw_id,
                                "text" => $rel_context_text
                            ], 30);
                        }
                    }

                    if ($e && $p) {
                        $e_nid = add_node_to_map($nodes, $e, $rec->get('e_labels'));
                        $p_nid = add_node_to_map($nodes, $p, $rec->get('p_labels'));
                        if ($n_nid && $e_nid) $edges[] = ["from" => $n_nid, "to" => $e_nid, "label" => docent_t("수행/참여", "Performed/Participated")];
                        if ($e_nid && $p_nid) $edges[] = ["from" => $e_nid, "to" => $p_nid, "label" => docent_t("수행/참여", "Performed/Participated")];
                    }
                }
            }

            $unique_edges = [];
            foreach ($edges as $e) {
                $key = $e['from'] . '-' . $e['to'] . '-' . $e['label'];
                $unique_edges[$key] = $e;
            }

            $prefetch_names = select_prefetch_names_by_degree($nodes, array_values($unique_edges));

            echo json_encode([
                "nodes" => array_values($nodes),
                "edges" => array_values($unique_edges),
                "evidences" => array_values($evidences),
                "prefetch_names" => $prefetch_names
            ], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
            exit;
        }

        // [Action 3] PostgreSQL 동적 연관 사료 수집
        if ($action === 'pg_prefetch') {
            $names = docent_decode_json_array('names', 20, 200);
            $pdo = get_pg();
            if (!$pdo) { echo json_encode([], JSON_UNESCAPED_UNICODE); exit; }

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
            exit;
        }

// [Action 4] AI RAG 도슨트 해설 생성
        if ($action === 'explain') {
            $term = docent_sanitize_text($_POST['term'] ?? '', 180, false);
            $explain_lang = in_array(strtolower((string)($_POST['lang'] ?? '')), ['en', 'ko'], true) ? strtolower((string)$_POST['lang']) : (docent_is_english() ? 'en' : 'ko');
            $use_english = ($explain_lang === 'en');
            $evidences = json_decode($_POST['evidences'] ?? '[]', true);
            $pg_texts = json_decode($_POST['pg_texts'] ?? '[]', true);
            if (!is_array($evidences)) $evidences = [];
            if (!is_array($pg_texts)) $pg_texts = [];
            $evidence_context = build_budgeted_evidence_context($evidences, $use_english, 16, 220, 420, 4200);
            $pg_context = build_budgeted_pg_context($pg_texts, $use_english, 10, 140, 240, 1600);
            $context_str = trim($evidence_context . ($evidence_context && $pg_context ? "\n\n" : "") . $pg_context);

            if ($use_english) {
                $sys_prompt = "You are a professional historical docent synthesizing claim-level evidence. "
                            . "Synthesize claim-level evidence faithfully, keep uncertainty explicit, and write only in English. "
                            . "Provide a complete, well-structured, and fully finished narrative without cutting off sentences.";
                
                $user_prompt = "You are a history docent following a GraphRAG-style claim synthesis process. Explain about '{$term}'.\n"
                             . "- Distinguish verified facts, inferences, and contested points.\n"
                             . "- Prioritize high-degree graph entities when establishing the narrative backbone.\n"
                             . "- Cite concrete evidence details from edge contexts and node evidence.\n"
                             . "- Avoid emotional or exaggerated language and keep a neutral academic tone.\n"
                             . "- Clearly state evidence limits when claims are weak.\n"
                             . "- Write COMPLETELY IN ENGLISH and translate Korean source details into English.\n\n"
                             . "[Sources / PG Evidence]\n{$context_str}";

                $res = call_gemini([
                    ["role" => "system", "content" => $sys_prompt],
                    ["role" => "user", "content" => $user_prompt]
                ], false, 4096);
            } else {
                $sys_prompt = "당신은 역사 지식망(GraphRAG)을 기반으로 사료와 사실을 분석하는 전문 역사 도슨트입니다. "
                            . "한국어 해설만 작성하되, 중간에 끊기지 않도록 완결된 문장 구조로 끝까지 작성하십시오.";

                $user_prompt = "다음 사료 및 지식그래프 근거를 종합하여 '{$term}'에 대한 완결된 역사 해설을 작성해 주십시오.\n\n"
                             . "- 서론, 본론, 결론을 갖춘 온전한 해설문 형태로 작성합니다.\n"
                             . "- 사실/추정/논쟁 지점을 구분해 서술합니다.\n"
                             . "- 연결 차수가 높은 핵심 개체를 우선 서사 축으로 삼습니다.\n"
                             . "- 엣지 문맥과 노드 사료에서 확인되는 구체적인 사료 근거를 제시합니다.\n"
                             . "- 감정적·과장 표현을 피하고 중립적 학술 어조를 유지합니다.\n"
                             . "- 근거가 부족한 부분은 명확히 한계를 밝힙니다.\n\n"
                             . "[사료/PG 근거]\n{$context_str}";

                $res = call_gemini([
                    ["role" => "system", "content" => $sys_prompt],
                    ["role" => "user", "content" => $user_prompt]
                ], false, 4096);
            }

            echo json_encode(["text" => $res], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
            exit;
        }
    } catch (Throwable $e) {
        error_log(sprintf('Docent AJAX failure [%s]: %s in %s:%d', $action, $e->getMessage(), $e->getFile(), $e->getLine()));
        http_response_code(500);
        echo json_encode([
            "error" => "서버 내부 오류가 발생했습니다."
        ], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
        exit;
    }
}

// Infrastructure Functions
function get_neo4j() {
    return ClientBuilder::create()
        ->withDriver('default', get_cfg('NEO4J_URI', 'bolt://127.0.0.1:7687'), Authenticate::basic(get_cfg('NEO4J_USER', 'neo4j'), get_cfg('NEO4J_PASSWORD')))
        ->build();
}

function get_pg() {
    try {
        $host = get_cfg('PG_HOST', '127.0.0.1');
        $port = get_cfg('PG_PORT', 5432);
        $dbname = get_cfg('PG_DATABASE', 'historical');
        $user = get_cfg('PG_USER', 'postgres');
        $pass = get_cfg('PG_PASSWORD', '');
        
        if (empty($pass)) return null;
        $dsn = "pgsql:host={$host};port={$port};dbname={$dbname}";
        return new PDO($dsn, $user, $pass, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    } catch (Exception $e) {
        return null;
    }
}

function call_gemini($msgs, $is_json = false, $max_tokens = null) {
    $api_key = get_cfg('GEMINI_API_KEY');
    if (!$api_key) {
        $api_key = get_cfg('API_KEY');
    }
    if (!$api_key) return $is_json ? '{"explanation":"API Key missing"}' : docent_t("API Key가 설정되지 않았습니다.", "API key is not configured.");

    $model = trim(get_cfg('GEMINI_MODEL', 'gemini-3.5-flash-lite'));
    if (!preg_match('/^[A-Za-z0-9._-]+$/', $model)) {
        return $is_json ? '{"explanation":"Invalid model configuration"}' : "AI 모델 설정이 올바르지 않습니다.";
    }
    $url = "https://generativelanguage.googleapis.com/v1beta/models/{$model}:generateContent";

    $system_prompt = "";
    $contents = [];
    foreach ($msgs as $m) {
        if ($m['role'] === 'system') {
            $system_prompt = $m['content'];
        } else {
            $role = ($m['role'] === 'assistant') ? 'model' : 'user';
            $contents[] = [
                "role" => $role,
                "parts" => [["text" => $m['content']]]
            ];
        }
    }

    $payload = [
        "contents" => $contents,
        "generationConfig" => [
            "temperature" => 0.0,
            "maxOutputTokens" => $max_tokens ? (int)max(1, min(8192, $max_tokens)) : 4096,
        ]
    ];

    if ($system_prompt !== "") {
        $payload["system_instruction"] = [
            "parts" => [["text" => $system_prompt]]
        ];
    }

    if ($is_json) {
        $payload["generationConfig"]["response_mime_type"] = "application/json";
    }

    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE),
        CURLOPT_HTTPHEADER => ["Content-Type: application/json", "x-goog-api-key: {$api_key}"],
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_TIMEOUT => 60,
        CURLOPT_SSL_VERIFYPEER => true,
        CURLOPT_SSL_VERIFYHOST => 2,
    ]);

    $res = curl_exec($ch);
    $curl_errno = curl_errno($ch);
    $curl_error = curl_error($ch);
    $http_code = (int)curl_getinfo($ch, CURLINFO_RESPONSE_CODE);

    if ($res === false || $curl_errno !== 0 || $http_code !== 200) {
        $provider_message = '';
        $error_data = json_decode((string)$res, true);
        if (is_array($error_data['error'] ?? null)) {
            $provider_message = docent_sanitize_text($error_data['error']['message'] ?? '', 240, false);
        }
        error_log(sprintf('Gemini request failed: HTTP %d, cURL %d, %s, provider: %s', $http_code, $curl_errno, $curl_error, $provider_message));
        $safe_message = $provider_message !== '' ? "Gemini 오류: {$provider_message}" : "AI 요청에 실패했습니다. (HTTP {$http_code})";
        return $is_json
            ? json_encode(["explanation" => $safe_message], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE)
            : $safe_message;
    }

    $data = json_decode($res, true);
    
    // 복수 parts를 안전하게 전체 결합하여 누락 방지
    $content = '';
    if (isset($data['candidates'][0]['content']['parts']) && is_array($data['candidates'][0]['content']['parts'])) {
        foreach ($data['candidates'][0]['content']['parts'] as $part) {
            if (isset($part['text'])) {
                $content .= $part['text'];
            }
        }
    }

    if (trim($content) === '') {
        return $is_json ? '{"explanation":"Empty response"}' : "AI 응답 본문이 비어 있습니다.";
    }
    return $content;
}


function clamp_int($value, $min, $max) {
    return max($min, min($max, (int)$value));
}

function dynamic_prefetch_ratio($node_count) {
    if ($node_count <= 10) return 0.40;
    if ($node_count >= 30) return 0.30;
    return 0.40 - (($node_count - 10) * (0.10 / 20.0));
}

function select_prefetch_names_by_degree($nodes, $edges) {
    $node_list = array_values($nodes);
    $node_count = count($node_list);
    if ($node_count === 0) return [];

    $degree = [];
    foreach ($node_list as $n) {
        $degree[$n['id']] = 0;
    }
    foreach ($edges as $e) {
        if (isset($degree[$e['from']])) $degree[$e['from']]++;
        if (isset($degree[$e['to']])) $degree[$e['to']]++;
    }

    usort($node_list, function ($a, $b) use ($degree) {
        $da = $degree[$a['id']] ?? 0;
        $db = $degree[$b['id']] ?? 0;
        if ($da === $db) {
            return strcmp((string)($a['raw_id'] ?? $a['id']), (string)($b['raw_id'] ?? $b['id']));
        }
        return $db <=> $da;
    });

    $ratio = dynamic_prefetch_ratio($node_count);
    $target = clamp_int((int)round($node_count * $ratio), 5, 15);
    $target = min($node_count, $target);

    $names = [];
    foreach ($node_list as $n) {
        $raw = trim((string)($n['raw_id'] ?? ''));
        if ($raw === '' || isset($names[$raw])) continue;
        $names[$raw] = true;
        if (count($names) >= $target) break;
    }
    return array_keys($names);
}

function add_fact_evidence(&$evidences, $prefix, $hash_parts, $payload, $max_count = 30) {
    if (count($evidences) >= $max_count) return false;
    $hash_source = implode('|', array_map(fn($v) => (string)$v, $hash_parts));
    $key = "{$prefix}-" . sha1($hash_source);
    if (isset($evidences[$key])) return false;
    $evidences[$key] = $payload;
    return true;
}

function normalize_whitespace_text($text) {
    $text = preg_replace('/\s+/u', ' ', (string)$text);
    return trim($text ?? '');
}

function build_budgeted_evidence_context($evidences, $use_english, $max_items, $min_chars, $max_chars, $total_char_budget) {
    $lines = [];
    $used_chars = 0;
    foreach ((array)$evidences as $e) {
        if (count($lines) >= $max_items) break;
        if (($total_char_budget - $used_chars) < ($min_chars + 80)) break;

        $doc = mb_substr(normalize_whitespace_text($e['doc'] ?? ''), 0, 120);
        $text = normalize_whitespace_text($e['text'] ?? '');
        if ($text === '') continue;

        $remaining = $total_char_budget - $used_chars;
        $body_cap = max($min_chars, min($max_chars, $remaining - 80));
        $body = mb_substr($text, 0, $body_cap);
        $line = $use_english
            ? "- Document: {$doc}\n  Evidence: {$body}\n"
            : "- 문서: {$doc}\n  근거: {$body}\n";

        $line_len = mb_strlen($line);
        if ($line_len > $remaining) {
            $trimmed_cap = max(120, $remaining - 80);
            $body = mb_substr($text, 0, $trimmed_cap);
            $line = $use_english
                ? "- Document: {$doc}\n  Evidence: {$body}\n"
                : "- 문서: {$doc}\n  근거: {$body}\n";
            $line_len = mb_strlen($line);
            if ($line_len > $remaining) break;
        }

        $lines[] = $line;
        $used_chars += $line_len;
    }
    return implode('', $lines);
}

function build_budgeted_pg_context($pg_texts, $use_english, $max_items, $min_chars, $max_chars, $total_char_budget) {
    $lines = [];
    $used_chars = 0;
    foreach ((array)$pg_texts as $t) {
        if (count($lines) >= $max_items) break;
        if (($total_char_budget - $used_chars) < ($min_chars + 20)) break;

        $text = normalize_whitespace_text($t);
        if ($text === '') continue;

        $remaining = $total_char_budget - $used_chars;
        $body_cap = max($min_chars, min($max_chars, $remaining - 20));
        $line = "- " . mb_substr($text, 0, $body_cap);
        $line_len = mb_strlen($line);
        if ($line_len > $remaining) {
            $line = "- " . mb_substr($text, 0, max(120, $remaining - 5));
            $line_len = mb_strlen($line);
            if ($line_len > $remaining) break;
        }
        $lines[] = $line;
        $used_chars += $line_len;
    }

    if (empty($lines)) return '';
    $header = $use_english ? "[PG Evidence]\n" : "[PG 근거]\n";
    return $header . implode("\n", $lines);
}

function add_node_to_map(&$map, $node, $labels_iterable) {
    if (!$node) return null;
    
    $props = [];
    if (method_exists($node, 'getProperties')) {
        foreach ($node->getProperties() as $k => $v) {
            $props[$k] = is_scalar($v) ? $v : (is_iterable($v) ? json_encode($v, JSON_UNESCAPED_UNICODE) : (string)$v);
        }
    }
    
    $labels_list = [];
    if (is_iterable($labels_iterable)) {
        foreach ($labels_iterable as $l) {
            $labels_list[] = (string)$l;
        }
    }
    
    $raw_id = $props['uid'] ?? $props['id'] ?? $props['명칭'] ?? $props['name'] ?? $props['title'] ?? 'unknown';
    $nid = 'n' . substr(sha1((string)$raw_id), 0, 12);
    
    if (!isset($map[$nid])) {
        $reading = $props['한글독음'] ?? '';
        $base = $props['제목'] ?? $props['명칭'] ?? $props['사건명'] ?? $raw_id;
        $label_text = ($reading && $base != $reading) ? "{$base} ({$reading})" : $base;

        $color = "#999999"; $icon = "";
        $lstr = implode(' ', $labels_list);
        if (strpos($lstr, '문건') !== false || strpos($lstr, '사료') !== false) { $color = "#F7A01F"; $icon = "📜\n"; }
        elseif (strpos($lstr, '인물') !== false) { $color = "#2563EB"; $icon = "👤\n"; }
        elseif (strpos($lstr, '사건') !== false) { $color = "#DC2626"; $icon = "🔥\n"; }
        elseif (strpos($lstr, '장소') !== false) { $color = "#16A34A"; $icon = "📍\n"; }
        elseif (strpos($lstr, '기관') !== false) { $color = "#7C3AED"; $icon = "🏢\n"; }

        $map[$nid] = [
            "id" => $nid,
            "label" => $icon . mb_substr((string)$label_text, 0, 20),
            "raw_id" => (string)$raw_id,
            "labels" => $labels_list,
            "color" => ["background" => $color, "border" => $color, "highlight" => ["background" => $color, "border" => "#333"]],
            "shape" => "box",
            "font" => ["color" => "#000", "size" => 14, "multi" => true],
            "borderWidth" => 2,
            "shadow" => true
        ];
    }
    return $nid;
}

function _get_rel_label($rtype) {
    $ko = ["P14_carried_out_by" => "수행(참여)", "P7_took_place_at" => "발생 장소", "P152_has_parent" => "가족 관계", "foaf:knows" => "동지/지인", "foaf:member" => "소속 기구", "P11_had_participant" => "참여 인물", "P108_has_produced" => "생성/저작", "P102_has_title" => "명칭/제목", "소속" => "소속"];
    $en = ["P14_carried_out_by" => "Performed/Participated", "P7_took_place_at" => "Location", "P152_has_parent" => "Family relation", "foaf:knows" => "Comrade/Acquaintance", "foaf:member" => "Affiliated organization", "P11_had_participant" => "Participant", "P108_has_produced" => "Created/Produced", "P102_has_title" => "Title", "소속" => "Affiliation"];
    return docent_is_english() ? ($en[$rtype] ?? $rtype) : ($ko[$rtype] ?? $rtype);
}

function get_pg_tables_metadata($pdo) {
    $cache_dir = realpath(__DIR__);
    $cache_file = ($cache_dir !== false ? $cache_dir : __DIR__) . '/.pg_meta_cache.json';
    $cache_ttl = 60 * 60 * 24;
    if (is_file($cache_file) && is_readable($cache_file)) {
        $cache_raw = @file_get_contents($cache_file);
        if (is_string($cache_raw) && $cache_raw !== '') {
            $cache = json_decode($cache_raw, true);
            if (
                is_array($cache)
                && isset($cache['cached_at'], $cache['meta'])
                && (time() - (int)$cache['cached_at'] < $cache_ttl)
                && is_array($cache['meta'])
            ) {
                return $cache['meta'];
            }
        }
    }

    $stmt = $pdo->query("SELECT table_schema, table_name FROM information_schema.columns WHERE column_name = 'tei' AND table_schema NOT IN ('pg_catalog', 'information_schema') GROUP BY table_schema, table_name");
    $tables = $stmt->fetchAll(PDO::FETCH_ASSOC);
    $meta = [];

    foreach ($tables as $t) {
        $schema = (string)($t['table_schema'] ?? '');
        $table = (string)($t['table_name'] ?? '');
        if (!preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $schema) || !preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $table)) {
            continue;
        }

        $s1 = $pdo->prepare("SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position ASC LIMIT 1");
        $s1->execute([$schema, $table]);
        $id_col = (string)($s1->fetchColumn() ?: 'rowid');
        if (!preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $id_col)) {
            $id_col = 'rowid';
        }

        $s2 = $pdo->prepare("SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND data_type IN ('character varying','text','character') ORDER BY ordinal_position ASC");
        $s2->execute([$schema, $table]);
        $text_cols = array_values(array_filter(array_map(function ($col) {
            $col = (string)$col;
            return preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $col) ? $col : null;
        }, $s2->fetchAll(PDO::FETCH_COLUMN))));

        foreach (['명칭', '사건명', '제목'] as $p) {
            if (($idx = array_search($p, $text_cols)) !== false) {
                array_splice($text_cols, $idx, 1); array_unshift($text_cols, $p);
            }
        }
        $meta[] = ['schema' => $schema, 'table' => $table, 'id_col' => $id_col, 'text_cols' => array_slice($text_cols, 0, 6)];
    }

    $cache_payload = json_encode(['cached_at' => time(), 'meta' => $meta], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
    if ($cache_payload !== false && is_dir(($cache_dir !== false ? $cache_dir : __DIR__))) {
        @file_put_contents($cache_file, $cache_payload, LOCK_EX);
    }

    return $meta;
}

function resolve_pg_row_node_id($row, $id_col, $search_cols, $names) {
    $id_value = isset($row[$id_col]) ? (string)$row[$id_col] : '';
    foreach ($names as $name) {
        if ($id_value !== '' && $id_value === (string)$name) {
            return (string)$name;
        }
    }

    $haystack_parts = [];
    foreach ($search_cols as $col) {
        if (isset($row[$col]) && $row[$col] !== null) {
            $haystack_parts[] = (string)$row[$col];
        }
    }
    $haystack = mb_strtolower(implode(' ', $haystack_parts));
    foreach ($names as $name) {
        $needle = mb_strtolower((string)$name);
        if ($needle !== '' && mb_strpos($haystack, $needle) !== false) {
            return (string)$name;
        }
    }

    return $names[0] ?? '';
}

function fetch_pg_rows_for_name($pdo, $name, $tables_meta) {
    return fetch_pg_rows_for_names($pdo, [$name], $tables_meta);
}

function fetch_pg_rows_for_names($pdo, $names, $tables_meta) {
    $names = array_values(array_filter(array_unique(array_map(fn($n) => trim((string)$n), (array)$names))));
    if (empty($names)) return [];

    $out = [];
    foreach ($tables_meta as $m) {
        $schema = docent_valid_identifier($m['schema'] ?? '', 'public');
        $table = docent_valid_identifier($m['table'] ?? '', '');
        $id_col = docent_valid_identifier($m['id_col'] ?? '', 'id');
        if ($table === '') continue;

        $fq = ($schema !== '' && $schema !== 'public') ? "\"{$schema}\".\"{$table}\"" : "\"{$table}\"";
        $search_cols = [$id_col, 'tei'];
        foreach ((array)($m['text_cols'] ?? []) as $c) {
            $safe_col = docent_valid_identifier($c, '');
            if ($safe_col !== '' && !in_array($safe_col, $search_cols, true)) $search_cols[] = $safe_col;
        }

        $params = [];
        $in_placeholders = implode(',', array_fill(0, count($names), '?'));
        $id_match = "\"{$id_col}\"::text IN ({$in_placeholders})";
        foreach ($names as $name) {
            $params[] = $name;
        }

        $text_clauses = [];
        foreach ($search_cols as $c) {
            if ($c === $id_col) continue;
            foreach ($names as $name) {
                $text_clauses[] = "\"{$c}\"::text ILIKE ?";
                $params[] = "%{$name}%";
            }
        }
        $text_where = empty($text_clauses) ? '' : " OR (" . implode(" OR ", $text_clauses) . ")";
        $q = "SELECT * FROM {$fq} WHERE ({$id_match}{$text_where}) LIMIT 25";
        
        try {
            $stmt = $pdo->prepare($q);
            $stmt->execute($params);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            foreach ($rows as $row) {
                $snippets = [];
                foreach ($row as $k => $v) if ($k !== $id_col && $k !== 'tei') $snippets[$k] = $v !== null ? strval($v) : '';
                $out[] = [
                    'table' => $table,
                    'schema' => $schema,
                    'rowid' => $row[$id_col] ?? null,
                    'id_col' => $id_col,
                    'match_cols' => $search_cols,
                    'tei' => $row['tei'] ?? null,
                    'snippets' => $snippets,
                    'node_id' => resolve_pg_row_node_id($row, $id_col, $search_cols, $names)
                ];
            }
        } catch (Exception $e) { continue; }
    }
    return $out;
}
?>
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>3.1 운동 역사 도슨트</title>
    <!-- Google tag (gtag.js) -->
    <script async src="https://www.googletagmanager.com/gtag/js?id=G-GRES32XWER"></script>
    <script>
        window.dataLayer = window.dataLayer || [];
        function gtag(){dataLayer.push(arguments);} 
        gtag('js', new Date());
        gtag('config', 'G-GRES32XWER', {
            send_page_view: true,
            page_path: window.location.pathname
        });
    </script>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.0/font/bootstrap-icons.css">
    <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
    <style>
        body { background-color: #f0f2f6; font-family: 'Pretendard', sans-serif; }
        .sidebar { height: 100vh; overflow-y: auto; background: #fff; border-right: 1px solid #dee2e6; padding: 2rem 1.5rem; }
        .main-content { padding: 2rem; height: 100vh; overflow-y: auto; }
        #graph { height: 460px; background: #fff; border-radius: 15px; border: 1px solid #dee2e6; margin-bottom: 1.5rem; cursor: pointer; }
        .docent-card { background: #fff; border-radius: 15px; border: 1px solid #dee2e6; padding: 2rem; box-shadow: 0 4px 6px rgba(0,0,0,0.05); }
        .legend-item { display: flex; align-items: center; gap: 8px; font-size: 0.85rem; font-weight: bold; }
        .legend-color { width: 14px; height: 14px; border-radius: 3px; }
        #status-log { font-size: 0.75rem; border-top: 1px solid #eee; padding-top: 1rem; margin-top: 2rem; }
        /* ✨ 새로 추가된 노드 정보 패널 스타일 */
        #node-info-panel { border-radius: 12px; box-shadow: 0 4px 10px rgba(0,0,0,0.08); border: none; overflow: hidden; }
        #node-info-panel .card-header { border-bottom: 2px solid #dee2e6; background-color: #f8f9fa; }
    </style>
</head>
<body>
<div class="container-fluid">
    <div class="row">
        <div class="col-md-2 sidebar">
            <h4 class="mb-4 fw-bold text-primary">🇰🇷 3.1 운동 역사 도슨트</h4>
            <div class="mb-4">
                <label class="form-label small fw-bold text-muted">통합 검색</label>
                <div class="input-group input-group-sm">
                    <input type="text" id="q" class="form-control" placeholder="인물, 사건, 장소...">
                    <button onclick="performSearch()" class="btn btn-primary">탐색</button>
                </div>
            </div>
            
            <div class="mb-4">
                <label class="form-label small fw-bold text-muted">추천 키워드</label>
                <div class="d-grid gap-2">
                    <?php foreach (["유관순", "안중근", "3.1 운동", "시위", "임시정부"] as $kw): ?>
                        <button onclick="setQuery('<?= $kw ?>')" class="btn btn-outline-secondary btn-sm text-start keyword-btn" data-ko-label="<?= $kw ?>">📌 <?= $kw ?></button>
                    <?php endforeach; ?>
                </div>
            </div>

            <div id="analysis-box" class="card bg-light p-3 mb-3 small" style="display:none">
                <div class="fw-bold text-primary mb-2"><i class="bi bi-cpu"></i> AI 질의 분석</div>
                <div class="mb-1">🎯 <strong>의도:</strong> <span id="intent-val"></span></div>
                <div class="mb-1">🔍 <strong>초점:</strong> <span id="focus-val"></span></div>
                <div class="text-muted mt-1" id="explanation-val" style="font-size:0.75rem;"></div>
            </div>

            <div class="card bg-warning-subtle p-3 mb-3 small border-1 border-warning" style="border-radius: 10px;">
                <div class="fw-bold mb-2"><i class="bi bi-info-circle"></i> 📋 기본정보</div>
                <div class="text-muted small" style="line-height: 1.5;">
                    <p class="mb-1"><strong>제작자:</strong> jo.gyungmin@gmail.com</p>
                    <p class="mb-1"><strong>💰 AI API 비용:</strong> 개인 감당 / 적절한 사용 부탁</p>
                    <p class="mb-0"><strong>🌐 English:</strong> Explanation only</p>
                </div>
            </div>

            <div id="status-log" class="text-muted">
                <div id="status-text">준비됨.</div>
            </div>
        </div>

        <div class="col-md-10 main-content">
            <h3 id="search-title" class="mb-4 fw-bold">역사를 탐색해 보세요.</h3>
            <div class="row">
                <div class="col-lg-6">
                    <div class="d-flex justify-content-between align-items-center mb-2 flex-wrap gap-2">
                        <div class="small fw-bold text-muted">지식그래프</div>
                        <button type="button" class="btn btn-sm btn-outline-primary" onclick="centerGraph()">
                            <i class="bi bi-bullseye"></i> 가운데로 다시 불러오기
                        </button>
                    </div>
                    <div id="graph"></div>
                    <div class="d-flex justify-content-center gap-3 mb-4 flex-wrap">
                        <div class="legend-item"><div class="legend-color" style="background: #F7A01F;"></div> 사료</div>
                        <div class="legend-item"><div class="legend-color" style="background: #2563EB;"></div> 인물</div>
                        <div class="legend-item"><div class="legend-color" style="background: #DC2626;"></div> 사건</div>
                        <div class="legend-item"><div class="legend-color" style="background: #16A34A;"></div> 장소</div>
                        <div class="legend-item"><div class="legend-color" style="background: #7C3AED;"></div> 기관</div>
                    </div>
                    
                    <div id="node-info-panel" class="card mb-4" style="display:none;">
                        <div class="card-header fw-bold text-primary">
                            <i class="bi bi-info-circle-fill"></i> 선택된 노드 상세 정보
                        </div>
                        <div class="card-body" id="node-info-content">
                            </div>
                    </div>
                </div>

                <div class="col-lg-6">
                    <div class="docent-card">
                        <div class="d-flex justify-content-between align-items-center mb-4 flex-wrap gap-2">
                            <h5 class="fw-bold m-0"><i class="bi bi-chat-dots-fill text-primary"></i> 도슨트 해설</h5>
                            <button id="explainBtn" onclick="generateExplanation()" class="btn btn-sm btn-primary" style="display:none;">
                                <i class="bi bi-stars"></i> 해설 생성
                            </button>
                        </div>
                        <div class="border rounded-3 bg-light p-3 mb-3">
                            <div class="d-flex justify-content-between align-items-center flex-wrap gap-2 mb-3">
                                <div class="small fw-bold text-muted">해설 언어</div>
                                <div class="btn-group btn-group-sm" role="group" aria-label="해설 언어 토글">
                                    <input type="radio" class="btn-check" name="explanation-lang" id="explanation-lang-ko" value="ko" <?php echo docent_is_english() ? '' : 'checked'; ?>>
                                    <label class="btn btn-outline-primary" for="explanation-lang-ko">한국어</label>
                                    <input type="radio" class="btn-check" name="explanation-lang" id="explanation-lang-en" value="en" <?php echo docent_is_english() ? 'checked' : ''; ?>>
                                    <label class="btn btn-outline-primary" for="explanation-lang-en">English</label>
                                </div>
                            </div>
                            <div id="explanation-content" class="text-secondary" style="line-height: 1.7; min-height: 100px;">
                                검색어를 입력하고 탐색 버튼을 누르면 인프라가 작동합니다.
                            </div>
                        </div>
                        
                        <div id="rag-section" style="display:none">
                            <h6 class="fw-bold mt-5 mb-3 border-top pt-3"><i class="bi bi-journal-text"></i> 수집된 사료/PG 근거</h6>
                            <div id="rag-evidence-list" class="small" style="max-height: 320px; overflow-y: auto;"></div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>

<script>
let network = null;
let lastEvidences = [];
let pgPrefetchTexts = [];
let currentNodes = []; // ✨ 검색된 노드 목록 보관용

function restoreSuggestedKeywords() {
    if (document.documentElement.lang !== 'en') return;
    document.querySelectorAll('.keyword-btn').forEach((button) => {
        const label = button.dataset.koLabel || button.textContent.replace(/^📌\s*/, '');
        button.textContent = `📌 ${label}`;
    });
}

restoreSuggestedKeywords();

function setQuery(q) {
    document.getElementById('q').value = q;
    performSearch();
}

function trackAnalyticsEvent(name, params = {}) {
    if (typeof gtag === 'function') {
        gtag('event', name, params);
    }
}

async function api(act, data = {}) {
    const fd = new FormData();
    for (let k in data) fd.append(k, data[k]);
    const csrfToken = '<?= htmlspecialchars($_SESSION['docent_csrf'], ENT_QUOTES, 'UTF-8') ?>';
    fd.append('csrf_token', csrfToken);
    const response = await fetch(`?ajax=${act}`, {
        method: 'POST',
        body: fd,
        credentials: 'same-origin',
        headers: { 'X-CSRF-Token': csrfToken }
    });
    
    const rawText = await response.text();
    
    if (!response.ok) {
        try {
            const err = JSON.parse(rawText);
            throw new Error('[' + act + ' 단계 HTTP ' + response.status + '] ' + (err.error || '서버 오류'));
        } catch(e) {
            if (e instanceof Error && e.message.startsWith('[' + act + ' 단계 HTTP ')) throw e;
            throw new Error('[' + act + ' 단계 HTTP ' + response.status + '] ' + rawText.substring(0, 150));
        }
    }
    
    try {
        return JSON.parse(rawText);
    } catch (e) {
        throw new Error('[' + act + ' ' + '단계 JSON 파싱 실패] ' + '원문: ' + rawText.substring(0, 150));
    }
}

async function performSearch() {
    const term = document.getElementById('q').value.trim();
    if (term.length < 2) return alert('2글자 이상 입력하세요.');

    trackAnalyticsEvent('search', { search_term: term });

    document.getElementById('analysis-box').style.display = 'none';
    document.getElementById('explainBtn').style.display = 'none';
    document.getElementById('rag-section').style.display = 'none';
    document.getElementById('node-info-panel').style.display = 'none'; // 초기화 시 노드 정보 패널 숨김
    pgPrefetchTexts = [];
    currentNodes = [];

    setStatus('<span class="spinner-border spinner-border-sm"></span> 의도 분석 중...');
    document.getElementById('search-title').innerText = `'${term}' ` + '분석 중...';
    
    try {
        const analysis = await api('analyze', { term });
        document.getElementById('intent-val').innerText = analysis.intent;
        document.getElementById('focus-val').innerText = analysis.focus;
        document.getElementById('explanation-val').innerText = analysis.explanation;
        document.getElementById('analysis-box').style.display = 'block';

        const keywords = analysis.keywords || [term];
        
        setStatus('<span class="spinner-border spinner-border-sm"></span> 지식망 및 사료 검색...');
        const graphData = await api('graph', { term, keywords: JSON.stringify(keywords) });
        lastEvidences = graphData.evidences;
        currentNodes = graphData.nodes; // ✨ 노드 데이터를 보관
        
        draw(graphData.nodes, graphData.edges);

        const nodeNames = Array.isArray(graphData.prefetch_names) && graphData.prefetch_names.length > 0
            ? graphData.prefetch_names
            : graphData.nodes.map(n => n.raw_id);
        if (nodeNames.length > 0) {
            setStatus('<span class="spinner-border spinner-border-sm"></span> PostgreSQL 사료 원문 연동 중...');
            const pgRows = await api('pg_prefetch', { names: JSON.stringify(nodeNames) });
            
            pgPrefetchTexts = pgRows.map(r => {
                let sStr = [];
                for(let k in r.snippets) { if(r.snippets[k]) sStr.push(`${k}: ${r.snippets[k]}`); }
                return `[${r.schema}.${r.table}] node=${r.node_id} rowid=${r.rowid} :: ${sStr.join('; ')}`;
            });
        }

        document.getElementById('search-title').innerText = `'${term}' ` + '지식망 탐색 완료';
        document.getElementById('explanation-content').innerHTML = "지식 구조 및 근거 수집 완료. <strong>'해설 생성'</strong> 버튼을 클릭하면 RAG 분석이 시작됩니다.";
        document.getElementById('explainBtn').style.display = 'inline-block';

        let ragHtml = '';
        lastEvidences.forEach(ev => {
            ragHtml += `<div class="p-2 mb-2 bg-light border-start border-warning border-3 rounded small">
                <div class="fw-bold text-dark">📜 ${ev.doc} (개체: ${ev.concept})</div>
                <div class="text-muted mt-1">${ev.text.substring(0, 300)}...</div>
            </div>`;
        });
        pgPrefetchTexts.forEach(txt => {
            ragHtml += `<div class="p-2 mb-2 bg-light border-start border-success border-3 rounded small">
                <div class="fw-bold text-dark"><i class="bi bi-database"></i> PostgreSQL 사료</div>
                <div class="text-muted mt-1">${txt}</div>
            </div>`;
        });

        if (ragHtml) {
            document.getElementById('rag-evidence-list').innerHTML = ragHtml;
            document.getElementById('rag-section').style.display = 'block';
        }
        setStatus('완료');

    } catch (e) {
        console.error(e);
        setStatus('<span class="text-danger"><i class="bi bi-exclamation-triangle"></i> ' + '에러: ' + e.message + '</span>');
        document.getElementById('explanation-content').innerHTML = "데이터 탐색 과정에서 실패했습니다. 에러 내용을 확인해 주세요.";
    }
}

async function generateExplanation() {
    const term = document.getElementById('q').value.trim();
    const langInput = document.querySelector('input[name="explanation-lang"]:checked');
    const lang = langInput ? langInput.value : 'ko';
    trackAnalyticsEvent('generate_explanation', {
        search_term: term,
        evidence_count: lastEvidences.length,
        pg_count: pgPrefetchTexts.length,
        explanation_lang: lang
    });
    document.getElementById('explanation-content').innerHTML = '<div class="text-center p-4"><div class="spinner-border text-primary" role="status"></div><br><small class="text-muted mt-2 d-inline-block">해설 생성중...</small></div>';
    
    try {
        const res = await api('explain', {
            term,
            lang,
            evidences: JSON.stringify(lastEvidences),
            pg_texts: JSON.stringify(pgPrefetchTexts)
        });
        document.getElementById('explanation-content').innerHTML = marked.parse(res.text);
    } catch (e) {
        document.getElementById('explanation-content').innerHTML = `<span class='text-danger'>해설 생성 중 오류: ${e.message}</span>`;
    }
}

function centerGraph() {
    if (!network) {
        setStatus('먼저 지식그래프를 탐색해 주세요.');
        return;
    }

    network.fit({
        animation: {
            duration: 600,
            easingFunction: 'easeInOutQuad'
        }
    });
}

function setStatus(html) {
    document.getElementById('status-text').innerHTML = html;
}

// ✨ 노드 정보를 패널에 렌더링하는 함수
function showNodeInfo(node) {
    const panel = document.getElementById('node-info-panel');
    const content = document.getElementById('node-info-content');
    
    // 기본 정보
    let html = `<h5 class="fw-bold mb-3 text-dark">${node.label.replace(/[\n📜👤🔥📍🏢]/g, '').trim()}</h5>`;
    html += `<div class="mb-1"><span class="badge bg-secondary me-2">개체 ID</span> <span class="text-muted">${node.raw_id}</span></div>`;
    
    if (node.labels && node.labels.length > 0) {
        html += `<div class="mb-3"><span class="badge bg-secondary me-2">속성</span> <span class="text-primary">${node.labels.join(', ')}</span></div>`;
    }

    // 관련된 Neo4j 사료 증거 찾기
    const relatedEvidences = lastEvidences.filter(ev => ev.concept === node.raw_id || ev.concept === node.id);
    if (relatedEvidences.length > 0) {
        html += `<hr><h6 class="fw-bold text-warning mb-2"><i class="bi bi-journal-bookmark-fill"></i> Neo4j 관련 사료 기록</h6>`;
        html += `<div style="max-height: 220px; overflow-y: auto; padding-right: 4px;">`;
        relatedEvidences.forEach(ev => {
            html += `<div class="mb-2 p-2 bg-light border rounded small" style="white-space: pre-wrap; word-break: break-word;">
                <strong class="text-dark">${ev.doc}</strong><br>
                <span class="text-muted">${ev.text.substring(0, 150)}...</span>
            </div>`;
        });
        html += `</div>`;
    }

    // 관련된 PostgreSQL 근거 텍스트 찾기
    const relatedPg = pgPrefetchTexts.filter(txt => txt.includes(`node=${node.raw_id}`));
    if (relatedPg.length > 0) {
        html += `<hr><h6 class="fw-bold text-success mb-2"><i class="bi bi-database-fill"></i> PostgreSQL 관련 근거</h6>`;
        html += `<div style="max-height: 220px; overflow-y: auto; padding-right: 4px;">`;
        relatedPg.forEach(txt => {
            // DB 출처 정보 강조 (예: [public.서지정보_260410])
            const parts = txt.split('::');
            const source = parts[0];
            const detail = parts[1] ? parts[1] : '';
            html += `<div class="mb-2 p-2 bg-light border rounded small">
                <strong class="text-success">${source.replace(/\[|\]/g, '')}</strong><br>
                <div class="text-muted" style="max-height: 120px; overflow-y: auto; white-space: pre-wrap; word-break: break-word;">${detail}</div>
            </div>`;
        });
        html += `</div>`;
    }

    if (relatedEvidences.length === 0 && relatedPg.length === 0) {
        html += `<div class="text-muted small mt-3"><i class="bi bi-info-circle"></i> 이 개체와 직접 연결된 사료나 추가 데이터가 없습니다.</div>`;
    }
    
    content.innerHTML = html;
    panel.style.display = 'block';
}

function draw(nodes, edges) {
    const container = document.getElementById('graph');
    const data = { nodes: new vis.DataSet(nodes), edges: new vis.DataSet(edges) };
    const options = {
        edges: { arrows: 'to', color: '#848484', font: { size: 11, align: 'middle' }, smooth: { type: 'continuous' } },
        physics: { enabled: true, repulsion: { nodeDistance: 240, centralGravity: 0.15 }, stabilization: { iterations: 120 } }
    };
    
    if (network) network.destroy();
    network = new vis.Network(container, data, options);

    // ✨ 마우스 클릭 이벤트 리스너 추가
    network.on("click", function(params) {
        if (params.nodes.length > 0) {
            const nodeId = params.nodes[0];
            const clickedNode = currentNodes.find(n => n.id === nodeId);
            if (clickedNode) {
                trackAnalyticsEvent('node_click', {
                    node_id: clickedNode.raw_id || nodeId,
                    node_labels: (clickedNode.labels || []).join('|')
                });
                showNodeInfo(clickedNode);
            }
        } else {
            // 빈 공간(배경) 클릭 시 패널 숨김
            document.getElementById('node-info-panel').style.display = 'none';
        }
    });
}
</script>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>