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
} elseif (file_exists(__DIR__ . '/.streamlit/secrets.toml')) {
    $secrets_content = file_get_contents(__DIR__ . '/.streamlit/secrets.toml');
    if ($secrets_content !== false) {
        if (preg_match_all('/^\s*([A-Za-z0-9_]+)\s*=\s*["\'](.*?)["\']\s*$/m', $secrets_content, $matches, PREG_SET_ORDER)) {
            foreach ($matches as $m) {
                $k = $m[1]; $v = $m[2];
                if (getenv($k) === false) {
                    putenv("{$k}={$v}");
                    $_ENV[$k] = $v;
                }
            }
        }
    }
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

function docent_escape_lucene($term) {
    $chars = ['\\', '+', '-', '&&', '||', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':', '/'];
    $escaped = ['\\\\', '\+', '\-', '\&&', '\||', '\!', '\(', '\)', '\{', '\}', '\[', '\]', '\^', '\"', '\~', '\*', '\?', '\:', '\/'];
    $term = str_replace($chars, $escaped, (string)$term);
    $term = trim($term);
    if (in_array(strtoupper($term), ['AND', 'OR', 'NOT'], true)) {
        $term = '"' . $term . '"';
    }
    return $term;
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

function docent_sanitize_json_list($value, $max_items = 50, $max_len = 500) {
    if (is_array($value)) {
        $decoded = $value;
    } else {
        $decoded = json_decode((string)$value, true);
    }
    if (!is_array($decoded)) return [];
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

// 1.5 CSRF Token Endpoint (Lightweight for headless frontends like docent.html)
if (isset($_GET['csrf']) || (isset($_GET['ajax']) && $_GET['ajax'] === 'csrf')) {
    header('Content-Type: application/json; charset=utf-8');
    header('X-Content-Type-Options: nosniff');
    header('X-Frame-Options: SAMEORIGIN');
    if (!docent_check_same_origin()) {
        http_response_code(403);
        echo json_encode(['error' => '접속 출처가 올바르지 않습니다.'], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
        exit;
    }
    echo json_encode(['csrf_token' => $_SESSION['docent_csrf']], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
    exit;
}

// 2. API Logic (AJAX Handlers)
if (isset($_GET['ajax'])) {
    header('Content-Type: application/json; charset=utf-8');
    header('X-Content-Type-Options: nosniff');
    header('X-Frame-Options: SAMEORIGIN');

    $action = strtolower((string)($_GET['ajax'] ?? ''));
    $allowed_actions = ['analyze', 'graph', 'pg_prefetch', 'explain', 'node_detail'];
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

            // 다국어 통합 Query Analyzer 프롬프트 (한국어/영어 분기 제거)
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

            // 새 스키마 필드 파싱 + 기존 키 호환 출력 (프런트엔드 및 graph 액션 하위 호환)
            $new_intent = $parsed['intent_type'] ?? 'ENTITY_SEARCH';
            $new_keywords = $parsed['search_keywords'] ?? [$term];
            $new_focus = normalize_focus($parsed['focus'] ?? $initial_focus, false); // 항상 한국어 focus
            $new_intent_ko = $parsed['analyzed_intent_ko'] ?? '검색어 기반 분석 수행';
            $new_resp_lang = $parsed['response_language'] ?? (docent_is_english() ? 'en' : 'ko');

            $output = [
                // 새 스키마 필드
                "intent_type" => $new_intent,
                "search_keywords" => $new_keywords,
                "analyzed_intent_ko" => $new_intent_ko,
                "response_language" => $new_resp_lang,
                // 기존 호환 필드 (graph 액션 등에서 사용)
                "intent" => $new_intent,
                "keywords" => $new_keywords,
                "focus" => $new_focus,
                "explanation" => $new_intent_ko
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
            
            // 불용어 및 질의 의도어 정의 (단독 검색 금지)
            static $graph_stopwords = [
                '시위', '만세', '만세시위', '만세운동', '독립만세', '독립운동', '운동',
                '과정', '배경', '사건', '전개', '역사', '활동', '내용', '결과',
                '영향', '의의', '기록', '사료', '원인', '설명', '관계', '인물',
                '장소', '지역', '단체', '조직', '관련', '조사', '보고', '개요',
                '3.1운동', '3·1운동', '삼일운동', '3.1', '3·1',
                '현황', '상황', '모습', '이유', '어떻게', '무엇', '누구', '언제',
                '어디', '대해', '대한', '통해', '통한', '당시', '이후', '이전',
                '알려줘', '설명해줘', '알고싶어', '알려주세요', '설명해주세요'
            ];

            // 광역 행정구역 및 도 단위 명칭
            static $broad_regions = [
                '수원', '서울', '경성', '경기', '경기도', '충남', '충북', '충청도',
                '전남', '전북', '전라도', '경남', '경북', '경상도', '강원', '강원도',
                '황해', '황해도', '평남', '평북', '평안도', '함남', '함북', '함경도'
            ];

            // 복합 질의어 및 추출 키워드 정제
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

            // 세부 고유명사(사강리, 제암리, 아우내 등)가 있으면 광역 지명(수원, 서울 등)은 완전히 제외
            if (!empty($specific_candidates)) {
                $expanded_keywords = $specific_candidates;
            } elseif (!empty($general_candidates)) {
                $expanded_keywords = $general_candidates;
            } else {
                $expanded_keywords = [$term];
            }

            $client = get_neo4j();
            $nodes = [];
            $edges = [];
            $evidences = [];
            $found_ids = [];
            $found_candidates = [];

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

            foreach ($expanded_keywords as $kw) {
                if (empty($kw)) continue;
                $escaped_kw = docent_escape_lucene($kw);
                if ($escaped_kw === '') continue;
                try {
                    $res1 = $client->run($search_query, ['term' => $escaped_kw]);
                } catch (Throwable $fulltextError) {
                    $res1 = $client->run($fallback_search_query, ['term' => $kw]);
                }
                
                foreach ($res1 as $record) {
                    $node = $record->get('n');
                    $labels_iterable = $record->get('labels');
                    
                    // add_node_to_map에서 프로퍼티/라벨을 일괄 추출 (중복 제거)
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
                        // ✨ 핵심 인물 노드 Evidence 수집 (유관순, 유중권 등 핵심 인물 사실 확보)
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
                // 카테시안 곱을 제거하고 인접 엣지와 사건 동반참여자를 안전하게 서브쿼리로 제한
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

                // 타임아웃 또는 서브쿼리 미지원 시 초고속 안전 fallback
                $fallback_graph_query = "
                    MATCH (n)
                    WHERE any(k IN ['id','명칭','한글독음','한글명칭','name','title','uid'] WHERE n[k] IS NOT NULL AND toString(n[k]) IN \$search_ids)
                    OPTIONAL MATCH (n)-[r]-(m)
                    RETURN DISTINCT n, labels(n) as n_labels, r, m, labels(m) as m_labels, r.context as rel_context, null as e, null as p, [] as e_labels, [] as p_labels
                    LIMIT 60
                ";

                try {
                    $res2 = $client->run($graph_query, ['search_ids' => $found_ids]);
                } catch (Throwable $queryErr) {
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

                        // ✨ 가족 관계(P152_has_parent 등) 엣지는 최고 우선순위(50)로 RAG 근거 등록
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

                        // 사건/문건 관련 인물 연계 Evidence 등록
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

            // ✨ 사건(Event) 노드의 ID(예: a_00351) 및 사건명을 prefetch_names에 포함하여 PostgreSQL 사료 결합
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

// [Action 4] AI RAG 도슨트 해설 생성 (2단계 파이프라인: 정밀 추출 -> 학술 합성)
        if ($action === 'explain') {
            $term = docent_sanitize_text($_POST['term'] ?? '', 180, false);
            $focus = docent_sanitize_text($_POST['focus'] ?? '', 100, false);
            $req_lang = strtolower(trim((string)($_POST['lang'] ?? '')));
            $supported_langs = ['en', 'ko', 'ja', 'zh'];
            $explain_lang = in_array($req_lang, $supported_langs, true) ? $req_lang : (docent_is_english() ? 'en' : 'ko');
            $use_english = ($explain_lang === 'en');
            $evidences = json_decode($_POST['evidences'] ?? '[]', true);
            $pg_texts = json_decode($_POST['pg_texts'] ?? '[]', true);
            if (!is_array($evidences)) $evidences = [];
            if (!is_array($pg_texts)) $pg_texts = [];

            $is_stream = isset($_GET['stream']) || isset($_POST['stream']);
            if ($is_stream) {
                // 세션 락 해제 (스트리밍 중 다른 AJAX 요청 블로킹 방지)
                if (session_status() === PHP_SESSION_ACTIVE) {
                    session_write_close();
                }

                header('Content-Type: text/event-stream; charset=utf-8');
                header('Cache-Control: no-cache, no-transform');
                header('X-Accel-Buffering: no');
                header('Connection: keep-alive');
                while (ob_get_level()) ob_end_clean();

                // 프록시/게이트웨이 타임아웃(HTTP 504) 방지를 위한 즉각적인 초기 SSE 핑 전송
                echo ": keepalive\n\n";
                flush();
            }

            // 1. 입력 사료 컨텍스트 구성 (노드 격벽 Scoping Envelope 및 질의 초점 필터링 적용)
            $evidence_context = build_budgeted_evidence_context($evidences, $explain_lang, 30, 250, 800, 12000, $term, $focus);
            $pg_context = build_budgeted_pg_context($pg_texts, $explain_lang, 40, 180, 500, 12000, $term, $focus);
            $context_str = trim($evidence_context . ($evidence_context && $pg_context ? "\n\n" : "") . $pg_context);

            // 2. [1단계 파이프라인] 사료 비판 기반 정밀 사건-장소-인물 팩트 추출 (In-Context Knowledge Table 구축)
            $fact_table = '';
            if (!empty($context_str)) {
                if ($is_stream) {
                    echo ": stage1-fact-table\n\n";
                    flush();
                }
                if ($explain_lang === 'en') {
                    $ext_sys = "You are an expert archival historian specializing in modern Korean history. "
                             . "Analyze the provided <SOURCE_EVIDENCE> blocks with strict source criticism. "
                             . "Extract a clean, verified fact table strictly without cross-attribution or regional mixing. "
                             . "Never blend figures or actions across different regions.";
                    $ext_user = "Extract a verified markdown table from the sources in English: [Source ID | Entity | Location/Region | Actual Actor | Key Fact (1-2 lines)].\n\n[Archival Envelopes]\n{$context_str}";
                } elseif ($explain_lang === 'ja') {
                    $ext_sys = "あなたは韓国近現代史および3・1運動の一次史料・公文書を専門とする歴史学者です。"
                             . "提供された <SOURCE_EVIDENCE> 史料ブロックを史料批判に基づき精緻に分析し、"
                             . "各史料の事実関係を地域・人物の混同や交差帰属なく、以下の日本語ファクト表として抽出してください。\n"
                             . "必ず各ブロックに明記された事実のみを日本語で記述し、他地域の人物や出来事を決して混入させないでください。";
                    $ext_user = "次の史料群から [史料ID | 대상エンティティ | 発生場所・地域 | 実際の行動人物 | 史料記録の核心事実 (1〜2行)] をMarkdown表形式で抽出してください（すべて日本語で記述し、韓国語の助詞や文を残さないでください）。\n\n[Archival Envelopes]\n{$context_str}";
                } elseif ($explain_lang === 'zh') {
                    $ext_sys = "您是精通韩国近现代史及三一运动一手史料分析的专业历史学者。"
                             . "请依据严格的史料批判方法分析所提供的 <SOURCE_EVIDENCE> 史料块，"
                             . "切勿跨区域混淆人物或事件，提取出客观准确的中文事实核查表。\n"
                             . "仅记录各史料块中明确记载的事实，严禁混入其他地区的人物或行动。";
                    $ext_user = "请从以下史料群中提取 [史料ID | 对象实体 | 发生地点/区域 | 实际行动人物 | 史料核心事实 (1~2行)] Markdown表格（全部使用规范简体中文描述，切勿残留韩语助词或句子）。\n\n[Archival Envelopes]\n{$context_str}";
                } else {
                    $ext_sys = "당신은 한국 근현대사 1차 사료 분석 전문가입니다. "
                             . "제공된 <SOURCE_EVIDENCE> 사료 블록들을 사료 비판적으로 정밀 분석하여, "
                             . "각 사료별 사실관계를 왜곡이나 교차 귀속(인물/지역 혼합) 없이 다음 정밀 팩트 표로 추출하십시오.\n"
                             . "반드시 각 블록에 명시된 사실만 기록하고, 타 지역 사건의 인물을 섞지 마십시오.";
                    $ext_user = "다음 사료군에서 [사료 ID | 대상 개체 | 발생 장소/지역 | 실제 행동 인물 | 사료에 기록된 핵심 팩트(1~2줄)] 표를 마크다운 표로 추출하십시오.\n\n[사료 원문 블록]\n{$context_str}";
                }

                try {
                    $fact_table = call_gemini([
                        ["role" => "system", "content" => $ext_sys],
                        ["role" => "user", "content" => $ext_user]
                    ], false, 1500, 0.0, 0.8, 12);
                } catch (Throwable $extErr) {
                    error_log("Stage 1 fact extraction failed: " . $extErr->getMessage());
                }
            }

            // 3. [2단계 파이프라인] 정제된 연결 정보 + 사료 격벽 원문 기반 도슨트 해설 원고 작성
            if ($explain_lang === 'en') {
                $sys_prompt = "# Role\n"
                            . "You are a 'Knowledge Graph-Based Historical Docent' specializing in the complex diffusion paths and hidden connections between figures of the March 1st Movement of 1919.\n"
                            . "As a distinguished senior historical docent and research scholar specializing in modern Korean history and the independence movement, "
                            . "produce a publication-ready academic documentary text based strictly on primary archival evidence and historical critique.\n\n"
                            . "# Objective\n"
                            . "Based on the user's query, analyze the [Source Context] retrieved from the knowledge graph (Graph DB) and explain the historical causality connecting persons, events, and places accurately and logically.\n\n"
                            . "# Strict Rules\n"
                            . "1. [Output Language & Zero Drift - CRITICAL]: Write the ENTIRE text in fluent, publication-ready academic English from start to finish. All titles, subtitles, section headings, narrative analysis, and final conclusion MUST be 100% in English. Never leak Korean particles, verbs, or sentences into sections 3, 4, or 5. Korean historical names and locations should be transliterated in English with original Hanja/Hangul in parentheses only when first introduced (e.g., 'Sagan-ri, Songsan-myeon, Suwon-gun', 'Yu Gwan-sun (柳寬順)'). Every paragraph must be complete in English.\n"
                            . "2. [Zero Hallucination]: Use ONLY the facts (names, places, dates, events) explicitly stated in the [Source Context] below and the Stage 1 fact table. Never fabricate or invent connections by mixing in pretrained external knowledge.\n"
                            . "3. [Multi-hop Explanation]: If the context contains 2-hop or more connections, explain how this causal chain links together logically so the user can follow the narrative seamlessly.\n"
                            . "4. [Hard Trap Defense]: If the provided [Source Context] is empty OR no connection related to the user's query can be found, output ONLY the following sentence and immediately terminate: 'According to the provided historical records, no information or connection regarding your query can be found.'\n"
                            . "5. [Tone & Style]: Maintain the professional and respectful tone of a museum docent. Begin immediately with the core causal relationship — no unnecessary preamble.\n\n"
                            . "# Additional Guidelines\n"
                            . "- Adhere strictly to factual grounding based on rigorous historical source criticism (史料批判). Never forcibly combine historically unrelated figures or events.\n"
                            . "- Facts stated within each <SOURCE_EVIDENCE> block are valid ONLY for the entity, region, and event of that block. Cross-attribution is strictly prohibited.\n"
                            . "- Do NOT expose any internal AI/system jargon (e.g., 'Zero-Correlation', 'knowledge graph', 'hallucination', 'nodes', 'database', 'prompt', hash IDs like n8774...) in the output.\n"
                            . "- Express all historical judgments using authentic scholarly terminology.";

                $fact_table_section = $fact_table ? "[Stage 1: Verified Archival Fact Table (In-Context Knowledge)]\n{$fact_table}\n\n" : "";

                $user_prompt = "■ Research Topic\n"
                             . "- Target Topic: {$term}\n"
                             . "(Note: Even if the query is in Korean or another language, produce the ENTIRE commentary, all section titles, and conclusion in 100% fluent academic English.)\n\n"
                             . "Produce an authoritative academic documentary text regarding '{$term}' based on the provided archival sources and verified fact table.\n\n"
                             . $fact_table_section
                             . "■ [CRITICAL HISTORICAL METHODOLOGY & GROUNDING RULES]\n"
                             . "1. SCOPING ENVELOPE & CROSS-ATTRIBUTION GUARD:\n"
                             . "   - Each <SOURCE_EVIDENCE> block is strictly scoped to its assigned entity, region, and event.\n"
                             . "   - The persons, actions, and dates within a block apply SOLELY to that specific entity and region. NEVER cross-attribute figures or actions to other regions or disparate demonstrations.\n"
                             . "2. OVERARCHING COMPARATIVE THEME:\n"
                             . "   - Formulate a coherent overarching academic theme and subtitle reflecting the historical breadth of the movement.\n"
                             . "3. FACTUAL GROUNDING & PHYSICAL/TEMPORAL INTEGRITY:\n"
                             . "   - Distinguish documentary co-occurrence in records from physical presence at local protests.\n"
                             . "4. 100% ENGLISH CONSISTENCY:\n"
                             . "   - Maintain fluent English throughout all sections including the conclusion. Do not revert to Korean.\n\n"
                             . "■ [Structural Outline - Follow Exactly]\n"
                             . "# [Master Academic Title]\n"
                             . "## [Academic Subtitle]\n"
                             . "1. Introduction: Comparative Historical Framing & Archival Verification of Regional Dynamics\n"
                             . "2. Trajectory of Primary Leadership / Regional Movement: Theaters of Operation and Strategy\n"
                             . "3. Localized Grassroots Uprising / Specific Development: The Battlefield of Demonstration\n"
                             . "4. Critical Cross-Examination of Primary Archives, Judicial Rulings, and Press Dispatches\n"
                             . "5. Synthesis & Historical Significance in Modern Korean Independence History\n\n"
                             . "[Archival Evidence Envelopes]\n{$context_str}\n\n"
                             . "[CRITICAL FINAL REMINDER - ZERO LANGUAGE DRIFT]: You must produce the ENTIRE commentary in 100% fluent academic English from the master title to the final word of Section 5 Conclusion. Do not leave any Korean particles or sentences untranslated.";

            } elseif ($explain_lang === 'ja') {
                $sys_prompt = "# 役割 (Role)\n"
                            . "あなたは1919年3・1独立万世運動の地域的拡散経路と人物間の連関を解明する「知識グラフ基盤・歴史専門ドキュメンタリー・ドーセント（学術解説員）」です。\n"
                            . "韓国近現代史および東アジア近代独立運動史の首席研究員・学術解説者として、提供された一次史料・公文書（京城地方法院検事局調書、高等法院判決文、警察訊問調書、新聞・電報等）を綿密に検証し、学術出版やテレビドキュメンタリー放送の基準を満たす、極めて格調高く流麗な日本語の学術解説原稿を執筆してください。\n\n"
                            . "# 目的 (Objective)\n"
                            . "ユーザーの質問に基づき、知識グラフから抽出された［史料コンテキスト］および第1段階検証表を分析し、人物・事件・場所へと連なる歴史的因果律を精緻かつ論理的に解説します。\n\n"
                            . "# 絶対遵守規則 (Strict Rules)\n"
                            . "1. 【出力言語の絶対的厳格性（最重要・言語ドリフト完全防止）】\n"
                            . "   - 全編（主タイトル、副題、章見出し、本文各段落、第5章結論に至るまで）を、100%流麗かつ洗練された学術日本語で記述してください。\n"
                            . "   - 【厳禁】韓国語の助詞（은, 는, 이, 가, 을, 를, 와, 과, 의, 에, 로, 으로 등）や文末・動詞（~하였다, ~되었다, ~이다, ~라 등）を日本語文中に残すことを絶対に禁止します。\n"
                            . "   - 【厳禁】史料中のハングル名詞（시위, 사건, 참살, 만세, 훈장, 추서, 경찰, 군중 등）をそのまま放置せず、必ず正確な日本語（示威・デモ、事件、殺害・処断、万歳、勲章、追叙、警察、群衆等）に翻訳して記述してください。\n"
                            . "   - 人名・地名は原則として漢字表記（例：張韶鎭、車喜植、民泳雲、文相翊、水原郡松山面泗江里、陸日光里、長安面）を用い、漢字が特定できない場合はカタカナ（例：チャ・インボム）を併記し、ハングル文字を地の文に混入させないでください。\n"
                            . "   - 【言語逆戻りの防止】特に後半部（第3章、第4章、そして第5章の結論）において韓国語に逆戻りする現象（言語ドリフト）を徹底的に防止し、最後の結びの1文字に至るまで、品格ある学術日本語で完結させてください。\n"
                            . "2. 【ゼロ・ハルシネーション（史料批判の徹底）】\n"
                            . "   - 下記［史料コンテキスト］および第1段階ファクト表に明記された事実（人名、地名、日付、事件）のみを用いて構成してください。外部の未確認知識を勝手に創作・混入させてはなりません。\n"
                            . "3. 【マルチホップ因果関係の明解な解説】\n"
                            . "   - 史料間に2段階（2-hop）以上の連関がある場合、その歴史的因果関係を読者が明瞭に理解できるよう論理的かつ説得力をもって展開してください。\n"
                            . "4. 【史料不在時の安全防護（Hard Trap Defense）】\n"
                            . "   - 該当する史料が存在しない場合は、次の1文のみを出力して直ちに終了してください：『提供された歴史記録（知識グラフ史料）においては、お問い合わせの事象や実体間の関連性を確認することができません。』\n"
                            . "5. 【トーン＆マナー】\n"
                            . "   - 博物館の首席学術解説員たる風格を保ち、無駄な前置きやAIメタ言及（「知識グラフ」「ノード」「プロンプト」「ハルシネーション」等）を一切排し、核心的な歴史的因果律から論述を開始してください。\n\n"
                            . "# 史料批判と記述上の禁則事項\n"
                            . "- <SOURCE_EVIDENCE> ブロックで区切られた人名・行動・日付は、該当する地域・事件の記述にのみ有効です。他地域の人物や行動を混同・交差帰属（Cross-attribution）させることを固く禁じます。\n"
                            . "- 「Zero-Correlation」「知識グラフ」「ノード」「データベース」「プロンプト」「ハルシネーション」「システム」等のAI・情報処理用語、および内部英数字識別子（n8774...等）を一切露出させてはなりません。";

                $fact_table_section = $fact_table ? "■［第1段階：史料批判に基づく検証済みファクト表（前提知識）］\n{$fact_table}\n\n" : "";

                $user_prompt = "■ 入力課題\n"
                             . "- 探究テーマ：{$term}\n"
                             . "（※探究テーマが韓国語または他言語であっても、解説本文・見出し・第5章結論に至るまですべて完全な学術日本語で執筆してください）\n"
                             . "- 史料コンテキスト：以下の一次史料ブロックおよび第1段階検証表に収蔵されています。\n\n"
                             . "上記テーマについて、提供された史料およびファクト表に基づき、学術ドキュメンタリー原稿を執筆してください。\n\n"
                             . $fact_table_section
                             . "■ 執筆ガイドライン\n"
                             . "1. 史料隔壁の厳守：各史料ブロックの人物・日付・行動はその地域・事件にのみ帰属させて記述すること。\n"
                             . "2. 巨視的・比較史的主題の設定：テーマの本質を捉えた格調高い主タイトルと学術副題を日本語で提示すること。\n"
                             . "3. 時空間の実在性検証：文書上の単なる連名記載と、実際の現場参加・直接行動を厳密に区別すること。\n"
                             . "4. 全文日本語の厳格維持：韓国語の助詞（은/는/이/가/을/를 등）や文末（~되었다 등）、未翻訳ハングルを一切残さず、すべての見出し・本文・結論を格式ある学術日本語で記述すること。特に第5章「結論」が韓国語に戻らないよう厳格に注意すること。\n\n"
                             . "■ 構成フォーマット（必ず以下の章構成に沿って執筆してください）\n"
                             . "# ［総合学術大主題］\n"
                             . "## ［学術副題］\n"
                             . "1. 序論：3・1独立運動の地域的展開と史料批判的アプローチ\n"
                             . "2. 当該地域・主導勢力における蜂起の胎動と組織的軌跡\n"
                             . "3. 地域基層民衆の自発的抗争と現場での実力行動の推移\n"
                             . "4. 当代一次史料および司法・警察記録（検事局調書・判決文・電報等）の批判的検証\n"
                             . "5. 結論：韓国近代独立運動史における本事件の歴史的位相と学術的意義\n\n"
                             . "［一次史料原文ブロック］\n{$context_str}\n\n"
                             . "【最重要・出力言語の厳守命令】全編（タイトルから第5章結論まで）を完全に格調高い学術日本語で出力してください。韓国語の助詞（은, 는, 이, 가, 을, 를, 와, 과, 의 等）、語尾、未翻訳ハングルは1文字たりとも残さず、全て日本語として完結させてください。";

            } elseif ($explain_lang === 'zh') {
                $sys_prompt = "# 角色定位 (Role)\n"
                            . "您是专门解读1919年三一独立运动复杂扩散路径与历史人物隐性关联的“知识图谱历史学术讲解员（AI Historical Docent）”。\n"
                            . "作为韩国近现代史与东亚近代独立运动史资深首席研究员兼学术讲解员，请依据提供的一手史料与司法档案（京城地方法院检察局调书、高等法院判决书、警察讯问调书、新闻电报等），撰写符合学术出版与高水准历史纪录片解说标准的中文学术文献原稿。\n\n"
                            . "# 核心目标 (Objective)\n"
                            . "根据用户的提问，严密分析从知识图谱提取的［史料语境］及第一阶段事实核查表，清晰、客观、逻辑严谨地阐明人物、事件与地域之间的历史因果律。\n\n"
                            . "# 严格遵守规则 (Strict Rules)\n"
                            . "1. 【输出语言绝对规范性（最高优先级・防止语言漂移）】\n"
                            . "   - 全篇文稿（大标题、副标题、章节标题、各段正文及第5章最终结论）必须100%使用规范严谨的学术简体中文撰写。\n"
                            . "   - 【严禁】在正文或结论中混入任何韩语助词（如 은, 는, 이, 가, 을, 를, 와, 과, 의 等）、终结词尾（如 ~하였다, ~되었다 等）或未翻译的谚文单词（如 시위, 사건, 참살, 훈장 等）。\n"
                            . "   - 朝鲜历史人名、地名等专有名词优先使用正统汉字表述（如 张韶镇、车喜植、闵泳云、文相翊、水原郡松山面泗江里、长安面等），切勿遗留未翻译的纯韩文字母。\n"
                            . "   - 【防止回漂】坚决杜绝后半部分（第3章至第5章结论）发生语言漂移回韩语的现象，整篇文稿直至最后一个标点符号均须保持典雅纯正的中文学术散文体。\n"
                            . "2. 【零幻觉（严格史料批判）】\n"
                            . "   - 仅依据下述［史料语境］及第一阶段事实核查表明确记载的事实（人名、地名、日期、事件）进行严密论述，绝不凭空臆造或掺杂外部未经核实的知识。\n"
                            . "3. 【多跳因果链条的清晰贯通】\n"
                            . "   - 若史料中存在2跳及以上的关联纽带，须向读者清晰剖析其因果承接脉络，使历史叙事一气呵成。\n"
                            . "4. 【无史料时的安全防御机制（Hard Trap Defense）】\n"
                            . "   - 若提供的史料中未发现与提问相关的线索，仅输出以下一句话并立即终止：“根据现存历史档案记录（知识图谱史料），未发现与您查询内容相关的历史关联信息。”\n"
                            . "5. 【基调与风格】\n"
                            . "   - 保持历史博物馆资深馆员的专业庄重口吻，直入历史因果核心，严禁出现“零相关”、“知识图谱”、“节点”、“数据库”、“提示词”等计算机与AI技术元术语。\n\n"
                            . "# 史料隔离与交叉归属禁令\n"
                            . "- 每个 <SOURCE_EVIDENCE> 史料块中记载的人物与行动仅对其标明的特定地域与事件有效，严禁跨区域生搬硬套或混淆人物归属。";

                $fact_table_section = $fact_table ? "■［第一阶段：基于史料批判的核实事实表（背景知识）］\n{$fact_table}\n\n" : "";

                $user_prompt = "■ 输入主题\n"
                             . "- 探讨主题：{$term}\n"
                             . "（※即使探讨主题包含韩语或其他语种，解说全文、所有小标题及第5章结论均必须全部使用规范学术简体中文撰写）\n"
                             . "- 史料语境：收录于以下一手史料块及第一阶段事实表中。\n\n"
                             . "请针对上述主题，依据一手史料与事实核查表，撰写权威学术纪录片解说文稿。\n\n"
                             . $fact_table_section
                             . "■ 撰写规范\n"
                             . "1. 严守史料隔离：各史料块的人物与事件仅在对应地域范畴内分析，严禁跨地域混淆。\n"
                             . "2. 确立宏观比较史主题：拟定富有学术深度的大标题与副标题。\n"
                             . "3. 时空真实性甄别：严格区分档案同案列名与实际现场直接参与行动的区别。\n"
                             . "4. 全文纯正中文：杜绝任何韩语残留，从章节标题到结论全部采用学术规范简体中文，严防第5章结论漂移回韩文。\n\n"
                             . "■ 结构提纲（请严格遵循以下章节结构撰写）\n"
                             . "# ［总括学术大标题］\n"
                             . "## ［学术副标题］\n"
                             . "1. 引言：三一独立运动的地域扩散态势与史料批判导论\n"
                             . "2. 核心领导力量与区域抗争脉络：行动舞台与斗争策略\n"
                             . "3. 基层民众的自发抗暴斗争与现场示威演进：民众实力抗争过程\n"
                             . "4. 当代一手史料与司法警察档案（检察局调书、判决书、电报等）的批判性互证\n"
                             . "5. 结论：本案在韩国近代独立运动史中的历史定位与学术启示\n\n"
                             . "［一手史料原文块］\n{$context_str}\n\n"
                             . "【最高优先级・输出语言严令】全篇（从主标题到第5章结论）必须全部输出为规范典雅的学术简体中文。严禁残留任何韩语助词（은, 는, 이, 가, 을, 를 等）、词尾或谚文字母，必须一气呵成以中文完结，防止后半部分漂移回韩语。";

            } else {
                $sys_prompt = "# 역할 (Role)\n"
                            . "당신은 1919년 3·1 운동의 복잡한 확산 경로와 인물 간의 숨겨진 연관성을 해설하는 '지식그래프 기반 역사 전문 도슨트'입니다.\n"
                            . "한국 근현대사 및 독립운동사 전문 수석 역사 도슨트이자 정통 역사학술 연구자로서, "
                            . "제공된 1차 사료와 문헌 기록을 바탕으로 학술 출판 및 다큐멘터리 방송에 즉시 사용할 수 있는 완성도 높은 해설 원고를 작성하십시오.\n\n"
                            . "# 목적 (Objective)\n"
                            . "사용자의 질의를 바탕으로 지식그래프(Graph DB)에서 인출된 [사료 컨텍스트]를 분석하여, "
                            . "인물-사건-장소로 이어지는 역사적 인과율을 정확하고 논리적으로 해설합니다.\n\n"
                            . "# 절대 준수 규칙 (Strict Rules)\n"
                            . "1. [Language Consistency]: 전체 해설을 품격 있고 정제된 학술 한국어로 일관되게 작성하십시오.\n"
                            . "2. [Zero Hallucination]: 오직 하단에 제공된 [사료 컨텍스트]에 명시된 사실(인명, 지명, 날짜, 사건)만을 사용하여 답변을 구성하십시오. "
                            . "사전 학습된 외부 지식을 섞어 지어내지 마십시오.\n"
                            . "3. [Multi-hop Explanation]: 컨텍스트에 2단계(2-hop) 이상의 연결 고리(예: 인물 A → 공동 사건 → 인물 B → 타지역 사건)가 있다면, "
                            . "이 인과 과정이 어떻게 이어지는지 사용자가 이해하기 쉽게 풀어서 설명하십시오. 서사의 흐름이 끊기지 않게 연결하십시오.\n"
                            . "4. [Hard Trap Defense]: 제공된 [사료 컨텍스트]가 비어있거나, 사용자 질의와 관련된 연관성을 찾을 수 없는 경우, "
                            . "오직 다음 문장만 출력하고 해설을 즉시 종료하십시오. 사전 학습된 배경지식을 동원한 부연 설명이나 해설을 절대 덧붙이지 마십시오. "
                            . "'제공된 역사 기록(지식그래프 사료)에서는 질문하신 내용이나 개체 간의 연관성을 찾을 수 없습니다.'\n"
                            . "5. [Tone & Style]: 전문적이고 정중한 박물관 해설사(도슨트)의 어조를 유지하며, 불필요한 서론 없이 핵심 인과관계부터 즉시 설명하십시오.\n\n"
                            . "# 추가 작성 지침\n"
                            . "- 철저한 사료 비판(史料批判)과 사실 검증에 입각하여 서술하며, 역사적 상관관계가 없는 인물과 사건을 억지로 결합하거나 사실을 왜곡하지 마십시오.\n"
                            . "- 각 <SOURCE_EVIDENCE> 블록에 명시된 인물·행동·일자는 해당 개체·지역·사건 서술에만 유효합니다. 교차 귀속(Cross-attribution)을 엄격히 금지합니다.\n"
                            . "- 원고 본문에 'Zero-Correlation', '지식 그래프', '노드', '데이터베이스', '프롬프트', '환각', '시스템', 영문 해시 식별자(예: n8774... 등) 같은 인공지능·전산 메타 용어를 절대 노출하지 마십시오.\n"
                            . "- 모든 판단과 분석은 정통 역사학 연구 어휘(예: '사료 비판을 통한 실증', '당대 1차 사료군 및 공문서 판결문 분석')로 품격 있게 서술하십시오.";

                $fact_table_section = $fact_table ? "■ [1단계: 사료 비판 기반 정밀 사건-장소-인물 교차 검증표 (In-Context Knowledge Table)]\n{$fact_table}\n\n" : "";

                $user_prompt = "# 입력 변수 (Input Variables)\n"
                             . "- [사용자 질의]: {$term}\n"
                             . "- [사료 컨텍스트]: 아래 사료 원문 블록 및 1단계 사료 비판 검증표에 포함되어 있습니다.\n\n"
                             . "위 [사용자 질의]에 대해, 제공된 [사료 컨텍스트]와 1단계 사료 비판 검증표를 바탕으로 '{$term}'에 관한 심층 역사 해설문을 학술 다큐멘터리 원고 양식으로 작성하십시오.\n\n"
                             . $fact_table_section
                             . "■ [핵심 원칙: 사료 비판 및 정통 역사학술 원고 작성 지침]\n"
                             . "1. 사료 격벽 준수 및 교차 귀속(Cross-attribution) 절대 금지:\n"
                             . "   - 제공된 사료는 <SOURCE_EVIDENCE id=\"...\" entity=\"...\" region=\"...\" event=\"...\"> 형태의 독립된 격벽 블록으로 구분되어 있습니다.\n"
                             . "   - 각 <SOURCE_EVIDENCE> 태그 및 1단계 검증표에 기록된 인물, 행동, 일자는 오직 해당 개체·지역(region)·사건(event) 서술에만 유효합니다.\n"
                             . "   - 특정 지역의 인물(예: 평양의 강규찬)을 다른 지역의 시위(예: 함흥 우시장 시위, 종로 보신각)로 교차 결합하거나 왜곡하는 행위를 엄격히 금지합니다.\n"
                             . "2. 총괄 비교사적 대주제 확립 (기획 구성의 당위성 부여):\n"
                             . "   - 검색어가 상이한 시공간적 무대와 성격을 지닌 복수 개체인 경우(예: '이동휘와 아우내 장터'), 단순히 두 단어를 나열하는 어색한 제목을 피하고, 1919년 독립운동의 총체성을 조망하는 거시적 비교사 대주제와 부제를 반드시 정립하십시오.\n"
                             . "     [예시]:\n"
                             . "     # 3·1운동의 다층적 지형: 국외 지도부의 무장투쟁 노선과 국내 기층 민중의 자발적 봉기 비교\n"
                             . "     ## — 이동휘의 북방 항일 투쟁과 천안 아우내 장터 만세시위를 중심으로 —\n"
                             . "   - 서론에서 왜 이 두 축을 함께 조명하는지 역사학적 당위성(독립운동의 외연과 다층성 조명)을 명확히 제시하되, 사료 비판을 통해 두 대상 간에 직접적인 현장 결합이나 조직적 지휘선이 존재하지 않았음을 서두에서 엄밀히 규명하십시오.\n"
                             . "3. 시공간적 실재성 검증 및 물리적 현장 참여 왜곡 방지 (사료 왜곡 차단):\n"
                             . "   - 당대 공문서, 외신 전보, 판결문, 사료 목록 등에 여러 독립운동 지도자(손병희, 안창호, 이승만, 이동휘 등)의 명단이나 동향 정보가 함께 기재되어 있다고 해서, 이를 특정 지역의 국지적 현장 시위(예: 경성 전차 투석, 지방 장터 시위 등)에 직접 참여하거나 현장을 지휘한 것으로 오인하여 서술하지 마십시오.\n"
                             . "   - 손병희는 3·1 독립선언 직후 체포되어 옥중에 수감되어 있었고, 안창호와 이승만 등은 해외 망명 및 체류 상태였으므로 물리적으로 현장 참여가 불가능했습니다. 사료상의 '단순 명단 기재·보고서 상의 동시 언급'과 '물리적 현장 참여'를 사료 비판을 통해 엄격히 분별하십시오.\n"
                             . "4. 독립된 활동 무대와 역사적 맥락의 엄격한 분리 서술:\n"
                             . "   - 직접적 연관이 없는 두 대상의 경우, 각 대상의 고유한 활동 무대(예: 이동휘의 함경도·북간도·연해주·상해 임시정부 무장투쟁 노선 vs 천안 아우내 장터 기층 민중의 자발적 만세봉기)를 독립된 장(章)으로 완결성 있게 심층 서술하십시오.\n"
                             . "5. 전산/AI 메타 용어 완전 배제 및 학술 어휘 준수:\n"
                             . "   - 본문 내에 'Zero-Correlation', '환각', '지식 그래프', '노드', '데이터베이스', '프롬프트', '시스템', '[Event,사건] n8774...' 등 전산·AI 용어를 일절 쓰지 마십시오. 대신 '사료 비판(史料批判)', '당대 1차 사료군(일제 감시 보고, 외신 전보, 판결문, 신문 기사)', '사료적 실증' 등의 학술 용어를 구사하십시오.\n"
                             . "6. 가계(가족) 및 동지 연대 서술:\n"
                             . "   - 검색 대상이 특정 인물 일가의 독립운동이나 혈연·조직적 동지 관계인 경우(예: 유관순 일가의 옥고와 순국 등), 제공된 사료에 입각하여 그 숭고한 항일 연대와 희생을 깊이 있게 조명하십시오.\n\n"
                             . "■ [서술 구조 가이드라인]\n"
                             . "# [총괄 학술 대주제]\n"
                             . "## [학술 부제]\n"
                             . "1. 서론: 3·1운동의 다층적 지형과 비교사적 문제 제기 (두 축의 설정 및 사료 비판을 통한 연관성 규명)\n"
                             . "2. 국외 항일 무장투쟁 지도부 / 지역 중심 궤적: 활동 무대와 노선 (심층 분석)\n"
                             . "3. 국내 기층 민중의 자발적 항쟁 / 국지적 시위 전개: 현장 전개와 민중의 저항 (심층 분석)\n"
                             . "4. 당대 1차 사료군 및 관찬·보도 기록의 비판적 검토 (외신 전보, 판결문, 보고서 정밀 교차 검증 및 단순 연계 왜곡 시정)\n"
                             . "5. 결론: 한국 근현대 독립운동사에서의 역사적 위상과 학술적 의의\n\n"
                             . "[사료 원문 블록]\n{$context_str}\n\n"
                             . "[최종 확인]: 서론부터 제5장 결론까지 전체를 품격 있고 완결성 높은 학술 한국어로 완성하십시오.";
            }

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
                exit;
            }

            // Fallback: 기존 동기식 JSON 응답
            $res = call_gemini([
                ["role" => "system", "content" => $sys_prompt],
                ["role" => "user", "content" => $user_prompt]
            ], false, 8192, 0.1, 0.85);

            echo json_encode(["text" => $res], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
            exit;
        }

        // [Action 5] 개체 원천 사료(PostgreSQL) 상세 레코드 실시간 조회
        if ($action === 'node_detail') {
            $raw_id = docent_sanitize_text($_POST['raw_id'] ?? '', 500, false);
            $table = docent_sanitize_text($_POST['table'] ?? '', 100, false);
            $rowid = (int)($_POST['rowid'] ?? 0);
            $label = docent_sanitize_text($_POST['label'] ?? '', 200, false);
            $aliases = docent_sanitize_json_list($_POST['aliases'] ?? '', 10, 100);

            // raw_id 패턴 분석 (예: raw_event_place_link:Generated from raw_event_place_link row 3647 on...)
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

                // 검색 대상 식별자/키워드 후보 수집
                $candidates = array_values(array_unique(array_filter(
                    array_merge([$raw_id, $label], $aliases),
                    static fn($v) => $v !== null && trim((string)$v) !== ''
                )));

                try {
                    // 1. table/rowid가 유효한 경우 직접 조회
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

                    // 2. table/rowid로 못 찾았고 후보 키워드들이 있는 경우 차례로 조회
                    if (!$response_data["found"] && !empty($candidates)) {
                        // Phase 2a: 선언적 배열 기반 일괄 정확매칭 (기존 7단계 순차 폭포 → 단일 루프)
                        // 주의: 컬럼명은 각 테이블의 실제 DB 스키마와 정확히 일치해야 함
                        $exact_search_tables = [
                            ['table' => 'raw_event_info', 'cols' => ['아이디', '사건명']],
                            ['table' => 'raw_event_place_link', 'cols' => ['demons_id', 'demons_title', 'place_id', 'place_name']],
                            ['table' => 'raw_detail_place', 'cols' => ['세부장소아이디', '명칭', '이칭']],
                            ['table' => 'raw_bibliography', 'cols' => ['문서아이디', '제목']],
                            ['table' => 'raw_oppression_org_police', 'cols' => ['기구ID', '기구명']],
                            ['table' => 'raw_oppression_org_gendarme', 'cols' => ['기구id', '기구명']],   // 헌병: 소문자 id
                            ['table' => 'raw_oppression_org_military', 'cols' => ['기구ID', '기구명칭']], // 군대: '기구명칭'
                        ];

                        // candidates 전체를 IN 절로 한번에 바인딩 (테이블당 1회 쿼리)
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

                        // Phase 2b: LIKE 기반 유사매칭 (기존 6,7단계)
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
            exit;
        }
    } catch (Throwable $e) {
        $detail_msg = $e->getMessage();
        $file_name = basename($e->getFile());
        $line_no = $e->getLine();
        $class_name = get_class($e);
        error_log(sprintf('Docent AJAX failure [%s]: [%s] %s in %s:%d', $action, $class_name, $detail_msg, $file_name, $line_no));
        http_response_code(500);
        echo json_encode([
            "error" => sprintf('[%s] %s (%s:%d)', $class_name, $detail_msg, $file_name, $line_no),
            "details" => [
                "class" => $class_name,
                "message" => $detail_msg,
                "file" => $file_name,
                "line" => $line_no
            ]
        ], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
        exit;
    }
}

// Infrastructure Functions
function get_neo4j() {
    static $client = null;
    if ($client !== null) return $client;
    $client = ClientBuilder::create()
        ->withDriver('default', get_cfg('NEO4J_URI', 'bolt://127.0.0.1:7687'), Authenticate::basic(get_cfg('NEO4J_USER', 'neo4j'), get_cfg('NEO4J_PASSWORD')))
        ->build();
    return $client;
}

function get_pg() {
    static $pdo = null;
    static $tried = false;
    if ($pdo !== null) return $pdo;
    if ($tried) return null;
    $tried = true;
    try {
        $host = get_cfg('PG_HOST', '127.0.0.1');
        $port = get_cfg('PG_PORT', 5432);
        $dbname = get_cfg('PG_DATABASE', 'historical');
        $user = get_cfg('PG_USER', 'postgres');
        $pass = get_cfg('PG_PASSWORD', '');
        
        if (empty($pass)) return null;
        $dsn = "pgsql:host={$host};port={$port};dbname={$dbname}";
        $pdo = new PDO($dsn, $user, $pass, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        return $pdo;
    } catch (Exception $e) {
        return null;
    }
}

function get_gemini_candidate_models() {
    $configured = trim((string)get_cfg('GEMINI_MODEL', 'gemini-3-flash-preview'));
    $candidates = [
        $configured,
        'gemini-3-flash-preview',
        'gemini-3.6-flash',
        'gemini-3.1-flash-lite-preview',
        'gemini-3.5-flash-lite',
    ];
    $clean = [];
    foreach ($candidates as $cand) {
        $cand = trim((string)$cand);
        if ($cand !== '' && preg_match('/^[A-Za-z0-9._-]+$/', $cand) && !in_array($cand, $clean, true)) {
            $clean[] = $cand;
        }
    }
    return !empty($clean) ? $clean : ['gemini-3-flash-preview'];
}

function call_gemini($msgs, $is_json = false, $max_tokens = null, $temperature = null, $top_p = null, $timeout = 25) {
    $api_key = get_cfg('GEMINI_API_KEY');
    if (!$api_key) {
        $api_key = get_cfg('API_KEY');
    }
    if (!$api_key) return $is_json ? '{"explanation":"API Key missing"}' : docent_t("API Key가 설정되지 않았습니다.", "API key is not configured.");

    $candidate_models = get_gemini_candidate_models();
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

    $default_temp = (float)get_cfg('GEMINI_TEMPERATURE', '0.1');
    $default_topp = (float)get_cfg('GEMINI_TOP_P', '0.85');
    $temp_val = is_numeric($temperature) ? (float)$temperature : $default_temp;
    $top_p_val = is_numeric($top_p) ? (float)$top_p : $default_topp;

    $payload = [
        "contents" => $contents,
        "generationConfig" => [
            "temperature" => $temp_val,
            "topP" => $top_p_val,
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

    $last_error_message = '';
    $post_json = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);

    foreach ($candidate_models as $model) {
        $url = "https://generativelanguage.googleapis.com/v1beta/models/{$model}:generateContent";
        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_POST => true,
            CURLOPT_POSTFIELDS => $post_json,
            CURLOPT_HTTPHEADER => ["Content-Type: application/json", "x-goog-api-key: {$api_key}"],
            CURLOPT_CONNECTTIMEOUT => 6,
            CURLOPT_TIMEOUT => max(5, (int)$timeout),
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
            $last_error_message = $provider_message !== '' ? "Gemini 오류 ({$model}): {$provider_message}" : "AI 요청 실패 ({$model}, HTTP {$http_code}, cURL {$curl_errno})";
            error_log(sprintf('Gemini model %s failed: HTTP %d, cURL %d, %s, provider: %s — trying next candidate', $model, $http_code, $curl_errno, $curl_error, $provider_message));
            continue;
        }

        $data = json_decode($res, true);
        $content = '';
        if (isset($data['candidates'][0]['content']['parts']) && is_array($data['candidates'][0]['content']['parts'])) {
            foreach ($data['candidates'][0]['content']['parts'] as $part) {
                if (isset($part['text'])) {
                    $content .= $part['text'];
                }
            }
        }

        if (trim($content) !== '') {
            return $content;
        }
    }

    $safe_message = $last_error_message !== '' ? $last_error_message : "AI 응답을 생성하지 못했습니다.";
    return $is_json
        ? json_encode(["explanation" => $safe_message], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE)
        : $safe_message;
}

function stream_gemini($msgs, callable $on_chunk, $max_tokens = 8192, $temperature = null, $top_p = null) {
    $api_key = get_cfg('GEMINI_API_KEY');
    if (!$api_key) $api_key = get_cfg('API_KEY');
    if (!$api_key) {
        $on_chunk(docent_t("API Key가 설정되지 않았습니다.", "API key is not configured."));
        return;
    }

    $candidate_models = get_gemini_candidate_models();
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

    $default_temp = (float)get_cfg('GEMINI_TEMPERATURE', '0.1');
    $default_topp = (float)get_cfg('GEMINI_TOP_P', '0.85');
    $temp_val = is_numeric($temperature) ? (float)$temperature : $default_temp;
    $top_p_val = is_numeric($top_p) ? (float)$top_p : $default_topp;

    $payload = [
        "contents" => $contents,
        "generationConfig" => [
            "temperature" => $temp_val,
            "topP" => $top_p_val,
            "maxOutputTokens" => (int)max(1, min(8192, $max_tokens)),
        ]
    ];

    if ($system_prompt !== "") {
        $payload["system_instruction"] = [
            "parts" => [["text" => $system_prompt]]
        ];
    }

    $post_json = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);

    foreach ($candidate_models as $model) {
        $url = "https://generativelanguage.googleapis.com/v1beta/models/{$model}:streamGenerateContent?alt=sse";
        $buffer = '';
        $chunks_delivered = 0;

        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_POST => true,
            CURLOPT_POSTFIELDS => $post_json,
            CURLOPT_HTTPHEADER => ["Content-Type: application/json", "x-goog-api-key: {$api_key}"],
            CURLOPT_CONNECTTIMEOUT => 8,
            CURLOPT_TIMEOUT => 90,
            CURLOPT_SSL_VERIFYPEER => true,
            CURLOPT_SSL_VERIFYHOST => 2,
            CURLOPT_WRITEFUNCTION => function($ch, $data) use (&$buffer, &$chunks_delivered, $on_chunk) {
                $buffer .= $data;
                while (preg_match('/^(.*?)(?:\r?\n\r?\n)/s', $buffer, $matches)) {
                    $block = $matches[1];
                    $matched_len = strlen($matches[0]);
                    $buffer = substr($buffer, $matched_len);

                    $lines = preg_split('/\r?\n/', $block);
                    foreach ($lines as $line) {
                        $line = trim($line);
                        if (str_starts_with($line, 'data: ')) {
                            $json_str = substr($line, 6);
                            $parsed = json_decode($json_str, true);
                            if (isset($parsed['candidates'][0]['content']['parts'])) {
                                foreach ($parsed['candidates'][0]['content']['parts'] as $part) {
                                    if (isset($part['text']) && $part['text'] !== '') {
                                        $chunks_delivered++;
                                        $on_chunk($part['text']);
                                    }
                                }
                            }
                        }
                    }
                }
                return strlen($data);
            }
        ]);

        curl_exec($ch);
        $curl_errno = curl_errno($ch);
        $http_code = (int)curl_getinfo($ch, CURLINFO_RESPONSE_CODE);

        // 청크가 성공적으로 전달되었으면 스트리밍 완료
        if ($chunks_delivered > 0 && $curl_errno === 0) {
            return;
        }

        // 청크가 전달되기 전에 실패한 경우 다음 후보 모델로 fallback 시도
        error_log(sprintf('stream_gemini model %s failed before stream (HTTP %d, cURL %d) — trying next model', $model, $http_code, $curl_errno));
    }

    $on_chunk("\n\n[AI 해설 생성 중 서비스 지연이 발생했습니다. 잠시 후 다시 시도해 주세요.]");
}


function clamp_int($value, $min, $max) {
    return max($min, min($max, (int)$value));
}

function dynamic_prefetch_ratio($node_count) {
    if ($node_count <= 10) return 0.40;
    if ($node_count >= 30) return 0.30;
    return 0.40 - (($node_count - 10) * (0.10 / 20.0));
}

function merge_same_person_nodes(&$nodes, &$edges, &$evidences) {
    if (empty($nodes)) return;

    $merge_target = []; // maps alias_nid => canon_nid

    // 1. Detect from edges with type === '동일인물', 'sameAs', 'owl:sameAs'
    foreach ($edges as $e) {
        $type = $e['type'] ?? '';
        if (in_array($type, ['동일인물', 'sameAs', 'owl:sameAs'], true)) {
            $from = $e['from'];
            $to = $e['to'];
            if (isset($nodes[$from], $nodes[$to]) && $from !== $to) {
                $raw1 = (string)($nodes[$from]['raw_id'] ?? '');
                $raw2 = (string)($nodes[$to]['raw_id'] ?? '');

                $is_hangul1 = (bool)preg_match('/[\x{AC00}-\x{D7A3}]/u', $raw1);
                $is_hangul2 = (bool)preg_match('/[\x{AC00}-\x{D7A3}]/u', $raw2);
                $is_hanja1 = (bool)preg_match('/[\x{4E00}-\x{9FFF}]/u', $raw1);
                $is_hanja2 = (bool)preg_match('/[\x{4E00}-\x{9FFF}]/u', $raw2);

                if ($is_hangul1 && !$is_hangul2 && $is_hanja2) {
                    $canon = $from; $alias = $to;
                } elseif ($is_hangul2 && !$is_hangul1 && $is_hanja1) {
                    $canon = $to; $alias = $from;
                } else {
                    $canon = $to; $alias = $from;
                }
                $merge_target[$alias] = $canon;
            }
        }
    }

    // 2. Secondary check: Person nodes with matching Hanja / Hangul reading
    $person_nids = [];
    foreach ($nodes as $nid => $node) {
        $type = $node['type'] ?? '';
        $labels = $node['labels'] ?? [];
        if ($type === '인물' || in_array('인물', $labels, true) || in_array('Person', $labels, true)) {
            $person_nids[] = $nid;
        }
    }

    $count_p = count($person_nids);
    for ($i = 0; $i < $count_p; $i++) {
        for ($j = $i + 1; $j < $count_p; $j++) {
            $nid1 = $person_nids[$i];
            $nid2 = $person_nids[$j];
            if (!isset($nodes[$nid1], $nodes[$nid2])) continue;

            $raw1 = trim((string)($nodes[$nid1]['raw_id'] ?? ''));
            $raw2 = trim((string)($nodes[$nid2]['raw_id'] ?? ''));
            $props1 = $nodes[$nid1]['props'] ?? [];
            $props2 = $nodes[$nid2]['props'] ?? [];

            $reading1 = trim((string)($props1['한글독음'] ?? $props1['한글명칭'] ?? ''));
            $reading2 = trim((string)($props2['한글독음'] ?? $props2['한글명칭'] ?? ''));
            $hanja1 = trim((string)($props1['한자'] ?? ''));
            $hanja2 = trim((string)($props2['한자'] ?? ''));

            $is_same = false;
            $canon = null; $alias = null;

            if ($reading1 !== '' && $reading1 === $raw2) {
                $is_same = true; $canon = $nid2; $alias = $nid1;
            } elseif ($reading2 !== '' && $reading2 === $raw1) {
                $is_same = true; $canon = $nid1; $alias = $nid2;
            } elseif ($hanja1 !== '' && $hanja1 === $raw2) {
                $is_same = true; $canon = $nid1; $alias = $nid2;
            } elseif ($hanja2 !== '' && $hanja2 === $raw1) {
                $is_same = true; $canon = $nid2; $alias = $nid1;
            }

            if ($is_same && $canon && $alias) {
                $merge_target[$alias] = $canon;
            }
        }
    }

    if (empty($merge_target)) return;

    // Resolve transitive mapping (A -> B -> C => A -> C, B -> C)
    foreach ($merge_target as $src => $dst) {
        $visited = [$src => true];
        $curr = $dst;
        while (isset($merge_target[$curr]) && !isset($visited[$curr])) {
            $visited[$curr] = true;
            $curr = $merge_target[$curr];
        }
        $merge_target[$src] = $curr;
    }

    // Merge nodes
    foreach ($merge_target as $alias_nid => $canon_nid) {
        if (!isset($nodes[$alias_nid], $nodes[$canon_nid])) continue;
        if ($alias_nid === $canon_nid) continue;

        $alias_node = $nodes[$alias_nid];
        $canon_node = &$nodes[$canon_nid];

        $alias_raw = (string)($alias_node['raw_id'] ?? '');
        $canon_raw = (string)($canon_node['raw_id'] ?? '');

        if (!isset($canon_node['aliases']) || !is_array($canon_node['aliases'])) {
            $canon_node['aliases'] = [];
        }
        if ($alias_raw !== '' && $alias_raw !== $canon_raw) {
            $canon_node['aliases'][] = $alias_raw;
        }
        if (!empty($alias_node['aliases'])) {
            $canon_node['aliases'] = array_merge($canon_node['aliases'], $alias_node['aliases']);
        }
        $canon_node['aliases'] = array_values(array_unique($canon_node['aliases']));

        // Format combined label: e.g. "이동휘 (李東輝)"
        $icon = "👤\n";
        $hanja_sub = '';
        foreach ($canon_node['aliases'] as $al) {
            if (preg_match('/[\x{4E00}-\x{9FFF}]/u', $al)) {
                $hanja_sub = $al;
                break;
            }
        }
        if ($hanja_sub !== '') {
            $canon_node['label'] = $icon . mb_substr("{$canon_raw} ({$hanja_sub})", 0, 25);
        } elseif (!empty($canon_node['aliases'])) {
            $first_al = $canon_node['aliases'][0];
            $canon_node['label'] = $icon . mb_substr("{$canon_raw} ({$first_al})", 0, 25);
        }

        if (!empty($alias_node['labels'])) {
            $canon_node['labels'] = array_values(array_unique(array_merge($canon_node['labels'] ?? [], $alias_node['labels'])));
        }
        if (isset($alias_node['props'])) {
            $canon_node['props'] = array_merge($alias_node['props'], $canon_node['props'] ?? []);
        }

        unset($nodes[$alias_nid]);
    }

    // Redirect edges & drop internal alias edges
    $new_edges = [];
    foreach ($edges as $e) {
        $from = $merge_target[$e['from']] ?? $e['from'];
        $to = $merge_target[$e['to']] ?? $e['to'];

        if ($from === $to) continue; // Drop self-loops resulting from merge
        if (in_array($e['type'] ?? '', ['동일인물', 'sameAs', 'owl:sameAs'], true)) {
            continue; // Drop 동일인물 edges between merged nodes
        }

        $e['from'] = $from;
        $e['to'] = $to;
        $new_edges[] = $e;
    }
    $edges = $new_edges;
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
        if (!empty($n['aliases'])) {
            foreach ($n['aliases'] as $alias) {
                $alias = trim((string)$alias);
                if ($alias !== '') $names[$alias] = true;
            }
        }
        if (count($names) >= $target) break;
    }
    return array_keys($names);
}

function add_fact_evidence(&$evidences, $prefix, $hash_parts, $payload, $max_count = 30) {
    if (!is_array($evidences)) $evidences = [];
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

function build_budgeted_evidence_context($evidences, $lang, $max_items, $min_chars, $max_chars, $total_char_budget, $term = '', $focus = '') {
    if (is_bool($lang)) {
        $lang = $lang ? 'en' : 'ko';
    }
    $lines = [];
    $used_chars = 0;
    foreach ((array)$evidences as $idx => $e) {
        if (count($lines) >= $max_items) break;
        if (($total_char_budget - $used_chars) < ($min_chars + 120)) break;

        $doc = mb_substr(normalize_whitespace_text($e['doc'] ?? ''), 0, 120);
        $doc = preg_replace('/\[[A-Za-z0-9_,]+\]\s*(n[a-f0-9]{8,}|[a-z]_\d+)/u', '[관련 사료 기록]', $doc);
        $doc = str_replace(
            ['[시소러스]', '[Thesaurus]', '[문건]', '[사료]', '[Event,사건]', '[Person,인물]'],
            ['[역사 용어 사전]', '[Historical Dictionary]', '[1차 사료]', '[1차 사료]', '[사건 기록]', '[인물 기록]'],
            $doc
        );
        $doc = preg_replace('/\b(n[a-f0-9]{10,}|[a-z]_\d{4,})\b/u', '', $doc);
        $doc = trim(preg_replace('/\s+/u', ' ', $doc));

        $concept = trim((string)($e['concept'] ?? ''));
        $entity_attr = htmlspecialchars($concept ?: $doc, ENT_QUOTES, 'UTF-8');
        $id_attr = 'g_' . ($idx + 1);

        $text = normalize_whitespace_text($e['text'] ?? '');
        $text = preg_replace('/\b(n[a-f0-9]{10,}|[a-z]_\d{4,})\b/u', '', $text);
        if ($text === '') continue;

        // 지명(region) 및 사건(event) 추출
        $region = '';
        if (preg_match('/(평안[남북]도|함경[남북]도|충청[남북]도|전라[남북]도|경상[남북]도|강원도|경기도|황해도|평양|함흥|천안|병천|단천|간도|연해주|상해|진천|청주|용산|종로|보신각|대구|부산|통영|의주|해주|원산)[^\s,]*/u', $text . ' ' . $doc, $rm)) {
            $region = $rm[0];
        }

        $event = '';
        if (preg_match('/([가-힣\w]+(?:시위|만세|선언식|의거|봉기|집회|행진|사건|파견|회합))/u', $doc . ' ' . $text, $em)) {
            $event = $em[0];
        }

        // 초점 기반 필터링 (장소적 시위 확산 중심 질의인 경우 외교/청원 문서는 1줄 요약)
        $is_spatial_query = preg_match('/(장소|지역|확산|전개|시위|만세)/u', $term . ' ' . $focus);
        $is_diplomatic = preg_match('/(파리강화회의|외교청원|청원서\s*서명|강화회의\s*파견)/u', $text . ' ' . $doc);

        if ($is_spatial_query && $is_diplomatic) {
            $body = "[외교 및 기타 활동 약력 기록: {$event} 관련 명단 수록 (장소적 시위 확산과 직접 무관)]";
        } else {
            $remaining = $total_char_budget - $used_chars;
            $body_cap = max($min_chars, min($max_chars, $remaining - 120));
            $body = mb_substr($text, 0, $body_cap);
        }

        if ($lang === 'ja') {
            $comment_str = "<!-- この史料の内容は [{$entity_attr}" . ($region ? " / {$region}" : "") . ($event ? " / {$event}" : "") . "] の記述にのみ有効であり他地域・事件との混同厳禁 -->";
            $doc_lbl = "史料";
            $body_lbl = "記述";
        } elseif ($lang === 'zh') {
            $comment_str = "<!-- 本史料内容仅对 [{$entity_attr}" . ($region ? " / {$region}" : "") . ($event ? " / {$event}" : "") . "] 的记述有效，严禁跨区域混淆 -->";
            $doc_lbl = "文献";
            $body_lbl = "内容";
        } elseif ($lang === 'en') {
            $comment_str = "<!-- This archival evidence applies strictly to [{$entity_attr}" . ($region ? " / {$region}" : "") . ($event ? " / {$event}" : "") . "] and must NOT be attributed to other regions/events -->";
            $doc_lbl = "Document";
            $body_lbl = "Content";
        } else {
            $comment_str = "<!-- 이 사료의 내용은 오직 [{$entity_attr}" . ($region ? " / {$region}" : "") . ($event ? " / {$event}" : "") . "] 서술에만 유효하며 타 지역/사건과 결합 금지 -->";
            $doc_lbl = "문서";
            $body_lbl = "내용";
        }

        $envelope = "<SOURCE_EVIDENCE id=\"{$id_attr}\" entity=\"{$entity_attr}\"" . ($region ? " region=\"{$region}\"" : "") . ($event ? " event=\"{$event}\"" : "") . ">\n"
                  . "  {$comment_str}\n"
                  . "  [{$doc_lbl}] {$doc}\n"
                  . "  [{$body_lbl}] {$body}\n"
                  . "</SOURCE_EVIDENCE>";

        $line_len = mb_strlen($envelope);
        if ($line_len > ($total_char_budget - $used_chars)) break;

        $lines[] = $envelope;
        $used_chars += $line_len;
    }
    return implode("\n\n", $lines);
}

function build_budgeted_pg_context($pg_texts, $lang, $max_items, $min_chars, $max_chars, $total_char_budget, $term = '', $focus = '') {
    if (is_bool($lang)) {
        $lang = $lang ? 'en' : 'ko';
    }
    $lines = [];
    $used_chars = 0;
    foreach ((array)$pg_texts as $idx => $t) {
        if (count($lines) >= $max_items) break;
        if (($total_char_budget - $used_chars) < ($min_chars + 120)) break;

        $text = normalize_whitespace_text($t);
        if ($text === '') continue;

        // node=..., rowid=... 추출
        $node_id = '';
        if (preg_match('/node=([^\s]+)/u', $text, $nm)) {
            $node_id = trim($nm[1]);
        }
        $rowid = '';
        if (preg_match('/rowid=([^\s]+)/u', $text, $rm)) {
            $rowid = trim($rm[1]);
        }
        $id_attr = $rowid ? "pg_{$rowid}" : "pg_" . ($idx + 1);
        $entity_attr = htmlspecialchars($node_id ?: "기록_{$idx}", ENT_QUOTES, 'UTF-8');

        // 지명(region) 및 사건(event) 추출
        $region = '';
        if (preg_match('/(지역|주소|본적|발생지):\s*([^;]+)/u', $text, $reg_m)) {
            $region = trim($reg_m[2]);
        } elseif (preg_match('/(평안[남북]도|함경[남북]도|충청[남북]도|전라[남북]도|경상[남북]도|강원도|경기도|황해도|평양|함흥|천안|병천|단천|간도|연해주|상해|진천|청주|용산|종로|보신각|대구|부산|통영|의주|해주|원산)[^\s,;]*/u', $text, $rm2)) {
            $region = $rm2[0];
        }

        $event = '';
        if (preg_match('/(사건명|제목):\s*([^;]+)/u', $text, $ev_m)) {
            $event = trim($ev_m[2]);
        } elseif (preg_match('/([가-힣\w]+(?:시위|만세|선언식|의거|봉기|집회|행진|사건|파견))/u', $text, $em2)) {
            $event = $em2[0];
        }

        // 초점 기반 필터링
        $is_spatial_query = preg_match('/(장소|지역|확산|전개|시위|만세)/u', $term . ' ' . $focus);
        $is_diplomatic = preg_match('/(파리강화회의|외교청원|청원서\s*서명|강화회의\s*파견)/u', $text);

        if ($is_spatial_query && $is_diplomatic) {
            $body = "[외교 및 기타 활동 약력 기록: {$event} 관련 명단 수록 (장소적 시위 확산과 직접 무관)]";
        } else {
            $remaining = $total_char_budget - $used_chars;
            $body_cap = max($min_chars, min($max_chars, $remaining - 120));
            $body = mb_substr($text, 0, $body_cap);
        }

        if ($lang === 'ja') {
            $comment_str = "<!-- この史料の内容は [{$entity_attr}" . ($region ? " / {$region}" : "") . ($event ? " / {$event}" : "") . "] の記述にのみ有効であり他地域・事件との混同厳禁 -->";
        } elseif ($lang === 'zh') {
            $comment_str = "<!-- 本史料内容仅对 [{$entity_attr}" . ($region ? " / {$region}" : "") . ($event ? " / {$event}" : "") . "] 的记述有效，严禁跨区域混淆 -->";
        } elseif ($lang === 'en') {
            $comment_str = "<!-- This archival evidence applies strictly to [{$entity_attr}" . ($region ? " / {$region}" : "") . ($event ? " / {$event}" : "") . "] and must NOT be attributed to other regions/events -->";
        } else {
            $comment_str = "<!-- 이 사료의 내용은 오직 [{$entity_attr}" . ($region ? " / {$region}" : "") . ($event ? " / {$event}" : "") . "] 서술에만 유효하며 타 지역/사건과 결합 금지 -->";
        }

        $envelope = "<SOURCE_EVIDENCE id=\"{$id_attr}\" entity=\"{$entity_attr}\"" . ($region ? " region=\"{$region}\"" : "") . ($event ? " event=\"{$event}\"" : "") . ">\n"
                  . "  {$comment_str}\n"
                  . "  {$body}\n"
                  . "</SOURCE_EVIDENCE>";

        $line_len = mb_strlen($envelope);
        if ($line_len > ($total_char_budget - $used_chars)) break;

        $lines[] = $envelope;
        $used_chars += $line_len;
    }

    if (empty($lines)) return '';
    if ($lang === 'ja') {
        $header = "[一次史料原文（PostgreSQL所蔵公文書）]\n";
    } elseif ($lang === 'zh') {
        $header = "[一手史料原文（PostgreSQL历史档案）]\n";
    } elseif ($lang === 'en') {
        $header = "[Primary Archival Sources (PostgreSQL)]\n";
    } else {
        $header = "[1차 사료 원문 (PostgreSQL)]\n";
    }
    return $header . implode("\n\n", $lines);
}

function add_node_to_map(&$map, $node, $labels_iterable) {
    if (!$node) return null;
    
    $props = [];
    if (method_exists($node, 'getProperties')) {
        foreach ($node->getProperties() as $k => $v) {
            if ($k === 'embedding') continue;
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

        // 허위 유령 개체 필터링 (불용어 또는 텍스트 오추출 잔여물 차단)
        static $ghost_stopwords = ['대표', '달하', '그러', '총독', '순사', '공판', '십자표', '펼치는데', '연행하', '거행하고'];
        if (in_array((string)$base, $ghost_stopwords, true) && empty($props['한글독음']) && empty($props['uid']) && empty($props['id'])) {
            return null;
        }

        $label_text = ($reading && $base != $reading) ? "{$base} ({$reading})" : $base;

        $color = "#999999"; $icon = "";
        $lstr = implode(' ', $labels_list);
        if (strpos($lstr, '문건') !== false || strpos($lstr, '사료') !== false) { $color = "#F7A01F"; $icon = "📜\n"; }
        elseif (strpos($lstr, '인물') !== false) { $color = "#2563EB"; $icon = "👤\n"; }
        elseif (strpos($lstr, '사건') !== false) { $color = "#DC2626"; $icon = "🔥\n"; }
        elseif (strpos($lstr, '장소') !== false) { $color = "#16A34A"; $icon = "📍\n"; }
        elseif (strpos($lstr, '기관') !== false) { $color = "#7C3AED"; $icon = "🏢\n"; }

        $type = (string)($props['type'] ?? '');
        if ($type === '') {
            $type_map = [
                '인물' => '인물', 'Person' => '인물',
                '장소' => '장소', 'Place' => '장소',
                '사건' => '사건', 'Event' => '사건',
                '기관' => '기관', 'Organization' => '기관',
                '사료' => '사료', '문건' => '사료', 'Document' => '사료',
            ];
            foreach ($labels_list as $label_name) {
                if (isset($type_map[$label_name])) {
                    $type = $type_map[$label_name];
                    break;
                }
            }
        }
        $map[$nid] = [
            "id" => $nid,
            "label" => $icon . mb_substr((string)$label_text, 0, 20),
            "raw_id" => (string)$raw_id,
            "labels" => $labels_list,
            "type" => $type,
            "props" => $props,
            "aliases" => [],
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
    $ko = [
        "P14_carried_out_by" => "수행(참여)",
        "P7_took_place_at" => "발생 장소",
        "ACTIVATED_AT" => "활동지",
        "P152_has_parent" => "가족 관계",
        "foaf:knows" => "동지/지인",
        "foaf:member" => "소속 기구",
        "P11_had_participant" => "참여 인물",
        "P108_has_produced" => "생성/저작",
        "P102_has_title" => "명칭/제목",
        "소속" => "소속",
        "동일인물" => "동일인물",
        "sameAs" => "동일인물",
        "owl:sameAs" => "동일인물"
    ];
    $en = [
        "P14_carried_out_by" => "Performed/Participated",
        "P7_took_place_at" => "Location",
        "ACTIVATED_AT" => "Activity place",
        "P152_has_parent" => "Family relation",
        "foaf:knows" => "Comrade/Acquaintance",
        "foaf:member" => "Affiliated organization",
        "P11_had_participant" => "Participant",
        "P108_has_produced" => "Created/Produced",
        "P102_has_title" => "Title",
        "소속" => "Affiliation",
        "동일인물" => "Same Person",
        "sameAs" => "Same As",
        "owl:sameAs" => "Same As"
    ];
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

    // 단일 쿼리로 tei 컬럼이 있는 모든 테이블의 전체 컬럼 정보를 한번에 수집 (N×2+1 → 1)
    $stmt = $pdo->query("
        SELECT c.table_schema, c.table_name, c.column_name, c.data_type, c.ordinal_position
        FROM information_schema.columns c
        INNER JOIN (
            SELECT DISTINCT table_schema, table_name
            FROM information_schema.columns
            WHERE column_name = 'tei' AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ) t USING (table_schema, table_name)
        WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY c.table_schema, c.table_name, c.ordinal_position ASC
    ");
    $all_cols = $stmt->fetchAll(PDO::FETCH_ASSOC);

    // PHP 측에서 테이블별로 id_col(첫 컬럼)과 text_cols를 동시 구성
    $tables_raw = [];
    foreach ($all_cols as $row) {
        $schema = (string)($row['table_schema'] ?? '');
        $table = (string)($row['table_name'] ?? '');
        $col_name = (string)($row['column_name'] ?? '');
        $data_type = (string)($row['data_type'] ?? '');
        $ordinal = (int)($row['ordinal_position'] ?? 0);

        if (!preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $schema) || !preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $table)) {
            continue;
        }
        $key = "{$schema}.{$table}";
        if (!isset($tables_raw[$key])) {
            $tables_raw[$key] = ['schema' => $schema, 'table' => $table, 'first_col' => null, 'text_cols' => []];
        }
        // 첫 컬럼 추적 (ordinal_position이 가장 작은 것)
        if ($tables_raw[$key]['first_col'] === null) {
            $tables_raw[$key]['first_col'] = $col_name;
        }
        // 텍스트 타입 컬럼 수집
        if (in_array($data_type, ['character varying', 'text', 'character'], true)) {
            if (preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $col_name)) {
                $tables_raw[$key]['text_cols'][] = $col_name;
            }
        }
    }

    $meta = [];
    foreach ($tables_raw as $entry) {
        $id_col = $entry['first_col'] ?? 'rowid';
        if (!preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $id_col)) {
            $id_col = 'rowid';
        }
        $text_cols = $entry['text_cols'];
        // 우선 컬럼을 앞으로 정렬
        foreach (['명칭', '사건명', '제목'] as $p) {
            if (($idx = array_search($p, $text_cols)) !== false) {
                array_splice($text_cols, $idx, 1); array_unshift($text_cols, $p);
            }
        }
        $meta[] = ['schema' => $entry['schema'], 'table' => $entry['table'], 'id_col' => $id_col, 'text_cols' => array_slice($text_cols, 0, 6)];
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
        // ID 정확매칭: names 개수만큼 IN 절 바인딩
        $in_placeholders = implode(',', array_fill(0, count($names), '?'));
        $id_match = "\"{$id_col}\"::text IN ({$in_placeholders})";
        foreach ($names as $name) {
            $params[] = $name;
        }

        // ILIKE 매칭: 패턴 배열을 한번만 생성, 각 컬럼에 재사용 (파라미터 폭발 방지)
        $like_patterns = array_map(fn($n) => "%{$n}%", $names);
        $like_placeholders = implode(',', array_fill(0, count($like_patterns), '?'));

        $text_clauses = [];
        foreach ($search_cols as $c) {
            if ($c === $id_col) continue;
            $text_clauses[] = "\"{$c}\"::text ILIKE ANY(ARRAY[{$like_placeholders}])";
            foreach ($like_patterns as $lp) $params[] = $lp;
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

// 4. Render frontend (docent.html with active session CSRF token)
if (!isset($_GET['ajax']) && !isset($_GET['csrf'])) {
    $csrf_token = htmlspecialchars($_SESSION['docent_csrf'] ?? '', ENT_QUOTES, 'UTF-8');
    $html_file = __DIR__ . '/docent.html';
    if (file_exists($html_file)) {
        $html = file_get_contents($html_file);
        $html = str_replace(
            '<head>',
            "<head>\n    <script>window.__PRELOADED_CSRF__ = '{$csrf_token}';</script>",
            $html
        );
        echo $html;
        exit;
    }
}
?>
