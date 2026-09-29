<?php
/**
 * docent.php — 3.1 운동 역사 도슨트 API 라우터 및 프론트엔드 서빙
 *
 * 모듈 구조:
 * - includes/config.php   : 환경변수, 세션, 포커스 및 언어 설정
 * - includes/security.php : CSRF, Origin 검증, Rate-Limit, Sanitize
 * - includes/database.php : Neo4j, PostgreSQL 접속 및 메타데이터 캐시
 * - includes/gemini.php   : Gemini API 동기/스트리밍 호출
 * - includes/prompts.php  : 4개 언어별 RAG 해설 프롬프트 템플릿
 * - includes/helpers.php  : 노드맵, 엣지, evidence, merge, prefetch 등 그래프 처리
 * - includes/actions.php  : AJAX 액션 핸들러 (analyze, graph, pg_prefetch, explain, node_detail)
 */

require_once __DIR__ . '/includes/config.php';
require_once __DIR__ . '/includes/security.php';
require_once __DIR__ . '/includes/database.php';
require_once __DIR__ . '/includes/gemini.php';
require_once __DIR__ . '/includes/helpers.php';
require_once __DIR__ . '/includes/prompts.php';
require_once __DIR__ . '/includes/actions.php';

// ── 1. CORS Preflight ──
if (($_SERVER['REQUEST_METHOD'] ?? '') === 'OPTIONS') {
    docent_check_same_origin();
    header('Access-Control-Max-Age: 86400');
    http_response_code(204);
    exit;
}

// ── 2. CSRF Token Endpoint (프론트엔드 토큰 초기화) ──
if (isset($_GET['csrf']) || (isset($_GET['ajax']) && $_GET['ajax'] === 'csrf')) {
    header('Content-Type: application/json; charset=utf-8');
    header('X-Content-Type-Options: nosniff');
    header('X-Frame-Options: SAMEORIGIN');
    if (!docent_check_same_origin()) {
        http_response_code(403);
        echo json_encode(['error' => '접속 출처가 올바르지 않습니다.'], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
        exit;
    }
    $token = docent_generate_csrf_token();
    $_SESSION['docent_csrf'] = $token;
    echo json_encode(['csrf_token' => $token], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
    exit;
}

// ── 3. AJAX Dispatcher ──
if (isset($_GET['ajax'])) {
    $action = strtolower((string)($_GET['ajax'] ?? ''));

    // 공통 보안 게이트 통과 (Method, Action 화이트리스트, Origin, CSRF, Rate-limit)
    docent_security_gate($action);

    header('Content-Type: application/json; charset=utf-8');
    header('X-Content-Type-Options: nosniff');
    header('X-Frame-Options: SAMEORIGIN');

    try {
        match ($action) {
            'analyze'     => handle_action_analyze(),
            'graph'       => handle_action_graph(),
            'pg_prefetch' => handle_action_pg_prefetch(),
            'explain'     => handle_action_explain(),
            'node_detail' => handle_action_node_detail(),
            default       => throw new InvalidArgumentException("Unknown action: {$action}"),
        };
    } catch (\Throwable $e) {
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
    }
    exit;
}

// ── 4. HTML Frontend 렌더링 ──
$token = !empty($_SESSION['docent_csrf']) ? $_SESSION['docent_csrf'] : docent_generate_csrf_token();
$_SESSION['docent_csrf'] = $token;
$csrf_token = htmlspecialchars($token, ENT_QUOTES, 'UTF-8');
$html_file = __DIR__ . '/docent.html';

if (file_exists($html_file)) {
    $html = file_get_contents($html_file);
    echo str_replace(
        '<head>',
        "<head>\n    <script>window.__PRELOADED_CSRF__ = '{$csrf_token}';</script>",
        $html
    );
    exit;
}

http_response_code(404);
echo "docent.html not found.";
