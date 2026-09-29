<?php
/**
 * includes/security.php — CSRF, Origin 검증, Rate-Limit, 입력 Sanitize
 */

// ── CSRF Secret / Token ──
function docent_get_csrf_secret() {
    static $secret = null;
    if ($secret !== null) return $secret;

    $env_secret = get_cfg('DOCENT_CSRF_SECRET', get_cfg('APP_SECRET', ''));
    if ($env_secret !== '') { $secret = $env_secret; return $secret; }

    $secret_file = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'docent_csrf_secret.key';
    if (file_exists($secret_file) && is_readable($secret_file)) {
        $stored = trim((string)@file_get_contents($secret_file));
        if (strlen($stored) >= 32) { $secret = $stored; return $secret; }
    }

    $candidate = bin2hex(random_bytes(32));
    if (@file_put_contents($secret_file, $candidate) !== false) { $secret = $candidate; return $secret; }

    $secret = hash('sha256', 'docent_salt_2026|' . get_cfg('PG_PASSWORD', '') . '|' . get_cfg('NEO4J_PASSWORD', '') . '|' . __FILE__);
    return $secret;
}

function docent_generate_csrf_token() {
    $secret = docent_get_csrf_secret();
    $timestamp = time();
    $nonce = bin2hex(random_bytes(16));
    $payload = $timestamp . '.' . $nonce;
    $hmac = hash_hmac('sha256', $payload, $secret);
    return $payload . '.' . $hmac;
}

if (empty($_SESSION['docent_csrf'])) {
    $_SESSION['docent_csrf'] = docent_generate_csrf_token();
}

// ── 입력 Sanitize ──
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
    if (!is_string($raw)) return [];
    $decoded = json_decode($raw, true);
    if (!is_array($decoded)) return [];

    $items = [];
    foreach ($decoded as $item) {
        if (is_array($item)) {
            $item = json_encode($item, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
        }
        $val = docent_sanitize_text((string)$item, $max_len, false);
        if ($val !== '') $items[] = $val;
        if (count($items) >= $max_items) break;
    }
    return $items;
}

function docent_sanitize_json_list($value, $max_items = 50, $max_len = 500) {
    $decoded = is_array($value) ? $value : json_decode((string)$value, true);
    if (!is_array($decoded)) return [];

    $items = [];
    foreach ($decoded as $item) {
        if (is_array($item)) {
            $item = json_encode($item, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
        }
        $val = docent_sanitize_text((string)$item, $max_len, false);
        if ($val !== '') $items[] = $val;
        if (count($items) >= $max_items) break;
    }
    return $items;
}

function docent_valid_identifier($value, $default = '') {
    $sanitized = preg_replace('/[^A-Za-z0-9_]/', '', (string)($value ?? $default));
    return $sanitized !== '' ? $sanitized : $default;
}

// ── Origin 검증 ──
function docent_check_same_origin() {
    $origin = trim((string)($_SERVER['HTTP_ORIGIN'] ?? ''));

    $default_allowed = 'https://gyungmin.tsar.kr,http://gyungmin.tsar.kr,https://11e.kr,http://11e.kr';
    $cfg_origins = get_cfg('DOCENT_ALLOWED_ORIGINS', $default_allowed);
    $allowed_origins = array_filter(array_map('trim', explode(',', $cfg_origins)));
    $allowed_origins = array_values(array_unique(array_merge($allowed_origins, explode(',', $default_allowed))));

    if ($origin !== '') {
        foreach ($allowed_origins as $allowed_origin) {
            if (strcasecmp($origin, rtrim($allowed_origin, '/')) === 0) {
                header("Access-Control-Allow-Origin: {$origin}");
                header('Access-Control-Allow-Credentials: true');
                header('Access-Control-Allow-Headers: Content-Type, X-CSRF-Token, X-Requested-With');
                header('Access-Control-Allow-Methods: GET, POST, OPTIONS');
                return true;
            }
        }
    } else {
        return true;
    }

    $origin_parts = parse_url($origin);
    if (!is_array($origin_parts) || !isset($origin_parts['host'])) return false;

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
    if ($is_local_origin && $is_local_host) return true;

    if (!in_array($origin_host, $host_candidates, true)) return false;
    if ($origin_scheme !== '' && $request_scheme !== '' && $origin_scheme !== $request_scheme) return false;
    if ($origin_port !== null && $request_port !== null && $origin_port !== $request_port) return false;

    header("Access-Control-Allow-Origin: {$origin}");
    header('Access-Control-Allow-Credentials: true');
    header('Access-Control-Allow-Headers: Content-Type, X-CSRF-Token, X-Requested-With');
    header('Access-Control-Allow-Methods: GET, POST, OPTIONS');
    return true;
}

// ── CSRF 검증 ──
function docent_check_csrf() {
    $request_token = $_SERVER['HTTP_X_CSRF_TOKEN'] ?? ($_POST['csrf_token'] ?? '');
    if (!is_string($request_token)) return false;
    $request_token = trim($request_token);
    if ($request_token === '') return false;

    // 1. Stateless HMAC token
    $parts = explode('.', $request_token);
    if (count($parts) === 3) {
        list($ts, $nonce, $hmac) = $parts;
        if (is_numeric($ts) && ctype_xdigit($nonce) && ctype_xdigit($hmac)) {
            $timestamp = (int)$ts;
            $now = time();
            if ($timestamp >= ($now - 86400) && $timestamp <= ($now + 300)) {
                $secret = docent_get_csrf_secret();
                $expected_hmac = hash_hmac('sha256', $ts . '.' . $nonce, $secret);
                if (hash_equals($expected_hmac, $hmac)) return true;
            }
        }
    }

    // 2. Session-based fallback
    if (isset($_SESSION['docent_csrf']) && is_string($_SESSION['docent_csrf']) && $_SESSION['docent_csrf'] !== '') {
        if (hash_equals((string)$_SESSION['docent_csrf'], $request_token)) return true;
    }

    return false;
}

// ── Rate-Limit ──
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

// ── 공통 보안 게이트 (AJAX 핸들러 진입 시 사용) ──
function docent_security_gate($action) {
    if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
        http_response_code(405);
        echo json_encode(['error' => 'Method not allowed.'], JSON_UNESCAPED_UNICODE);
        exit;
    }
    $allowed_actions = ['analyze', 'graph', 'pg_prefetch', 'explain', 'node_detail'];
    if (!in_array($action, $allowed_actions, true)) {
        http_response_code(400);
        echo json_encode(['error' => '허용되지 않은 요청입니다.'], JSON_UNESCAPED_UNICODE);
        exit;
    }
    if (!docent_check_same_origin()) {
        error_log(sprintf('Docent request rejected: invalid origin for action %s', $action));
        http_response_code(403);
        echo json_encode(['error' => '접속 출처가 올바르지 않습니다. 페이지를 새로고침해 주세요.'], JSON_UNESCAPED_UNICODE);
        exit;
    }
    if (!docent_check_csrf()) {
        error_log(sprintf('Docent request rejected: invalid CSRF token for action %s', $action));
        http_response_code(403);
        echo json_encode(['error' => '보안 토큰이 만료되었습니다. 페이지를 새로고침해 주세요.'], JSON_UNESCAPED_UNICODE);
        exit;
    }
    if (!docent_rate_limit($action)) {
        http_response_code(429);
        header('Retry-After: 60');
        echo json_encode(['error' => '요청이 너무 많습니다. 잠시 후 다시 시도하세요.'], JSON_UNESCAPED_UNICODE);
        exit;
    }
}
