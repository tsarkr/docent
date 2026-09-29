<?php
/**
 * includes/gemini.php — Gemini API 동기/스트리밍 호출
 */

function get_gemini_candidate_models() {
    $configured = trim((string)get_cfg('GEMINI_MODEL', 'gemini-3.5-flash'));
    $candidates = [
        $configured, 'gemini-3.5-flash', 'gemini-3.5-flash-lite',
        'gemini-3.8-flash', 'gemini-3-flash-preview', 'gemini-3.1-flash-lite',
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

/**
 * 동기식 Gemini API 호출 (model fallback 포함)
 */
function call_gemini($msgs, $is_json = false, $max_tokens = null, $temperature = null, $top_p = null, $timeout = 25) {
    $api_key = get_cfg('GEMINI_API_KEY') ?: get_cfg('API_KEY');
    if (!$api_key) return $is_json ? '{"explanation":"API Key missing"}' : docent_t("API Key가 설정되지 않았습니다.", "API key is not configured.");

    $candidate_models = get_gemini_candidate_models();
    $payload = _build_gemini_payload($msgs, $is_json, $max_tokens, $temperature, $top_p);
    $post_json = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);

    $last_error_message = '';
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
        $http_code = (int)curl_getinfo($ch, CURLINFO_RESPONSE_CODE);

        if ($res === false || $curl_errno !== 0 || $http_code !== 200) {
            $provider_message = '';
            $error_data = json_decode((string)$res, true);
            if (is_array($error_data['error'] ?? null)) {
                $provider_message = docent_sanitize_text($error_data['error']['message'] ?? '', 240, false);
            }
            $last_error_message = $provider_message !== '' ? "Gemini 오류 ({$model}): {$provider_message}" : "AI 요청 실패 ({$model}, HTTP {$http_code}, cURL {$curl_errno})";
            error_log(sprintf('Gemini model %s failed: HTTP %d, cURL %d — trying next candidate', $model, $http_code, $curl_errno));
            continue;
        }

        $content = _extract_gemini_text($res);
        if (trim($content) !== '') return $content;
    }

    $safe_message = $last_error_message !== '' ? $last_error_message : "AI 응답을 생성하지 못했습니다.";
    return $is_json
        ? json_encode(["explanation" => $safe_message], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE)
        : $safe_message;
}

/**
 * SSE 스트리밍 Gemini API 호출
 */
function stream_gemini($msgs, callable $on_chunk, $max_tokens = 8192, $temperature = null, $top_p = null) {
    $api_key = get_cfg('GEMINI_API_KEY') ?: get_cfg('API_KEY');
    if (!$api_key) {
        $on_chunk(docent_t("API Key가 설정되지 않았습니다.", "API key is not configured."));
        return;
    }

    $candidate_models = get_gemini_candidate_models();
    $payload = _build_gemini_payload($msgs, false, $max_tokens, $temperature, $top_p);
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
                    $buffer = substr($buffer, strlen($matches[0]));
                    $lines = preg_split('/\r?\n/', $block);
                    foreach ($lines as $line) {
                        $line = trim($line);
                        if (str_starts_with($line, 'data: ')) {
                            $parsed = json_decode(substr($line, 6), true);
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

        if ($chunks_delivered > 0 && $curl_errno === 0) return;

        error_log(sprintf('stream_gemini model %s failed (HTTP %d, cURL %d) — trying next model', $model, $http_code, $curl_errno));
    }

    $on_chunk("\n\n[AI 해설 생성 중 서비스 지연이 발생했습니다. 잠시 후 다시 시도해 주세요.]");
}

// ── 내부 헬퍼 ──
function _build_gemini_payload($msgs, $is_json, $max_tokens, $temperature, $top_p) {
    $system_prompt = "";
    $contents = [];
    foreach ($msgs as $m) {
        if ($m['role'] === 'system') {
            $system_prompt = $m['content'];
        } else {
            $role = ($m['role'] === 'assistant') ? 'model' : 'user';
            $contents[] = ["role" => $role, "parts" => [["text" => $m['content']]]];
        }
    }

    $default_temp = (float)get_cfg('GEMINI_TEMPERATURE', '0.1');
    $default_topp = (float)get_cfg('GEMINI_TOP_P', '0.85');

    $payload = [
        "contents" => $contents,
        "generationConfig" => [
            "temperature" => is_numeric($temperature) ? (float)$temperature : $default_temp,
            "topP" => is_numeric($top_p) ? (float)$top_p : $default_topp,
            "maxOutputTokens" => $max_tokens ? (int)max(1, min(8192, $max_tokens)) : 4096,
        ]
    ];

    if ($system_prompt !== "") {
        $payload["system_instruction"] = ["parts" => [["text" => $system_prompt]]];
    }
    if ($is_json) {
        $payload["generationConfig"]["response_mime_type"] = "application/json";
    }
    return $payload;
}

function _extract_gemini_text($json_response) {
    $data = json_decode($json_response, true);
    $content = '';
    if (isset($data['candidates'][0]['content']['parts']) && is_array($data['candidates'][0]['content']['parts'])) {
        foreach ($data['candidates'][0]['content']['parts'] as $part) {
            if (isset($part['text'])) $content .= $part['text'];
        }
    }
    return $content;
}
