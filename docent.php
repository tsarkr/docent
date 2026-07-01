<?php
/**
 * 3.1 운동 역사 도슨트 - Advanced Robust RAG Version (Node Click & Info Panel Edition)
 * Handles Interactive Graph node clicking to show localized metadata and evidences.
 */

require_once __DIR__ . '/vendor/autoload.php';

use Dotenv\Dotenv;
use Laudis\Neo4j\ClientBuilder;
use Laudis\Neo4j\Authentication\Authenticate;

// 1. Load Environment Variables
if (file_exists(__DIR__ . '/.env')) {
    $dotenv = Dotenv::createImmutable(__DIR__);
    $dotenv->load();
}

// Utility to get config safely
function get_cfg($key, $default = '') {
    $val = getenv($key);
    if ($val !== false) return trim($val);
    if (isset($_ENV[$key])) return trim($_ENV[$key]);
    if (isset($_SERVER[$key])) return trim($_SERVER[$key]);
    return $default;
}

$gtm_id = get_cfg('GTM_ID');
$ga_measurement_id = get_cfg('GA_MEASUREMENT_ID');

// 2. API Logic (AJAX Handlers)
if (isset($_GET['ajax'])) {
    header('Content-Type: application/json; charset=utf-8');
    $action = $_GET['ajax'];
    
    try {
        // [Action 1] 의도 분석
        if ($action === 'analyze') {
            $term = $_POST['term'] ?? '';
            $system_prompt = "당신은 역사 지식 검색을 위한 전문 분석가입니다. 사용자의 질문을 분석하여 다음 JSON 형식으로만 답하십시오.\n"
                           . "{\n"
                           . "  \"intent\": \"ENTITY_SEARCH\",\n"
                           . "  \"keywords\": [\"" . addslashes($term) . "\"],\n"
                           . "  \"focus\": \"인물\",\n"
                           . "  \"explanation\": \"검색어 기반 분석 수행\"\n"
                           . "}\n"
                           . "부가적인 설명 없이 JSON만 반환하십시오.";

            $res = call_deepseek([
                ["role" => "system", "content" => $system_prompt],
                ["role" => "user", "content" => (string)$term]
            ], true);
            
            $parsed = json_decode($res, true);
            $output = [
                "intent" => $parsed['intent'] ?? 'ENTITY_SEARCH',
                "keywords" => $parsed['keywords'] ?? [$term],
                "focus" => $parsed['focus'] ?? '인물',
                "explanation" => $parsed['explanation'] ?? '분석 완료'
            ];
            
            echo json_encode($output, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
            exit;
        }

        // [Action 2] Neo4j 지식망 및 증거 탐색 (Closure Safe & Edge Fix)
        if ($action === 'graph') {
            $term = $_POST['term'] ?? '';
            $keywords = json_decode($_POST['keywords'] ?? '[]', true);
            if (empty($keywords)) $keywords = [$term];
            
            $client = get_neo4j();
            $nodes = [];
            $edges = [];
            $evidences = [];
            $found_ids = [];

            $search_query = "
                MATCH (n)
                WHERE any(k IN ['명칭','한글독음','한글명칭','제목','사건명','id','name','title','uid'] 
                          WHERE n[k] IS NOT NULL AND toLower(toString(n[k])) CONTAINS toLower(\$term))
                RETURN DISTINCT n, labels(n) as labels
                LIMIT 50
            ";
            
            foreach ($keywords as $kw) {
                if (empty($kw)) continue;
                $res1 = $client->run($search_query, ['term' => $kw]);
                
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
                        $evidences[(string)$node_id] = [
                            "doc" => (string)($props['제목'] ?? '제목 미상'),
                            "quote" => mb_substr((string)($props['설명'] ?? ''), 0, 500),
                            "concept" => (string)$node_id,
                            "text" => mb_substr((string)($props['설명'] ?? ''), 0, 1000)
                        ];
                    } elseif (in_array('사건', $labels)) {
                        $evidences[(string)$node_id] = [
                            "doc" => (string)($props['사건명'] ?? '사건명 미상'),
                            "quote" => (string)($props['날짜'] ?? ''),
                            "concept" => (string)$node_id,
                            "text" => "날짜: " . ($props['날짜'] ?? '') . "\n설명: " . mb_substr((string)($props['설명'] ?? ''), 0, 1000)
                        ];
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
                    RETURN DISTINCT n, r, m, n_labels, m_labels, e, p, labels(e) as e_labels, labels(p) as p_labels
                ";

                $res2 = $client->run($graph_query, ['search_ids' => $found_ids]);
                foreach ($res2 as $rec) {
                    $n = $rec->get('n'); $m = $rec->get('m'); $r = $rec->get('r');
                    $e = $rec->get('e'); $p = $rec->get('p');

                    $n_nid = add_node_to_map($nodes, $n, $rec->get('n_labels'));
                    $m_nid = $m ? add_node_to_map($nodes, $m, $rec->get('m_labels')) : null;

                    if ($r && $n_nid && $m_nid) {
                        $edges[] = ["from" => $n_nid, "to" => $m_nid, "label" => _get_rel_label(method_exists($r, 'getType') ? $r->getType() : '연결')];
                    }

                    if ($e && $p) {
                        $e_nid = add_node_to_map($nodes, $e, $rec->get('e_labels'));
                        $p_nid = add_node_to_map($nodes, $p, $rec->get('p_labels'));
                        if ($n_nid && $e_nid) $edges[] = ["from" => $n_nid, "to" => $e_nid, "label" => "수행/참여"];
                        if ($e_nid && $p_nid) $edges[] = ["from" => $e_nid, "to" => $p_nid, "label" => "수행/참여"];
                    }
                }
            }

            $unique_edges = [];
            foreach ($edges as $e) {
                $key = $e['from'] . '-' . $e['to'] . '-' . $e['label'];
                $unique_edges[$key] = $e;
            }

            echo json_encode([
                "nodes" => array_values($nodes),
                "edges" => array_values($unique_edges),
                "evidences" => array_values($evidences)
            ], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
            exit;
        }

        // [Action 3] PostgreSQL 동적 연관 사료 수집
        if ($action === 'pg_prefetch') {
            $names = json_decode($_POST['names'] ?? '[]', true);
            $pdo = get_pg();
            if (!$pdo) { echo json_encode([], JSON_UNESCAPED_UNICODE); exit; }

            $all_rows = [];
            $tables_meta = get_pg_tables_metadata($pdo);
            $seen = [];

            foreach ((array)$names as $name) {
                if (empty($name) || isset($seen[$name])) continue;
                $seen[$name] = true;

                $rows = fetch_pg_rows_for_name($pdo, $name, $tables_meta);
                foreach ($rows as $r) {
                    $r['node_id'] = $name;
                    $all_rows[] = $r;
                }
            }

            echo json_encode($all_rows, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
            exit;
        }

        // [Action 4] AI RAG 도슨트 해설 생성
        if ($action === 'explain') {
            $term = $_POST['term'] ?? '';
            $evidences = json_decode($_POST['evidences'] ?? '[]', true) ?? [];
            $pg_texts = json_decode($_POST['pg_texts'] ?? '[]', true) ?? [];

            $context_str = "";
            if (!empty($evidences)) {
                foreach ($evidences as $e) {
                    $context_str .= "- 문서: {$e['doc']}\n  내용: {$e['text']}...\n";
                }
            }
            if (!empty($pg_texts)) {
                if ($context_str) $context_str .= "\n\n";
                $context_str .= "[PG 근거]\n" . implode("\n", array_map(fn($t) => "- " . $t, $pg_texts));
            }

            $prompt = "당신은 역사 도슨트입니다. 다음 사료와 PG 근거를 바탕으로 '{$term}'에 대해 균형잡힌 해설을 작성하세요.\n"
                    . "- 사실/추정/논쟁 지점을 구분해 서술합니다.\n"
                    . "- 감정적·과장 표현을 피하고 중립적 톤을 유지합니다.\n"
                    . "- 근거가 부족한 부분은 명확히 한계를 밝힙니다.\n\n"
                    . "[사료/PG 근거]\n{$context_str}";

            $res = call_deepseek([
                ["role" => "system", "content" => "당신은 역사 도슨트입니다. 한국어 해설만 작성하십시오."],
                ["role" => "user", "content" => $prompt]
            ], false);

            echo json_encode(["text" => $res], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
            exit;
        }

    } catch (Throwable $e) {
        http_response_code(500);
        echo json_encode([
            "error" => $e->getMessage() . " (Line: " . $e->getLine() . ")"
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

function call_deepseek($msgs, $is_json = false) {
    $api_key = get_cfg('DEEPSEEK_API_KEY');
    if (!$api_key) return $is_json ? '{"explanation":"API Key missing"}' : "API Key가 설정되지 않았습니다.";

    $ch = curl_init(rtrim(get_cfg('DEEPSEEK_BASE_URL', 'https://api.deepseek.com'), '/') . '/chat/completions');
    $payload = [
        "model" => get_cfg('DEEPSEEK_MODEL', 'deepseek-v4-flash'),
        "messages" => $msgs,
        "temperature" => 0.0
    ];
    if ($is_json) $payload["response_format"] = ["type" => "json_object"];

    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => json_encode($payload),
        CURLOPT_HTTPHEADER => ["Authorization: Bearer " . $api_key, "Content-Type: application/json"],
        CURLOPT_TIMEOUT => 120
    ]);
    
    $res = curl_exec($ch);
    curl_close($ch);
    
    $data = json_decode($res, true);
    $content = $data['choices'][0]['message']['content'] ?? '';

    if ($is_json) return $content ?: '{"explanation":"JSON Content empty"}';
    return $content ?: "해설을 생성할 수 없습니다.";
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
        elseif (strpos($lstr, '인물') !== false) { $color = "#1CE1D4"; $icon = "👤\n"; }
        elseif (strpos($lstr, '사건') !== false) { $color = "#FF6B6B"; $icon = "🔥\n"; }
        elseif (strpos($lstr, '장소') !== false) { $color = "#4ECDC4"; $icon = "📍\n"; }
        elseif (strpos($lstr, '기관') !== false) { $color = "#95E1D3"; $icon = "🏢\n"; }

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
    $map = ["P14_carried_out_by" => "수행(참여)", "P7_took_place_at" => "발생 장소", "P152_has_parent" => "가족 관계", "foaf:knows" => "동지/지인", "foaf:member" => "소속 기구", "P11_had_participant" => "참여 인물", "P108_has_produced" => "생성/저작", "P102_has_title" => "명칭/제목", "소속" => "소속"];
    return $map[$rtype] ?? $rtype;
}

function get_pg_tables_metadata($pdo) {
    $stmt = $pdo->query("SELECT table_schema, table_name FROM information_schema.columns WHERE column_name = 'tei' AND table_schema NOT IN ('pg_catalog', 'information_schema') GROUP BY table_schema, table_name");
    $tables = $stmt->fetchAll(PDO::FETCH_ASSOC);
    $meta = [];

    foreach ($tables as $t) {
        $schema = $t['table_schema']; $table = $t['table_name'];
        $s1 = $pdo->prepare("SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position ASC LIMIT 1");
        $s1->execute([$schema, $table]);
        $id_col = $s1->fetchColumn() ?: 'rowid';

        $s2 = $pdo->prepare("SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND data_type IN ('character varying','text','character') ORDER BY ordinal_position ASC");
        $s2->execute([$schema, $table]);
        $text_cols = $s2->fetchAll(PDO::FETCH_COLUMN);

        foreach (['명칭', '사건명', '제목'] as $p) {
            if (($idx = array_search($p, $text_cols)) !== false) {
                array_splice($text_cols, $idx, 1); array_unshift($text_cols, $p);
            }
        }
        $meta[] = ['schema' => $schema, 'table' => $table, 'id_col' => $id_col, 'text_cols' => array_slice($text_cols, 0, 6)];
    }
    return $meta;
}

function fetch_pg_rows_for_name($pdo, $name, $tables_meta) {
    $out = [];
    foreach ($tables_meta as $m) {
        $fq = ($m['schema'] && $m['schema'] !== 'public') ? "\"{$m['schema']}\".\"{$m['table']}\"" : "\"{$m['table']}\"";
        $search_cols = [$m['id_col'], 'tei'];
        foreach ($m['text_cols'] as $c) if (!in_array($c, $search_cols)) $search_cols[] = $c;

        $clauses = []; foreach ($search_cols as $c) $clauses[] = "\"{$c}\"::text ILIKE ?";
        $q = "SELECT * FROM {$fq} WHERE (" . implode(" OR ", $clauses) . ") LIMIT 5";
        
        try {
            $stmt = $pdo->prepare($q);
            $stmt->execute(array_fill(0, count($clauses), "%{$name}%"));
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            foreach ($rows as $row) {
                $snippets = [];
                foreach ($row as $k => $v) if ($k !== $m['id_col'] && $k !== 'tei') $snippets[$k] = $v !== null ? strval($v) : '';
                $out[] = ['table' => $m['table'], 'schema' => $m['schema'], 'rowid' => $row[$m['id_col']] ?? null, 'id_col' => $m['id_col'], 'match_cols' => $search_cols, 'tei' => $row['tei'] ?? null, 'snippets' => $snippets];
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
    <?php if (!empty($gtm_id)): ?>
    <!-- Google Tag Manager -->
    <script>(function(w,d,s,l,i){w[l]=w[l]||[];w[l].push({'gtm.start':
    new Date().getTime(),event:'gtm.js'});var f=d.getElementsByTagName(s)[0],
    j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src=
    'https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);
    })(window,document,'script','dataLayer','<?= htmlspecialchars($gtm_id, ENT_QUOTES, 'UTF-8') ?>');</script>
    <!-- End Google Tag Manager -->
    <?php endif; ?>

    <?php if (!empty($ga_measurement_id)): ?>
    <!-- Google tag (gtag.js) -->
    <script async src="https://www.googletagmanager.com/gtag/js?id=<?= urlencode($ga_measurement_id) ?>"></script>
    <script>
        window.dataLayer = window.dataLayer || [];
        function gtag(){dataLayer.push(arguments);}
        gtag('js', new Date());
        gtag('config', '<?= htmlspecialchars($ga_measurement_id, ENT_QUOTES, 'UTF-8') ?>');
    </script>
    <?php endif; ?>
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
<?php if (!empty($gtm_id)): ?>
<!-- Google Tag Manager (noscript) -->
<noscript><iframe src="https://www.googletagmanager.com/ns.html?id=<?= urlencode($gtm_id) ?>"
height="0" width="0" style="display:none;visibility:hidden"></iframe></noscript>
<!-- End Google Tag Manager (noscript) -->
<?php endif; ?>
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
                        <button onclick="setQuery('<?= $kw ?>')" class="btn btn-outline-secondary btn-sm text-start">📌 <?= $kw ?></button>
                    <?php endforeach; ?>
                </div>
            </div>

            <div id="analysis-box" class="card bg-light p-3 mb-3 small" style="display:none">
                <div class="fw-bold text-primary mb-2"><i class="bi bi-cpu"></i> AI 질의 분석</div>
                <div class="mb-1">🎯 <strong>의도:</strong> <span id="intent-val"></span></div>
                <div class="mb-1">🔍 <strong>초점:</strong> <span id="focus-val"></span></div>
                <div class="text-muted mt-1" id="explanation-val" style="font-size:0.75rem;"></div>
            </div>

            <div id="status-log" class="text-muted">
                <div id="status-text">준비됨.</div>
            </div>
        </div>

        <div class="col-md-10 main-content">
            <h3 id="search-title" class="mb-4 fw-bold">역사를 탐색해 보세요.</h3>
            <div class="row">
                <div class="col-lg-6">
                    <div id="graph"></div>
                    <div class="d-flex justify-content-center gap-3 mb-4 flex-wrap">
                        <div class="legend-item"><div class="legend-color" style="background: #F7A01F;"></div> 사료</div>
                        <div class="legend-item"><div class="legend-color" style="background: #1CE1D4;"></div> 인물</div>
                        <div class="legend-item"><div class="legend-color" style="background: #FF6B6B;"></div> 사건</div>
                        <div class="legend-item"><div class="legend-color" style="background: #4ECDC4;"></div> 장소</div>
                        <div class="legend-item"><div class="legend-color" style="background: #95E1D3;"></div> 기관</div>
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
                        <div class="d-flex justify-content-between align-items-center mb-4">
                            <h5 class="fw-bold m-0"><i class="bi bi-chat-dots-fill text-primary"></i> 도슨트 해설</h5>
                            <button id="explainBtn" onclick="generateExplanation()" class="btn btn-sm btn-primary" style="display:none;">
                                <i class="bi bi-stars"></i> 해설 생성
                            </button>
                        </div>
                        <div id="explanation-content" class="text-secondary" style="line-height: 1.7; min-height: 100px;">
                            검색어를 입력하고 탐색 버튼을 누르면 인프라가 작동합니다.
                        </div>
                        
                        <div id="rag-section" style="display:none">
                            <h6 class="fw-bold mt-5 mb-3 border-top pt-3"><i class="bi bi-journal-text"></i> 수집된 사료/PG 근거</h6>
                            <div id="rag-summary" class="small mb-3"></div>
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

function setQuery(q) {
    document.getElementById('q').value = q;
    performSearch();
}

async function api(act, data = {}) {
    const fd = new FormData();
    for (let k in data) fd.append(k, data[k]);
    const response = await fetch(`?ajax=${act}`, { method: 'POST', body: fd });
    
    const rawText = await response.text();
    
    if (!response.ok) {
        try {
            const err = JSON.parse(rawText);
            throw new Error(`[${act} 단계 오류] ` + (err.error || '서버 오류'));
        } catch(e) {
            throw new Error(`[${act} 단계 500 에러] ` + rawText.substring(0, 150));
        }
    }
    
    try {
        return JSON.parse(rawText);
    } catch (e) {
        throw new Error(`[${act} 단계 JSON 파싱 실패] 원문: ` + rawText.substring(0, 150));
    }
}

async function performSearch() {
    const term = document.getElementById('q').value.trim();
    if (term.length < 2) return alert('2글자 이상 입력하세요.');

    document.getElementById('analysis-box').style.display = 'none';
    document.getElementById('explainBtn').style.display = 'none';
    document.getElementById('rag-section').style.display = 'none';
    document.getElementById('node-info-panel').style.display = 'none'; // 초기화 시 노드 정보 패널 숨김
    pgPrefetchTexts = [];
    currentNodes = [];

    setStatus('<span class="spinner-border spinner-border-sm"></span> 의도 분석 중...');
    document.getElementById('search-title').innerText = `'${term}' 분석 중...`;
    
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

        const nodeNames = graphData.nodes.map(n => n.raw_id);
        if (nodeNames.length > 0) {
            setStatus('<span class="spinner-border spinner-border-sm"></span> PostgreSQL 사료 원문 연동 중...');
            const pgRows = await api('pg_prefetch', { names: JSON.stringify(nodeNames) });
            
            pgPrefetchTexts = pgRows.map(r => {
                let sStr = [];
                for(let k in r.snippets) { if(r.snippets[k]) sStr.push(`${k}: ${r.snippets[k]}`); }
                return `[${r.schema}.${r.table}] node=${r.node_id} rowid=${r.rowid} :: ${sStr.join('; ')}`;
            });
        }

        document.getElementById('search-title').innerText = `'${term}' 지식망 탐색 완료`;
        document.getElementById('explanation-content').innerHTML = "지식 구조 및 근거 수집 완료. <strong>'해설 생성'</strong> 버튼을 클릭하면 RAG 분석이 시작됩니다.";
        document.getElementById('explainBtn').style.display = 'inline-block';

        const ragUsedDocCount = lastEvidences.length;
        const ragUsedPgCount = pgPrefetchTexts.length;
        const hasNeo4jDocs = ragUsedDocCount > 0;
        const targetLabels = [...new Set(lastEvidences.map(ev => `${ev.doc} (대상: ${ev.concept})`))];

        let summaryHtml = `<div class="p-2 bg-light border rounded">
            <div class="fw-bold text-dark mb-2"><i class="bi bi-info-circle"></i> 근거 사용 현황</div>`;

        if (hasNeo4jDocs) {
            summaryHtml += `<div class="mb-1">검색된 사료 수: <span class="badge bg-warning text-dark">${lastEvidences.length}</span></div>
            <div class="mb-1">RAG 사용 사료 수: <span class="badge bg-primary">${ragUsedDocCount}</span></div>`;
        } else {
            summaryHtml += `<div class="mb-1 text-muted"><i class="bi bi-info-circle"></i> 이번 결과는 Neo4j 사료 없이 PG 근거 중심으로 구성되었습니다.</div>`;
        }

        summaryHtml += `
            <div class="mb-1">검색된 PG 근거 수: <span class="badge bg-success">${pgPrefetchTexts.length}</span></div>
            <div class="mb-2">RAG 사용 PG 근거 수: <span class="badge bg-primary">${ragUsedPgCount}</span></div>`;

        if (hasNeo4jDocs && targetLabels.length > 0) {
            const targetPreview = targetLabels.slice(0, 6).map(label => `<span class="badge text-bg-light border me-1 mb-1">${escapeHtml(label)}</span>`).join('');
            const remain = targetLabels.length > 6 ? `<div class="text-muted mt-1">외 ${targetLabels.length - 6}건</div>` : '';
            summaryHtml += `<div class="mt-2"><div class="fw-semibold mb-1">대상 사료</div>${targetPreview}${remain}</div>`;
        }
        summaryHtml += `</div>`;
        document.getElementById('rag-summary').innerHTML = summaryHtml;

        let ragHtml = '';
        lastEvidences.forEach((ev) => {
            const usageBadge = '<span class="badge bg-primary ms-2">RAG 사용</span>';
            ragHtml += `<div class="p-2 mb-2 bg-light border-start border-warning border-3 rounded small">
                <div class="fw-bold text-dark">📜 ${escapeHtml(ev.doc)} <span class="text-muted">(대상: ${escapeHtml(ev.concept)})</span>${usageBadge}</div>
                <div class="text-muted mt-1">${escapeHtml(ev.text).substring(0, 300)}...</div>
            </div>`;
        });
        pgPrefetchTexts.forEach((txt) => {
            const usageBadge = '<span class="badge bg-primary ms-2">RAG 사용</span>';
            ragHtml += `<div class="p-2 mb-2 bg-light border-start border-success border-3 rounded small">
                <div class="fw-bold text-dark"><i class="bi bi-database"></i> PostgreSQL 사료${usageBadge}</div>
                <div class="text-muted mt-1">${escapeHtml(txt)}</div>
            </div>`;
        });

        if (ragHtml) {
            document.getElementById('rag-evidence-list').innerHTML = ragHtml;
            document.getElementById('rag-section').style.display = 'block';
        }
        setStatus('완료');

    } catch (e) {
        console.error(e);
        setStatus(`<span class="text-danger"><i class="bi bi-exclamation-triangle"></i> 에러: ${e.message}</span>`);
        document.getElementById('explanation-content').innerHTML = "데이터 탐색 과정에서 실패했습니다. 에러 내용을 확인해 주세요.";
    }
}

async function generateExplanation() {
    const term = document.getElementById('q').value.trim();
    document.getElementById('explanation-content').innerHTML = '<div class="text-center p-4"><div class="spinner-border text-primary" role="status"></div><br><small class="text-muted mt-2 d-inline-block">해설 생성중...</small></div>';
    
    try {
        const res = await api('explain', {
            term,
            evidences: JSON.stringify(lastEvidences),
            pg_texts: JSON.stringify(pgPrefetchTexts)
        });
        document.getElementById('explanation-content').innerHTML = marked.parse(res.text);
    } catch (e) {
        document.getElementById('explanation-content').innerHTML = `<span class='text-danger'>해설 생성 중 오류: ${e.message}</span>`;
    }
}

function setStatus(html) {
    document.getElementById('status-text').innerHTML = html;
}

function escapeHtml(value) {
    return String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
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
        relatedEvidences.forEach(ev => {
            html += `<div class="mb-2 p-2 bg-light border rounded small">
                <strong class="text-dark">${ev.doc}</strong><br>
                <span class="text-muted">${ev.text.substring(0, 150)}...</span>
            </div>`;
        });
    }

    // 관련된 PostgreSQL 근거 텍스트 찾기
    const relatedPg = pgPrefetchTexts.filter(txt => txt.includes(`node=${node.raw_id}`));
    if (relatedPg.length > 0) {
        html += `<hr><h6 class="fw-bold text-success mb-2"><i class="bi bi-database-fill"></i> PostgreSQL 관련 근거</h6>`;
        relatedPg.forEach(txt => {
            // DB 출처 정보 강조 (예: [public.서지정보_260410])
            const parts = txt.split('::');
            const source = parts[0];
            const detail = parts[1] ? parts[1].substring(0, 150) : '';
            html += `<div class="mb-2 p-2 bg-light border rounded small">
                <strong class="text-success">${source.replace(/\[|\]/g, '')}</strong><br>
                <span class="text-muted">${detail}...</span>
            </div>`;
        });
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