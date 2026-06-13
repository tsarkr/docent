<?php
/**
 * 3.1 운동 역사 도슨트 - Advanced PHP Version
 * Aligned with app_v2.py functionality and structure.
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

// Utility to get config
function get_cfg($key, $default = '') {
    return $_ENV[$key] ?? getenv($key) ?: $default;
}

// 2. API Logic (AJAX Handlers)
if (isset($_GET['ajax'])) {
    header('Content-Type: application/json; charset=utf-8');
    $action = $_GET['ajax'];
    
    try {
        if ($action === 'analyze') {
            $term = $_POST['term'] ?? '';
            $system_prompt = "당신은 역사 지식 검색을 위한 전문 분석가입니다. 사용자의 질문을 분석하여 다음 JSON 형식으로만 답하십시오.\n"
                . "{\n"
                . "  \"intent\": \"질문의 의도 (ENTITY_SEARCH, RELATION_FIND, TEMPORAL_QUERY, DOCUMENT_REQUEST 중 하나)\",\n"
                . "  \"keywords\": [\"핵심 키워드 1\", \"핵심 키워드 2\", \"핵심 키워드 3\"],\n"
                . "  \"focus\": \"분석의 핵심 초점 (인물, 사건, 장소 등)\",\n"
                . "  \"explanation\": \"의도 분석에 대한 간략한 이유\"\n"
                . "}\n"
                . "- ENTITY_SEARCH: 특정 인물, 장소, 사건 등에 대한 일반적인 정보 검색\n"
                . "- RELATION_FIND: 두 개체 간의 관계나 연결 고리 검색\n"
                . "- TEMPORAL_QUERY: 특정 시기나 시간 순서에 따른 사건 검색\n"
                . "- DOCUMENT_REQUEST: 특정 사료나 문헌 검색\n"
                . "부가적인 설명 없이 JSON만 반환하십시오.";
            
            $res = call_deepseek([
                ["role" => "system", "content" => $system_prompt],
                ["role" => "user", "content" => (string)$term]
            ], ["type" => "json_object"]);
            echo $res;
            exit;
        }

        if ($action === 'graph') {
            $keywords = json_decode($_POST['keywords'] ?? '[]', true);
            if (empty($keywords)) {
                $term = $_POST['term'] ?? '';
                $keywords = [$term];
            }
            
            $client = get_neo4j();
            $found_ids = [];
            $evidences = [];
            $nodes = [];
            $edges = [];
            $queries = [];

            // Step 1: Initial search for nodes
            $search_query = "
                MATCH (n)
                WHERE any(k IN ['명칭','한글독음','한글명칭','제목','사건명','id','name','title','uid'] 
                          WHERE n[k] IS NOT NULL AND toLower(toString(n[k])) CONTAINS toLower(\$term))
                RETURN DISTINCT n
                LIMIT 50
            ";
            $queries[] = ["title" => "초기 노드 검색", "query" => $search_query];

            foreach ($keywords as $term) {
                if (empty($term)) continue;
                $res = $client->run($search_query, ['term' => $term]);
                foreach ($res as $record) {
                    $node = $record->get('n');
                    $props = $node->getProperties();
                    $labels = (array)$node->getLabels();
                    $node_id = $props['uid'] ?? $props['id'] ?? $props['명칭'] ?? $props['name'] ?? $props['title'] ?? 'unknown';
                    $found_ids[] = (string)$node_id;

                    if (in_array('문건', $labels)) {
                        $evidences[] = [
                            "doc" => $props['제목'] ?? '제목 미상',
                            "quote" => mb_substr($props['설명'] ?? '', 0, 500),
                            "concept" => $node_id,
                            "text" => mb_substr($props['설명'] ?? '', 0, 1000)
                        ];
                    } elseif (in_array('사건', $labels)) {
                        $evidences[] = [
                            "doc" => $props['사건명'] ?? '사건명 미상',
                            "quote" => $props['날짜'] ?? '',
                            "concept" => $node_id,
                            "text" => "날짜: " . ($props['날짜'] ?? '') . "\n설명: " . mb_substr($props['설명'] ?? '', 0, 1000)
                        ];
                    }
                }
            }

            // Step 2: Expand graph
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
                    LIMIT 200
                ";
                $queries[] = ["title" => "지식망 확장 (2-hop)", "query" => $graph_query];

                $res2 = $client->run($graph_query, ['search_ids' => $found_ids]);
                foreach ($res2 as $rec) {
                    $n = $rec->get('n');
                    $m = $rec->get('m');
                    $r = $rec->get('r');
                    $e = $rec->get('e');
                    $p = $rec->get('p');

                    $n_nid = add_node_to_map($nodes, $n, (array)$rec->get('n_labels'));
                    
                    if ($m) {
                        $m_nid = add_node_to_map($nodes, $m, (array)$rec->get('m_labels'));
                        if ($r) {
                            $edges[] = [
                                "from" => $n_nid,
                                "to" => $m_nid,
                                "label" => format_rel_label($r->getType())
                            ];
                        }
                    }

                    if ($e && $p) {
                        $e_nid = add_node_to_map($nodes, $e, (array)$rec->get('e_labels'));
                        $p_nid = add_node_to_map($nodes, $p, (array)$rec->get('p_labels'));
                        
                        $edges[] = ["from" => $n_nid, "to" => $e_nid, "label" => "수행/참여"];
                        $edges[] = ["from" => $e_nid, "to" => $p_nid, "label" => "수행/참여"];
                    }
                }
            }

            // Deduplicate edges
            $unique_edges = [];
            foreach ($edges as $edge) {
                $key = $edge['from'] . '-' . $edge['to'] . '-' . $edge['label'];
                $unique_edges[$key] = $edge;
            }

            echo json_encode([
                "nodes" => array_values($nodes),
                "edges" => array_values($unique_edges),
                "evidences" => $evidences,
                "queries" => $queries
            ]);
            exit;
        }

        if ($action === 'pg_prefetch') {
            $names = json_decode($_POST['names'] ?? '[]', true);
            $all_rows = [];
            $pdo = get_pg();
            if ($pdo && !empty($names)) {
                $tables_meta = get_pg_tables_metadata($pdo);
                foreach ($names as $name) {
                    $rows = fetch_pg_rows_for_name($pdo, $name, $tables_meta);
                    foreach ($rows as $r) {
                        $r['node_id'] = $name;
                        $all_rows[] = $r;
                    }
                }
            }
            echo json_encode($all_rows);
            exit;
        }

        if ($action === 'node_details') {
            $name = $_POST['name'] ?? '';
            $pdo = get_pg();
            if (!$pdo) {
                echo json_encode([]);
                exit;
            }
            $tables_meta = get_pg_tables_metadata($pdo);
            $rows = fetch_pg_rows_for_name($pdo, $name, $tables_meta);
            echo json_encode($rows);
            exit;
        }

        if ($action === 'explain') {
            $term = $_POST['term'] ?? '';
            $evidences_json = $_POST['evidences'] ?? '[]';
            $pg_texts_json = $_POST['pg_texts'] ?? '[]';
            
            $evidences = json_decode($evidences_json, true);
            $pg_texts = json_decode($pg_texts_json, true);
            
            $context_str = "";
            if (!empty($evidences)) {
                foreach (array_slice($evidences, 0, 20) as $e) {
                    $context_str .= "- 문서: {$e['doc']}\n  내용: {$e['text']}...\n  (주석: " . ($e['quote'] ?? '') . ")\n";
                }
            }
            if (!empty($pg_texts)) {
                $context_str .= "\n[PG 근거]\n" . implode("\n", array_map(fn($t) => "- $t", array_slice($pg_texts, 0, 50)));
            }

            $prompt = "당신은 역사 도슨트입니다. 다음 사료와 PG 근거를 바탕으로 '$term'에 대해 균형잡힌 해설을 작성하세요.\n"
                    . "- 사실/추정/논쟁 지점을 구분해 서술합니다.\n"
                    . "- 감정적·과장 표현을 피하고 중립적 톤을 유지합니다.\n"
                    . "- 근거가 부족한 부분은 명확히 한계를 밝힙니다.\n\n"
                    . "[사료/PG 근거]\n$context_str";

            $res = call_deepseek([
                ["role" => "system", "content" => "당신은 역사 도슨트입니다. 중립적이고 간결한 한국어 해설만 작성하십시오."],
                ["role" => "user", "content" => $prompt]
            ]);
            
            echo json_encode(["text" => $res]);
            exit;
        }

        if ($action === 'export_ttl') {
            $nodes = json_decode($_POST['nodes'] ?? '[]', true);
            $edges = json_decode($_POST['edges'] ?? '[]', true);
            echo export_to_cidoc_ttl($nodes, $edges);
            exit;
        }

    } catch (Exception $e) {
        http_response_code(500);
        echo json_encode(["error" => $e->getMessage(), "trace" => $e->getTraceAsString()]);
        exit;
    }
}

// Helpers
function get_neo4j() {
    return ClientBuilder::create()
        ->withDriver('default', get_cfg('NEO4J_URI'), Authenticate::basic(get_cfg('NEO4J_USER'), get_cfg('NEO4J_PASSWORD')))
        ->build();
}

function get_pg() {
    try {
        $host = get_cfg('PG_HOST');
        if (!$host) return null;
        $dsn = "pgsql:host=".$host.";port=".get_cfg('PG_PORT', 5432).";dbname=".get_cfg('PG_DATABASE');
        return new PDO($dsn, get_cfg('PG_USER'), get_cfg('PG_PASSWORD'), [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_TIMEOUT => 2
        ]);
    } catch (Exception $e) {
        return null;
    }
}

function call_deepseek($msgs, $fmt = null) {
    $api_key = get_cfg('DEEPSEEK_API_KEY');
    if (!$api_key) throw new Exception("DEEPSEEK_API_KEY가 설정되지 않았습니다.");

    $ch = curl_init(rtrim(get_cfg('DEEPSEEK_BASE_URL', 'https://api.deepseek.com'), '/') . '/chat/completions');
    $payload = [
        "model" => get_cfg('DEEPSEEK_MODEL', 'deepseek-v4-flash'),
        "messages" => $msgs,
        "temperature" => 0
    ];
    if ($fmt) $payload["response_format"] = $fmt;
    
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => json_encode($payload),
        CURLOPT_HTTPHEADER => [
            "Authorization: Bearer " . $api_key,
            "Content-Type: application/json"
        ],
        CURLOPT_TIMEOUT => 120
    ]);
    
    $res = curl_exec($ch);
    if (curl_errno($ch)) throw new Exception(curl_error($ch));
    curl_close($ch);
    
    $data = json_decode($res, true);
    if (isset($data['error'])) throw new Exception($data['error']['message'] ?? 'DeepSeek API Error');
    
    return $data['choices'][0]['message']['content'] ?? "";
}

function add_node_to_map(&$map, $node, $labels) {
    $props = $node->getProperties();
    $raw_id = $props['uid'] ?? $props['id'] ?? $props['명칭'] ?? $props['name'] ?? $props['title'] ?? 'unknown';
    $nid = 'n' . substr(sha1((string)$raw_id), 0, 12);
    
    if (!isset($map[$nid])) {
        $color = "#999999";
        $icon = "";
        $base_name = $props['명칭'] ?? $props['제목'] ?? $props['사건명'] ?? $raw_id;
        $reading = $props['한글독음'] ?? '';
        
        $label_text = $base_name;
        if ($reading && $base_name != $reading) {
            $label_text = "{$base_name} ({$reading})";
        }

        if (in_array('문건', $labels)) {
            $color = "#F7A01F"; $icon = "📜\n";
        } elseif (in_array('인물', $labels)) {
            $color = "#1CE1D4"; $icon = "👤\n";
        } elseif (in_array('사건', $labels)) {
            $color = "#FF6B6B"; $icon = "🔥\n";
        } elseif (in_array('장소', $labels)) {
            $color = "#4ECDC4"; $icon = "📍\n";
        } elseif (in_array('기관', $labels)) {
            $color = "#95E1D3"; $icon = "🏢\n";
        }

        $map[$nid] = [
            "id" => $nid,
            "label" => $icon . mb_substr($label_text, 0, 20),
            "color" => $color,
            "raw_id" => (string)$raw_id,
            "title" => (string)$raw_id,
            "shape" => "box"
        ];
    }
    return $nid;
}

function format_rel_label($rtype) {
    $map = [
        "P14_carried_out_by" => "수행(참여)",
        "P7_took_place_at" => "발생 장소",
        "P152_has_parent" => "가족 관계",
        "foaf:knows" => "동지/지인",
        "foaf:member" => "소속 기구",
        "P11_had_participant" => "참여 인물",
        "P108_has_produced" => "생성/저작",
        "P102_has_title" => "명칭/제목",
        "소속" => "소속"
    ];
    return $map[$rtype] ?? $rtype;
}

function get_pg_tables_metadata($pdo) {
    $stmt = $pdo->query("
        SELECT table_schema, table_name
        FROM information_schema.columns
        WHERE column_name = 'tei'
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        GROUP BY table_schema, table_name
    ");
    $tables = $stmt->fetchAll(PDO::FETCH_ASSOC);
    
    $meta = [];
    foreach ($tables as $t) {
        $schema = $t['table_schema'];
        $table = $t['table_name'];
        
        // ID col
        $s1 = $pdo->prepare("SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position ASC LIMIT 1");
        $s1->execute([$schema, $table]);
        $id_col = $s1->fetchColumn() ?: 'rowid';
        
        // Text cols
        $s2 = $pdo->prepare("SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND data_type IN ('character varying','text','character') ORDER BY ordinal_position ASC");
        $s2->execute([$schema, $table]);
        $text_cols = $s2->fetchAll(PDO::FETCH_COLUMN);
        
        // Priority
        foreach (array_reverse(['명칭', '사건명', '제목']) as $p) {
            if (($key = array_search($p, $text_cols)) !== false) {
                unset($text_cols[$key]);
                array_unshift($text_cols, $p);
            }
        }
        
        $meta[] = [
            'schema' => $schema,
            'table' => $table,
            'id_col' => $id_col,
            'text_cols' => array_slice($text_cols, 0, 10)
        ];
    }
    return $meta;
}

function fetch_pg_rows_for_name($pdo, $name, $tables_meta) {
    $out = [];
    foreach ($tables_meta as $m) {
        $fq = ($m['schema'] && $m['schema'] != 'public') ? "\"{$m['schema']}\".\"{$m['table']}\"" : "\"{$m['table']}\"";
        $search_cols = array_unique(array_merge([$m['id_col'], 'tei'], $m['text_cols']));
        $search_cols = array_slice($search_cols, 0, 8);
        
        $clauses = [];
        foreach ($search_cols as $c) $clauses[] = "\"$c\"::text ILIKE ?";
        
        $q = "SELECT * FROM $fq WHERE (" . implode(" OR ", $clauses) . ") LIMIT 10";
        $stmt = $pdo->prepare($q);
        $stmt->execute(array_fill(0, count($clauses), "%$name%"));
        
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            $item = [
                'table' => $m['table'],
                'schema' => $m['schema'],
                'rowid' => $row[$m['id_col']] ?? null,
                'id_col' => $m['id_col'],
                'match_cols' => $search_cols,
                'tei' => $row['tei'] ?? null,
                'snippets' => array_diff_key($row, ['tei' => 1])
            ];
            $out[] = $item;
        }
    }
    return $out;
}

function export_to_cidoc_ttl($nodes, $edges) {
    $lines = [
        "@prefix ex: <http://example.org/docent/> .",
        "@prefix crm: <http://www.cidoc-crm.org/cidoc-crm/> .",
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
        ""
    ];
    
    $type_map = [
        "#F7A01F" => "crm:E31_Document",
        "#1CE1D4" => "crm:E21_Person",
        "#FF6B6B" => "crm:E5_Event",
        "#4ECDC4" => "crm:E53_Place",
        "#95E1D3" => "crm:E74_Group"
    ];

    foreach ($nodes as $node) {
        $raw = $node['raw_id'] ?? $node['id'];
        $frag = preg_replace('/[^0-9A-Za-z_]+/', '_', $raw);
        $color = $node['color'] ?? "#999999";
        $rdf_type = $type_map[$color] ?? "crm:E1_CRM_Entity";
        $label = trim(preg_replace('/[^\x{AC00}-\x{D7A3}x{1100}-\x{11FF}\x{3130}-\x{318F}\x{A960}-\x{A97F}\x{D7B0}-\x{D7FF}0-9a-zA-Z\s]+/u', '', $node['label']));
        
        $lines[] = "ex:{$frag} a {$rdf_type} ;";
        $lines[] = "    rdfs:label \"{$label}\" .";
        $lines[] = "";
    }

    foreach ($edges as $edge) {
        $src_node = array_filter($nodes, fn($n) => $n['id'] === $edge['from']);
        $tgt_node = array_filter($nodes, fn($n) => $n['id'] === $edge['to']);
        if (empty($src_node) || empty($tgt_node)) continue;
        
        $src = preg_replace('/[^0-9A-Za-z_]+/', '_', reset($src_node)['raw_id']);
        $tgt = preg_replace('/[^0-9A-Za-z_]+/', '_', reset($tgt_node)['raw_id']);
        $label = $edge['label'];
        
        $crm_pred = "ex:" . preg_replace('/[^0-9A-Za-z_]+/', '_', $label);
        if ($label === "수행(참여)") $crm_pred = "crm:P14_carried_out_by";
        elseif ($label === "발생 장소") $crm_pred = "crm:P7_took_place_at";

        $lines[] = "ex:{$src} {$crm_pred} ex:{$tgt} .";
    }

    return implode("\n", $lines);
}
?>
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>3.1 운동 역사 도슨트</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.0/font/bootstrap-icons.css">
    <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
    <style>
        body { background-color: #f0f2f6; font-family: 'Pretendard', sans-serif; }
        .sidebar { height: 100vh; overflow-y: auto; background: #fff; border-right: 1px solid #dee2e6; padding: 1.5rem 1rem; }
        .main-content { padding: 1.5rem; height: 100vh; overflow-y: auto; }
        #graph { height: 550px; background: #fff; border-radius: 12px; border: 1px solid #dee2e6; margin-bottom: 1rem; }
        .docent-card { background: #fff; border-radius: 12px; border: 1px solid #dee2e6; padding: 1.5rem; margin-bottom: 2rem; }
        .node-details { background: #fff; border-radius: 12px; border: 1px solid #dee2e6; padding: 1.5rem; }
        .keyword-btn { margin-bottom: 0.5rem; text-align: left; transition: all 0.2s; }
        .keyword-btn:hover { transform: translateX(5px); }
        .status-badge { font-size: 0.8rem; padding: 0.3rem 0.6rem; border-radius: 20px; }
        pre { background: #f8f9fa; padding: 1rem; border-radius: 8px; font-size: 0.85rem; border: 1px solid #eee; }
        .tei-box { max-height: 300px; overflow-y: auto; border: 1px solid #eee; padding: 10px; font-family: monospace; }
        .sticky-sidebar { position: sticky; top: 0; }
        .cypher-box { background: #fdfdfe; border-radius: 8px; border: 1px solid #e9ecef; margin-bottom: 1rem; }
        .cypher-box header { background: #f8f9fa; padding: 0.5rem 1rem; border-bottom: 1px solid #e9ecef; font-weight: bold; font-size: 0.8rem; border-radius: 8px 8px 0 0; }
        .cypher-box pre { margin: 0; border: none; border-radius: 0 0 8px 8px; background: transparent; }
    </style>
</head>
<body>
<div class="container-fluid">
    <div class="row">
        <!-- Sidebar -->
        <div class="col-md-2 sidebar">
            <div class="sticky-sidebar">
                <h5 class="mb-4">🇰🇷 역사 도슨트</h5>
                
                <div class="mb-4">
                    <label class="form-label fw-bold small">🔍 통합 검색</label>
                    <div class="input-group input-group-sm">
                        <input type="text" id="q" class="form-control" placeholder="인물, 사건 등">
                        <button onclick="performSearch()" class="btn btn-primary">탐색</button>
                    </div>
                </div>

                <div class="mb-4">
                    <label class="form-label fw-bold small">💡 추천 키워드</label>
                    <div class="d-grid gap-1">
                        <?php $exs = ["유관순", "안중근", "3.1 운동", "시위", "임시정부", "독립신문"]; ?>
                        <?php foreach ($exs as $ex): ?>
                            <button onclick="setQuery('<?= $ex ?>')" class="btn btn-outline-secondary btn-xs keyword-btn py-1" style="font-size: 0.75rem;">
                                <i class="bi bi-pin-angle"></i> <?= $ex ?>
                            </button>
                        <?php endforeach; ?>
                    </div>
                </div>

                <div id="analysis-box" class="mb-4" style="display:none">
                    <label class="form-label fw-bold small">🎯 분석 정보</label>
                    <div class="p-2 bg-light rounded border small">
                        <div class="mb-1"><span class="badge bg-info text-dark" id="intent-val"></span></div>
                        <div class="mb-1"><strong>초점:</strong> <span id="focus-val"></span></div>
                        <div class="text-muted" id="explanation-val"></div>
                    </div>
                </div>

                <div id="status-log" class="small text-muted mt-auto" style="font-size: 0.7rem;">
                    <hr>
                    <div id="status-text">대기 중...</div>
                </div>
            </div>
        </div>

        <!-- Main Content -->
        <div class="col-md-10 main-content">
            <div class="d-flex justify-content-between align-items-center mb-4">
                <h3 id="search-title">반갑습니다! 역사를 탐색해 보세요.</h3>
                <div id="actions" style="display:none">
                    <button onclick="exportTTL()" class="btn btn-outline-primary btn-sm">
                        <i class="bi bi-download"></i> TTL
                    </button>
                    <button onclick="resetGraph()" class="btn btn-outline-secondary btn-sm ms-2">
                        <i class="bi bi-arrows-move"></i> 정렬
                    </button>
                </div>
            </div>

            <div class="row">
                <!-- Left: Graph & Extraction Queries -->
                <div class="col-lg-6">
                    <div id="graph"></div>
                    
                    <!-- Extraction Queries Section -->
                    <div id="extraction-queries-section" style="display:none">
                        <div class="d-flex align-items-center mb-2">
                            <small class="fw-bold text-muted"><i class="bi bi-database-fill-gear"></i> Graph Extraction Cypher</small>
                        </div>
                        <div id="cypher-list">
                            <!-- Queries will be injected here -->
                        </div>
                    </div>
                </div>

                <!-- Right: Docent & RAG Evidence -->
                <div class="col-lg-6">
                    <div class="docent-card">
                        <div class="d-flex justify-content-between align-items-center mb-3">
                            <h5 class="m-0"><i class="bi bi-chat-left-text"></i> 도슨트 해설</h5>
                            <button id="explainBtn" onclick="generateExplanation()" class="btn btn-success btn-sm" style="display:none">
                                <i class="bi bi-stars"></i> 해설 생성
                            </button>
                        </div>
                        <div id="explanation-content" class="text-secondary">
                            검색 후 해설 생성 버튼을 눌러주세요.
                        </div>
                        <div id="pg-stats" class="mt-3 small text-muted"></div>
                        
                        <!-- RAG Context Section -->
                        <div id="rag-context-section" class="mt-4" style="display:none">
                            <button class="btn btn-link btn-sm text-decoration-none p-0 w-100 text-start text-dark fw-bold" type="button" data-bs-toggle="collapse" data-bs-target="#ragContextCollapse">
                                <i class="bi bi-journal-text"></i> 수집된 사료 근거 (RAG Context)
                            </button>
                            <div class="collapse mt-2 show" id="ragContextCollapse">
                                <div class="card card-body bg-light border-0 p-2 small">
                                    <div id="rag-evidence-list" style="max-height: 400px; overflow-y: auto;">
                                        <!-- Evidence items will be injected here -->
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>

                    <div id="node-info-card" class="node-details" style="display:none">
                        <h5 class="mb-3"><i class="bi bi-info-circle"></i> 상세 정보: <span id="selected-node-name"></span></h5>
                        <div id="pg-data-accordion" class="accordion accordion-flush">
                            <!-- PG rows will be injected here -->
                        </div>
                    </div>
                </div>
            </div>

            <!-- Debug / Log Section -->
            <div class="mt-4">
                <button class="btn btn-link btn-sm text-muted p-0" type="button" data-bs-toggle="collapse" data-bs-target="#debugCollapse">
                    <i class="bi bi-bug"></i> 시스템 로그
                </button>
                <div class="collapse mt-2" id="debugCollapse">
                    <div class="card card-body bg-dark text-white p-3" style="font-family: monospace; font-size: 0.8rem;">
                        <div id="debug-log"></div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>

<script>
let network = null;
let lastNodes = [];
let lastEdges = [];
let lastEvidences = [];
let pgPrefetchTexts = [];

async function api(act, data = {}) {
    logDebug(`API Call: ${act}`);
    const fd = new FormData();
    for (let k in data) fd.append(k, data[k]);
    const response = await fetch(`?ajax=${act}`, { method: 'POST', body: fd });
    if (!response.ok) {
        const err = await response.json();
        throw new Error(err.error || 'Unknown error');
    }
    return response.json();
}

function logDebug(msg) {
    const log = document.getElementById('debug-log');
    const time = new Date().toLocaleTimeString();
    log.innerHTML += `<div>[${time}] ${msg}</div>`;
    log.scrollTop = log.scrollHeight;
}

function setQuery(q) {
    document.getElementById('q').value = q;
    performSearch();
}

async function performSearch() {
    const term = document.getElementById('q').value.trim();
    if (term.length < 2) return alert('2글자 이상 입력해주세요.');

    setStatus("분석 중...");
    document.getElementById('search-title').innerText = `'${term}' 탐색 중...`;
    
    try {
        // 1. Analyze
        const analysis = await api('analyze', { term });
        document.getElementById('analysis-box').style.display = 'block';
        document.getElementById('intent-val').innerText = analysis.intent;
        document.getElementById('focus-val').innerText = analysis.focus;
        document.getElementById('explanation-val').innerText = analysis.explanation;
        
        const keywords = analysis.keywords || [term];
        setStatus(`키워드 [${keywords.join(', ')}] 기반 그래프 검색...`);

        // 2. Graph
        const graphData = await api('graph', { keywords: JSON.stringify(keywords), term: term });
        lastNodes = graphData.nodes;
        lastEdges = graphData.edges;
        lastEvidences = graphData.evidences;

        draw(lastNodes, lastEdges);
        
        // Display Cypher Queries
        if (graphData.queries) {
            const list = document.getElementById('cypher-list');
            list.innerHTML = graphData.queries.map(q => `
                <div class="cypher-box">
                    <header>${q.title}</header>
                    <pre><code class="language-cypher">${escapeHtml(q.query.trim())}</code></pre>
                </div>
            `).join('');
            document.getElementById('extraction-queries-section').style.display = 'block';
        }

        document.getElementById('actions').style.display = 'block';
        document.getElementById('explainBtn').style.display = 'inline-block';
        document.getElementById('search-title').innerText = `'${term}' 지식망 탐색 완료`;

        // 3. PG Prefetch
        const nodeNames = lastNodes.map(n => n.raw_id);
        if (nodeNames.length > 0) {
            setStatus("PostgreSQL 근거 수집 중...");
            try {
                const pgRows = await api('pg_prefetch', { names: JSON.stringify(nodeNames) });
                pgPrefetchTexts = pgRows.map(r => {
                    const snippet = Object.entries(r.snippets).map(([k, v]) => `${k}: ${v}`).join('; ');
                    return `[${r.table}] node=${r.node_id} rowid=${r.rowid} :: ${snippet}`;
                });
                document.getElementById('pg-stats').innerText = `PostgreSQL 근거 ${pgRows.length}건 수집됨`;
                
                // Populate RAG Evidence List
                const ragList = document.getElementById('rag-evidence-list');
                let ragHtml = '<h6><i class="bi bi-graph-up"></i> Neo4j Graph Evidences</h6>';
                if (lastEvidences.length > 0) {
                    lastEvidences.forEach(ev => {
                        ragHtml += `<div class="mb-2 p-2 bg-white border-start border-warning border-4 rounded shadow-sm">
                            <div class="fw-bold text-primary">${ev.doc} <small class="text-muted">(ID: ${ev.concept})</small></div>
                            <div class="mt-1">${ev.text}</div>
                            ${ev.quote ? `<div class="mt-1 small text-muted font-italic">"${ev.quote}"</div>` : ''}
                        </div>`;
                    });
                } else {
                    ragHtml += '<div class="text-muted mb-2">No graph evidence found.</div>';
                }
                
                ragHtml += '<h6 class="mt-3"><i class="bi bi-database"></i> PostgreSQL Document Evidences</h6>';
                if (pgRows.length > 0) {
                    pgRows.forEach(r => {
                        const snippet = Object.entries(r.snippets).map(([k, v]) => `<strong>${k}:</strong> ${v}`).join('<br>');
                        ragHtml += `<div class="mb-2 p-2 bg-white border-start border-info border-4 rounded shadow-sm">
                            <div class="fw-bold text-success">[${r.table}] <small>rowid: ${r.rowid}</small></div>
                            <div class="mt-1 small">${snippet}</div>
                            ${r.tei ? `<div class="mt-2 p-1 bg-light border rounded" style="font-size: 0.7rem; font-family: monospace;">${escapeHtml(r.tei.substring(0, 300))}...</div>` : ''}
                        </div>`;
                    });
                } else {
                    ragHtml += '<div class="text-muted">No document evidence found.</div>';
                }
                ragList.innerHTML = ragHtml;
                document.getElementById('rag-context-section').style.display = 'block';
            } catch (pgErr) {
                logDebug(`PG Prefetch failed (likely DB down): ${pgErr.message}`);
                document.getElementById('pg-stats').innerText = "PostgreSQL 근거 수집 건너뜀 (DB 연결 불가)";
            }
        }

        setStatus("완료");
    } catch (e) {
        console.error(e);
        setStatus(`에러: ${e.message}`);
        logDebug(`Critical Error: ${e.message}`);
        // Only alert for non-database related critical failures if necessary, 
        // but the user wants to avoid unnecessary popups.
    }
}

function setStatus(txt) {
    document.getElementById('status-text').innerText = txt;
    logDebug(`Status: ${txt}`);
}

function draw(nodes, edges) {
    const container = document.getElementById('graph');
    const data = { nodes: new vis.DataSet(nodes), edges: new vis.DataSet(edges) };
    const options = {
        nodes: { 
            font: { size: 14, multi: true },
            borderWidth: 2,
            shadow: true
        },
        edges: { 
            arrows: { to: { enabled: true, scaleFactor: 0.5 } },
            color: '#848484',
            font: { align: 'middle', size: 12 }
        },
        physics: {
            enabled: true,
            solver: 'repulsion',
            repulsion: { nodeDistance: 200, centralGravity: 0.2 },
            stabilization: { iterations: 100 }
        }
    };
    network = new vis.Network(container, data, options);
    
    network.on("click", params => {
        if (params.nodes.length) {
            const nodeId = params.nodes[0];
            const node = nodes.find(n => n.id === nodeId);
            if (node) showNodeDetails(node.raw_id);
        }
    });
}

async function showNodeDetails(name) {
    const card = document.getElementById('node-info-card');
    const nameSpan = document.getElementById('selected-node-name');
    const accordion = document.getElementById('pg-data-accordion');
    
    card.style.display = 'block';
    nameSpan.innerText = name;
    accordion.innerHTML = '<div class="text-center p-3"><div class="spinner-border spinner-border-sm"></div> 조회 중...</div>';
    
    try {
        const rows = await api('node_details', { name });
        if (rows.length === 0) {
            accordion.innerHTML = '<div class="p-3 text-muted">관련 데이터를 찾을 수 없거나 DB가 중단되었습니다.</div>';
            return;
        }

        let html = '';
        rows.forEach((r, i) => {
            const tableInfo = r.schema ? `${r.schema}.${r.table}` : r.table;
            html += `
                <div class="accordion-item">
                    <h2 class="accordion-header">
                        <button class="accordion-button collapsed py-2" type="button" data-bs-toggle="collapse" data-bs-target="#row-${i}">
                            <small class="text-primary me-2">[${tableInfo}]</small> ID: ${r.rowid}
                        </button>
                    </h2>
                    <div id="row-${i}" class="accordion-collapse collapse" data-bs-parent="#pg-data-accordion">
                        <div class="accordion-body p-2">
                            <table class="table table-sm table-borderless mb-2" style="font-size: 0.85rem;">
                                ${Object.entries(r.snippets).map(([k, v]) => `<tr><th width="30%">${k}</th><td>${v}</td></tr>`).join('')}
                            </table>
                            ${r.tei ? `<div class="mt-2"><small class="fw-bold">TEI Extract:</small><div class="tei-box bg-light small">${escapeHtml(r.tei)}</div></div>` : ''}
                        </div>
                    </div>
                </div>`;
        });
        accordion.innerHTML = html;
        card.scrollIntoView({ behavior: 'smooth' });
    } catch (e) {
        accordion.innerHTML = `<div class="p-3 text-muted">데이터 조회 불가 (PostgreSQL 연결 확인 필요)</div>`;
        logDebug(`Node details failed: ${e.message}`);
    }
}

async function generateExplanation() {
    const term = document.getElementById('q').value;
    const contentBox = document.getElementById('explanation-content');
    contentBox.innerHTML = '<div class="text-center p-4"><div class="spinner-grow text-success"></div><br><small>역사적 근거를 바탕으로 해설을 작성 중입니다...</small></div>';
    
    try {
        const res = await api('explain', {
            term: term,
            evidences: JSON.stringify(lastEvidences),
            pg_texts: JSON.stringify(pgPrefetchTexts)
        });
        // Use marked.js for Markdown rendering
        contentBox.innerHTML = `<div class="markdown-body p-2" style="line-height: 1.6;">${marked.parse(res.text)}</div>`;
    } catch (e) {
        contentBox.innerHTML = `<div class="alert alert-danger">해설 생성 실패: ${e.message}</div>`;
    }
}

async function exportTTL() {
    try {
        const ttl = await api('export_ttl', {
            nodes: JSON.stringify(lastNodes),
            edges: JSON.stringify(lastEdges)
        });
        const blob = new Blob([ttl], { type: 'text/turtle' });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `history_docent_${new Date().getTime()}.ttl`;
        a.click();
    } catch (e) {
        alert('TTL 내보내기 실패: ' + e.message);
    }
}

function resetGraph() {
    if (network) network.stabilize();
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
</script>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
