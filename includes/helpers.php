<?php
/**
 * includes/helpers.php — 노드맵, 엣지, evidence, merge, prefetch 유틸리티
 */

function clamp_int($value, $min, $max) {
    return max($min, min($max, (int)$value));
}

function normalize_whitespace_text($text) {
    $text = preg_replace('/\s+/u', ' ', (string)$text);
    return trim($text ?? '');
}

// ── 노드 → vis.js 맵 추가 ──
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
        foreach ($labels_iterable as $l) $labels_list[] = (string)$l;
    }

    $raw_id = $props['uid'] ?? $props['id'] ?? $props['명칭'] ?? $props['name'] ?? $props['title'] ?? 'unknown';
    $nid = 'n' . substr(sha1((string)$raw_id), 0, 12);

    if (!isset($map[$nid])) {
        $reading = $props['한글독음'] ?? '';
        $base = $props['제목'] ?? $props['명칭'] ?? $props['사건명'] ?? $raw_id;

        // 유령 개체 필터링
        static $ghost_stopwords = ['대표', '달하', '그러', '총독', '순사', '공판', '십자표', '펼치는데', '연행하', '거행하고'];
        if (in_array((string)$base, $ghost_stopwords, true) && empty($props['한글독음']) && empty($props['uid']) && empty($props['id'])) {
            return null;
        }

        $label_text = ($reading && $base != $reading) ? "{$base} ({$reading})" : $base;

        // 노드 스타일 결정
        $color = "#999999"; $icon = "";
        $lstr = implode(' ', $labels_list);
        if (strpos($lstr, '문건') !== false || strpos($lstr, '사료') !== false) { $color = "#F7A01F"; $icon = "📜\n"; }
        elseif (strpos($lstr, '인물') !== false) { $color = "#2563EB"; $icon = "👤\n"; }
        elseif (strpos($lstr, '사건') !== false) { $color = "#DC2626"; $icon = "🔥\n"; }
        elseif (strpos($lstr, '장소') !== false) { $color = "#16A34A"; $icon = "📍\n"; }
        elseif (strpos($lstr, '기관') !== false) { $color = "#7C3AED"; $icon = "🏢\n"; }

        // 타입 추론
        $type = (string)($props['type'] ?? '');
        if ($type === '') {
            $type_map = [
                '인물' => '인물', 'Person' => '인물', '장소' => '장소', 'Place' => '장소',
                '사건' => '사건', 'Event' => '사건', '기관' => '기관', 'Organization' => '기관',
                '사료' => '사료', '문건' => '사료', 'Document' => '사료',
            ];
            foreach ($labels_list as $label_name) {
                if (isset($type_map[$label_name])) { $type = $type_map[$label_name]; break; }
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

// ── 관계 라벨 ──
function _get_rel_label($rtype) {
    $ko = [
        "P14_carried_out_by" => "수행(참여)", "P7_took_place_at" => "발생 장소",
        "ACTIVATED_AT" => "활동지", "P152_has_parent" => "가족 관계",
        "foaf:knows" => "동지/지인", "foaf:member" => "소속 기구",
        "P11_had_participant" => "참여 인물", "P108_has_produced" => "생성/저작",
        "P102_has_title" => "명칭/제목", "소속" => "소속",
        "동일인물" => "동일인물", "sameAs" => "동일인물", "owl:sameAs" => "동일인물"
    ];
    $en = [
        "P14_carried_out_by" => "Performed/Participated", "P7_took_place_at" => "Location",
        "ACTIVATED_AT" => "Activity place", "P152_has_parent" => "Family relation",
        "foaf:knows" => "Comrade/Acquaintance", "foaf:member" => "Affiliated organization",
        "P11_had_participant" => "Participant", "P108_has_produced" => "Created/Produced",
        "P102_has_title" => "Title", "소속" => "Affiliation",
        "동일인물" => "Same Person", "sameAs" => "Same As", "owl:sameAs" => "Same As"
    ];
    return docent_is_english() ? ($en[$rtype] ?? $rtype) : ($ko[$rtype] ?? $rtype);
}

// ── Evidence 중복 제거 추가 ──
function add_fact_evidence(&$evidences, $prefix, $hash_parts, $payload, $max_count = 30) {
    if (!is_array($evidences)) $evidences = [];
    if (count($evidences) >= $max_count) return false;
    $key = "{$prefix}-" . sha1(implode('|', array_map(fn($v) => (string)$v, $hash_parts)));
    if (isset($evidences[$key])) return false;
    $evidences[$key] = $payload;
    return true;
}

// ── 동일인물 노드 병합 ──
function merge_same_person_nodes(&$nodes, &$edges, &$evidences) {
    if (empty($nodes)) return;

    $merge_target = [];

    // 1. 엣지 기반 동일인물 감지
    foreach ($edges as $e) {
        $type = $e['type'] ?? '';
        if (in_array($type, ['동일인물', 'sameAs', 'owl:sameAs'], true)) {
            $from = $e['from']; $to = $e['to'];
            if (isset($nodes[$from], $nodes[$to]) && $from !== $to) {
                $raw1 = (string)($nodes[$from]['raw_id'] ?? '');
                $raw2 = (string)($nodes[$to]['raw_id'] ?? '');
                $is_hangul1 = (bool)preg_match('/[\x{AC00}-\x{D7A3}]/u', $raw1);
                $is_hangul2 = (bool)preg_match('/[\x{AC00}-\x{D7A3}]/u', $raw2);
                $is_hanja1 = (bool)preg_match('/[\x{4E00}-\x{9FFF}]/u', $raw1);
                $is_hanja2 = (bool)preg_match('/[\x{4E00}-\x{9FFF}]/u', $raw2);

                if ($is_hangul1 && !$is_hangul2 && $is_hanja2) { $canon = $from; $alias = $to; }
                elseif ($is_hangul2 && !$is_hangul1 && $is_hanja1) { $canon = $to; $alias = $from; }
                else { $canon = $to; $alias = $from; }
                $merge_target[$alias] = $canon;
            }
        }
    }

    // 2. 한글독음/한자 기반 동일인물 감지
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
            $nid1 = $person_nids[$i]; $nid2 = $person_nids[$j];
            if (!isset($nodes[$nid1], $nodes[$nid2])) continue;
            $raw1 = trim((string)($nodes[$nid1]['raw_id'] ?? ''));
            $raw2 = trim((string)($nodes[$nid2]['raw_id'] ?? ''));
            $props1 = $nodes[$nid1]['props'] ?? []; $props2 = $nodes[$nid2]['props'] ?? [];
            $reading1 = trim((string)($props1['한글독음'] ?? $props1['한글명칭'] ?? ''));
            $reading2 = trim((string)($props2['한글독음'] ?? $props2['한글명칭'] ?? ''));
            $hanja1 = trim((string)($props1['한자'] ?? '')); $hanja2 = trim((string)($props2['한자'] ?? ''));

            $is_same = false; $canon = null; $alias = null;
            if ($reading1 !== '' && $reading1 === $raw2) { $is_same = true; $canon = $nid2; $alias = $nid1; }
            elseif ($reading2 !== '' && $reading2 === $raw1) { $is_same = true; $canon = $nid1; $alias = $nid2; }
            elseif ($hanja1 !== '' && $hanja1 === $raw2) { $is_same = true; $canon = $nid1; $alias = $nid2; }
            elseif ($hanja2 !== '' && $hanja2 === $raw1) { $is_same = true; $canon = $nid2; $alias = $nid1; }

            if ($is_same && $canon && $alias) $merge_target[$alias] = $canon;
        }
    }

    if (empty($merge_target)) return;

    // Transitive resolution
    foreach ($merge_target as $src => $dst) {
        $visited = [$src => true]; $curr = $dst;
        while (isset($merge_target[$curr]) && !isset($visited[$curr])) {
            $visited[$curr] = true; $curr = $merge_target[$curr];
        }
        $merge_target[$src] = $curr;
    }

    // Merge nodes
    foreach ($merge_target as $alias_nid => $canon_nid) {
        if (!isset($nodes[$alias_nid], $nodes[$canon_nid]) || $alias_nid === $canon_nid) continue;
        $alias_node = $nodes[$alias_nid]; $canon_node = &$nodes[$canon_nid];
        $alias_raw = (string)($alias_node['raw_id'] ?? '');
        $canon_raw = (string)($canon_node['raw_id'] ?? '');

        if (!isset($canon_node['aliases']) || !is_array($canon_node['aliases'])) $canon_node['aliases'] = [];
        if ($alias_raw !== '' && $alias_raw !== $canon_raw) $canon_node['aliases'][] = $alias_raw;
        if (!empty($alias_node['aliases'])) $canon_node['aliases'] = array_merge($canon_node['aliases'], $alias_node['aliases']);
        $canon_node['aliases'] = array_values(array_unique($canon_node['aliases']));

        // 라벨 포맷
        $icon = "👤\n";
        $hanja_sub = '';
        foreach ($canon_node['aliases'] as $al) {
            if (preg_match('/[\x{4E00}-\x{9FFF}]/u', $al)) { $hanja_sub = $al; break; }
        }
        if ($hanja_sub !== '') {
            $canon_node['label'] = $icon . mb_substr("{$canon_raw} ({$hanja_sub})", 0, 25);
        } elseif (!empty($canon_node['aliases'])) {
            $canon_node['label'] = $icon . mb_substr("{$canon_raw} ({$canon_node['aliases'][0]})", 0, 25);
        }

        if (!empty($alias_node['labels'])) $canon_node['labels'] = array_values(array_unique(array_merge($canon_node['labels'] ?? [], $alias_node['labels'])));
        if (isset($alias_node['props'])) $canon_node['props'] = array_merge($alias_node['props'], $canon_node['props'] ?? []);
        unset($nodes[$alias_nid]);
    }

    // Redirect edges
    $new_edges = [];
    foreach ($edges as $e) {
        $from = $merge_target[$e['from']] ?? $e['from'];
        $to = $merge_target[$e['to']] ?? $e['to'];
        if ($from === $to) continue;
        if (in_array($e['type'] ?? '', ['동일인물', 'sameAs', 'owl:sameAs'], true)) continue;
        $e['from'] = $from; $e['to'] = $to;
        $new_edges[] = $e;
    }
    $edges = $new_edges;
}

// ── Prefetch 대상 선정 ──
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
    foreach ($node_list as $n) $degree[$n['id']] = 0;
    foreach ($edges as $e) {
        if (isset($degree[$e['from']])) $degree[$e['from']]++;
        if (isset($degree[$e['to']])) $degree[$e['to']]++;
    }

    usort($node_list, function ($a, $b) use ($degree) {
        $da = $degree[$a['id']] ?? 0; $db = $degree[$b['id']] ?? 0;
        return $da === $db ? strcmp((string)($a['raw_id'] ?? $a['id']), (string)($b['raw_id'] ?? $b['id'])) : $db <=> $da;
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

// ── Evidence Context 구축 (예산 제한) ──
function build_budgeted_evidence_context($evidences, $lang, $max_items, $min_chars, $max_chars, $total_char_budget, $term = '', $focus = '') {
    if (is_bool($lang)) $lang = $lang ? 'en' : 'ko';
    $lines = []; $used_chars = 0;

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

        // 지명/사건 추출
        $region = ''; $event = '';
        if (preg_match('/(평안[남북]도|함경[남북]도|충청[남북]도|전라[남북]도|경상[남북]도|강원도|경기도|황해도|평양|함흥|천안|병천|단천|간도|연해주|상해|진천|청주|용산|종로|보신각|대구|부산|통영|의주|해주|원산)[^\s,]*/u', $text . ' ' . $doc, $rm)) $region = $rm[0];
        if (preg_match('/([가-힣\w]+(?:시위|만세|선언식|의거|봉기|집회|행진|사건|파견|회합))/u', $doc . ' ' . $text, $em)) $event = $em[0];

        // 초점 필터링
        $is_spatial_query = preg_match('/(장소|지역|확산|전개|시위|만세)/u', $term . ' ' . $focus);
        $is_diplomatic = preg_match('/(파리강화회의|외교청원|청원서\s*서명|강화회의\s*파견)/u', $text . ' ' . $doc);

        if ($is_spatial_query && $is_diplomatic) {
            $body = "[외교 및 기타 활동 약력 기록: {$event} 관련 명단 수록 (장소적 시위 확산과 직접 무관)]";
        } else {
            $remaining = $total_char_budget - $used_chars;
            $body = mb_substr($text, 0, max($min_chars, min($max_chars, $remaining - 120)));
        }

        $labels = _get_envelope_labels($lang);
        $comment_str = _build_scope_comment($lang, $entity_attr, $region, $event);
        $envelope = "<SOURCE_EVIDENCE id=\"{$id_attr}\" entity=\"{$entity_attr}\"" . ($region ? " region=\"{$region}\"" : "") . ($event ? " event=\"{$event}\"" : "") . ">\n"
                  . "  {$comment_str}\n  [{$labels['doc']}] {$doc}\n  [{$labels['body']}] {$body}\n</SOURCE_EVIDENCE>";

        $line_len = mb_strlen($envelope);
        if ($line_len > ($total_char_budget - $used_chars)) break;
        $lines[] = $envelope;
        $used_chars += $line_len;
    }
    return implode("\n\n", $lines);
}

function build_budgeted_pg_context($pg_texts, $lang, $max_items, $min_chars, $max_chars, $total_char_budget, $term = '', $focus = '') {
    if (is_bool($lang)) $lang = $lang ? 'en' : 'ko';
    $lines = []; $used_chars = 0;

    foreach ((array)$pg_texts as $idx => $t) {
        if (count($lines) >= $max_items) break;
        if (($total_char_budget - $used_chars) < ($min_chars + 120)) break;

        $text = normalize_whitespace_text($t);
        if ($text === '') continue;

        $node_id = ''; $rowid = '';
        if (preg_match('/node=([^\s]+)/u', $text, $nm)) $node_id = trim($nm[1]);
        if (preg_match('/rowid=([^\s]+)/u', $text, $rm)) $rowid = trim($rm[1]);
        $id_attr = $rowid ? "pg_{$rowid}" : "pg_" . ($idx + 1);
        $entity_attr = htmlspecialchars($node_id ?: "기록_{$idx}", ENT_QUOTES, 'UTF-8');

        $region = ''; $event = '';
        if (preg_match('/(지역|주소|본적|발생지):\s*([^;]+)/u', $text, $reg_m)) $region = trim($reg_m[2]);
        elseif (preg_match('/(평안[남북]도|함경[남북]도|충청[남북]도|전라[남북]도|경상[남북]도|강원도|경기도|황해도|평양|함흥|천안|병천|단천|간도|연해주|상해|진천|청주|용산|종로|보신각|대구|부산|통영|의주|해주|원산)[^\s,;]*/u', $text, $rm2)) $region = $rm2[0];
        if (preg_match('/(사건명|제목):\s*([^;]+)/u', $text, $ev_m)) $event = trim($ev_m[2]);
        elseif (preg_match('/([가-힣\w]+(?:시위|만세|선언식|의거|봉기|집회|행진|사건|파견))/u', $text, $em2)) $event = $em2[0];

        $is_spatial_query = preg_match('/(장소|지역|확산|전개|시위|만세)/u', $term . ' ' . $focus);
        $is_diplomatic = preg_match('/(파리강화회의|외교청원|청원서\s*서명|강화회의\s*파견)/u', $text);

        if ($is_spatial_query && $is_diplomatic) {
            $body = "[외교 및 기타 활동 약력 기록: {$event} 관련 명단 수록 (장소적 시위 확산과 직접 무관)]";
        } else {
            $remaining = $total_char_budget - $used_chars;
            $body = mb_substr($text, 0, max($min_chars, min($max_chars, $remaining - 120)));
        }

        $comment_str = _build_scope_comment($lang, $entity_attr, $region, $event);
        $envelope = "<SOURCE_EVIDENCE id=\"{$id_attr}\" entity=\"{$entity_attr}\"" . ($region ? " region=\"{$region}\"" : "") . ($event ? " event=\"{$event}\"" : "") . ">\n"
                  . "  {$comment_str}\n  {$body}\n</SOURCE_EVIDENCE>";

        $line_len = mb_strlen($envelope);
        if ($line_len > ($total_char_budget - $used_chars)) break;
        $lines[] = $envelope;
        $used_chars += $line_len;
    }

    if (empty($lines)) return '';
    $headers = ['ja' => '[一次史料原文（PostgreSQL所蔵公文書）]', 'zh' => '[一手史料原文（PostgreSQL历史档案）]', 'en' => '[Primary Archival Sources (PostgreSQL)]'];
    $header = ($headers[$lang] ?? '[1차 사료 원문 (PostgreSQL)]') . "\n";
    return $header . implode("\n\n", $lines);
}

// ── 내부 헬퍼 ──
function _get_envelope_labels($lang) {
    $map = [
        'ja' => ['doc' => '史料', 'body' => '記述'],
        'zh' => ['doc' => '文献', 'body' => '内容'],
        'en' => ['doc' => 'Document', 'body' => 'Content'],
    ];
    return $map[$lang] ?? ['doc' => '문서', 'body' => '내용'];
}

function _build_scope_comment($lang, $entity, $region, $event) {
    $scope = "[{$entity}" . ($region ? " / {$region}" : "") . ($event ? " / {$event}" : "") . "]";
    $templates = [
        'ja' => "<!-- この史料の内容は {$scope} の記述にのみ有効であり他地域・事件との混同厳禁 -->",
        'zh' => "<!-- 本史料内容仅对 {$scope} 的记述有效，严禁跨区域混淆 -->",
        'en' => "<!-- This archival evidence applies strictly to {$scope} and must NOT be attributed to other regions/events -->",
    ];
    return $templates[$lang] ?? "<!-- 이 사료의 내용은 오직 {$scope} 서술에만 유효하며 타 지역/사건과 결합 금지 -->";
}
