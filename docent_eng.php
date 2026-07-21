<?php
if (!defined('DOCENT_LANG')) {
    define('DOCENT_LANG', 'en');
}

ob_start(function ($buffer) {
    $replacements = [
        '3.1 운동 역사 도슨트' => '3.1 Movement History Docent',
        '통합 검색' => 'Unified Search',
        '인물, 사건, 장소...' => 'Person, event, place...',
        '탐색' => 'Search',
        '추천 키워드' => 'Suggested Keywords',
        'AI 질의 분석' => 'AI Query Analysis',
        '의도:' => 'Intent:',
        '초점:' => 'Focus:',
        '준비됨.' => 'Ready.',
        '역사를 탐색해 보세요.' => 'Explore history.',
        '사료' => 'Source',
        '인물' => 'Person',
        '사건' => 'Event',
        '장소' => 'Place',
        '기관' => 'Organization',
        '선택된 노드 상세 정보' => 'Selected Node Details',
        '도슨트 해설' => 'Docent Explanation',
        '해설 생성' => 'Generate Explanation',
        '검색어를 입력하고 탐색 버튼을 누르면 인프라가 작동합니다.' => 'Enter a query and click Search to start the workflow.',
        '수집된 사료/PG 근거' => 'Collected Source/PG Evidence',
        '의도 분석 중...' => 'Analyzing intent...',
        '지식망 및 사료 검색...' => 'Searching knowledge graph and sources...',
        'PostgreSQL 사료 원문 연동 중...' => 'Linking PostgreSQL source text...',
        '지식망 탐색 완료' => 'Knowledge graph exploration complete',
        "지식 구조 및 근거 수집 완료. <strong>'해설 생성'</strong> 버튼을 클릭하면 RAG 분석이 시작됩니다." => "Knowledge structure and evidence collected. Click <strong>'Generate Explanation'</strong> to start RAG analysis.",
        '완료' => 'Done',
        '에러: ' => 'Error: ',
        '데이터 탐색 과정에서 실패했습니다. 에러 내용을 확인해 주세요.' => 'The search workflow failed. Check the error message.',
        '해설 생성중...' => 'Generating explanation...',
        '해설 생성 중 오류: ' => 'Error generating explanation: ',
        '개체 ID' => 'Entity ID',
        '속성' => 'Attributes',
        'Neo4j 관련 사료 기록' => 'Related Neo4j Source Records',
        'PostgreSQL 관련 근거' => 'Related PostgreSQL Evidence',
        '이 개체와 직접 연결된 사료나 추가 데이터가 없습니다.' => 'No directly linked sources or additional data for this entity.',
        '문서:' => 'Document:',
        '내용:' => 'Content:',
        '개체:' => 'Entity:',
        'PostgreSQL 사료' => 'PostgreSQL source',
        '개체: ' => 'Entity: ',
        '사료/PG 근거' => 'Sources/PG evidence',
        '수행/참여' => 'Performed/Participated',
        '한국어 해설만 작성하십시오.' => 'Write the explanation in English only.',
        '검색어 기반 분석 수행' => 'Keyword-based analysis completed',
        '분석 완료' => 'Analysis complete',
        'API Key가 설정되지 않았습니다.' => 'API key is not configured.',
        '해설을 생성할 수 없습니다.' => 'Unable to generate an explanation.',
        '한국어' => 'Korean',
        '3.1 운동 역사 도슨트' => '3.1 Movement History Docent',
        '사료 및 PG 근거' => 'Source and PG Evidence',
        '사료 기록' => 'Source Records',
        '수행(참여)' => 'Performed/Participated',
        '발생 장소' => 'Location',
        '가족 관계' => 'Family relation',
        '동지/지인' => 'Comrade/Acquaintance',
        '소속 기구' => 'Affiliated organization',
        '참여 인물' => 'Participant',
        '생성/저작' => 'Created/Produced',
        '명칭/제목' => 'Title',
        '소속' => 'Affiliation'
    ];

    return str_replace(array_keys($replacements), array_values($replacements), $buffer);
});

require_once __DIR__ . '/docent.php';
