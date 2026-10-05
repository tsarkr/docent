# Docent

Docent is a research data and retrieval-augmented generation (RAG) repository
for connecting March 1st Movement historical sources with PostgreSQL, TEI/XML,
CIDOC-CRM, and Neo4j.

The repository has four layers:

1. **Preprocessing** loads CSV/XLSX source files into PostgreSQL `raw_*` tables
   and creates and refines row-level TEI.
2. **Ontology processing** converts TEI entities into CIDOC-CRM activities and
   actors, then loads the resulting nodes and relationships into Neo4j.
3. **Serving** uses [docent.php](./docent.php) to search Neo4j and PostgreSQL
   and generate evidence-grounded multilingual explanations. The
   [mcp_server.py](./mcp_server.py) process exposes read-only database tools to
   IDE and desktop MCP clients.
4. **Evaluation** compares Vector RAG, Standard GraphRAG, and Docent Hybrid RAG
   and produces RAGAS and retrieval-diagnostic metrics.

There is no `app.py` or Streamlit application in the current repository. The
web entry points are [index.html](./index.html) and
[docent.php](./docent.php).

## Architecture

```text
data/*.csv, data/*.xlsx
        │
        ▼
upload_data.py
        │  PostgreSQL raw_* tables
        ▼
scripts/pg_to_pg_with_tei.py
        │  rowid + TEI/XML
        ▼
scripts/tag_tei_with_dict.py
        │  dictionary/LLM-based TEI refinement
        ▼
scripts/link_persnames_i815.py
scripts/extract_all_entities.py
        │
        ├── Extracted_Historical_Entities.csv/.xlsx
        └── scripts/generate_cidoc_mappings.py
                    │
                    ├── tei_cidoc_mappings
                    └── CIDOC_Timeline_Mappings.ttl
                                │
                                ▼
                    scripts/graph_builder.py
                                │
                                ▼
                    Neo4j graph
                       │       │
          vectorize_neo4j.py    │
          apply_vectors_to_neo4j.py
                       │       │
                       ▼       ▼
                   vector_store/  PHP Docent / MCP
```

## Requirements

- Python 3.11 or newer
- PostgreSQL
- Neo4j 5.x
- Ollama
  - `qwen3.6` for TEI person-name assistance
  - `nomic-embed-text` for embeddings
  - `gemma4:26b` for the local RAGAS and direct context-precision judges
- PHP and Composer
  - `laudis/neo4j-php-client`
  - `vlucas/phpdotenv`
- Internet access for the first i815 person-linking run

Python dependencies are listed in [requirements.txt](./requirements.txt).
PHP dependencies are defined in [composer.json](./composer.json).

## Installation

```bash
python3 -m venv .venv3.14
.venv3.14/bin/python -m pip install -r requirements.txt
composer install
```

The pipeline searches for Python in this order:
`.venv3.14/bin/python`, `.venv/bin/python3`, and `.venv/bin/python`.
Replace the executable path in the examples if you use another environment.

Prepare the Ollama models separately:

```bash
ollama pull qwen3.6
ollama pull nomic-embed-text
ollama pull gemma4:26b       # required for local RAGAS/Judge runs
```

## Configuration

Python code gives precedence to environment variables and then reads
`.streamlit/secrets.toml` through [scripts/config.py](./scripts/config.py).
The PHP layer reads environment variables, `.env`, and
`.streamlit/secrets.toml`.

Never commit passwords or API keys.

```toml
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "historical"
PG_USER = "postgres"
PG_PASSWORD = "change-me"

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "change-me"

GEMINI_API_KEY = "..."
GEMINI_MODEL = "gemini-flash-lite-latest"
```

Important optional settings:

| Variable | Default | Purpose |
|---|---|---|
| `TEI_TABLE_PREFIX` | `raw_` | Prefix for TEI source tables |
| `TEI_TABLES` | all `raw_*` tables | Comma-separated TEI table selection |
| `TEI_COMMIT_BATCH_SIZE` | `50` | TEI database commit batch size |
| `USE_PROCESS_POOL` | `1` | Enable the TEI tagging process pool |
| `MAX_WORKERS` | CPU-dependent | Number of TEI workers |
| `CHUNK_SIZE` | `200` | TEI tagging batch size |
| `SKIP_HITL_PAUSE` | unset | Skip the human-review pause when set to `1`, `true`, `yes`, or `on` |
| `PIPELINE_STAGE` | `all` | Select `all`, `rdbms`, or `neo4j` |
| `OLLAMA_HOST` | Ollama default | Ollama endpoint for embeddings and thesaurus processing |
| `OLLAMA_EMBED_MODEL` | `nomic-embed-text` | Neo4j embedding model |
| `GEMINI_TEMPERATURE` | `0.1` | PHP Gemini temperature |
| `GEMINI_TOP_P` | `0.85` | PHP Gemini top-p |
| `RAGAS_PROVIDER` | `ollama` | Select `ollama` or `gemini` |

## Full data pipeline

Check the plan before running a database-changing pipeline:

```bash
.venv3.14/bin/python run_pipeline.py --dry-run
```

Run all stages:

```bash
.venv3.14/bin/python run_pipeline.py
```

[run_pipeline.py](./run_pipeline.py) executes these stages:

1. [upload_data.py](./upload_data.py) loads CSV/XLSX files from `data/` into
   PostgreSQL `raw_*` tables and writes column mappings.
2. [scripts/pg_to_pg_with_tei.py](./scripts/pg_to_pg_with_tei.py) creates
   `rowid` and TEI columns for source rows.
3. [scripts/tag_tei_with_dict.py](./scripts/tag_tei_with_dict.py) refines
   Chinese characters, names, places, and dates and adds TEI tags.
4. [scripts/link_persnames_i815.py](./scripts/link_persnames_i815.py) links
   person candidates with exactly one i815 match.
5. [scripts/extract_all_entities.py](./scripts/extract_all_entities.py)
   extracts TEI entities into `Extracted_Historical_Entities.csv`.
6. The default run pauses for a human review of the generated XLSX file.
7. [scripts/generate_cidoc_mappings.py](./scripts/generate_cidoc_mappings.py)
   creates `tei_cidoc_mappings` and
   [CIDOC_Timeline_Mappings.ttl](./CIDOC_Timeline_Mappings.ttl).
8. [scripts/graph_builder.py](./scripts/graph_builder.py) builds the Neo4j
   nodes and relationships.
9. [thesaurus_to_neo4j.py](./thesaurus_to_neo4j.py) loads the thesaurus.
10. [scripts/vectorize_neo4j.py](./scripts/vectorize_neo4j.py) creates node
    embeddings in [vector_store/](./vector_store/).
11. [scripts/apply_vectors_to_neo4j.py](./scripts/apply_vectors_to_neo4j.py)
    writes embeddings to Neo4j node properties.

Skip the human-review pause:

```bash
.venv3.14/bin/python run_pipeline.py --skip-hitl
```

Run only one pipeline stage:

```bash
# PostgreSQL/TEI/CIDOC preprocessing
.venv3.14/bin/python run_pipeline.py --stage rdbms --skip-hitl

# Neo4j, thesaurus, and vector processing from existing RDBMS output
.venv3.14/bin/python run_pipeline.py --stage neo4j
```

The `--skip-thesaurus-embed` option is retained for compatibility. The current
orchestrator skips per-thesaurus embedding during thesaurus loading and creates
the complete graph embedding in the later `vectorize_neo4j.py` stage. To avoid
embedding altogether, run only `--stage rdbms` or invoke the Neo4j stages
individually.

Re-running `upload_data.py` drops and recreates target tables, so derived
columns such as `tei` and `tei_status` are removed. After reloading source data,
run the TEI and later stages again. [graph_builder.py](./scripts/graph_builder.py)
also changes the graph and is not needed for read-only search or UI work.

### Individual preprocessing commands

```bash
# Load source files
.venv3.14/bin/python upload_data.py

# Process selected tables
TEI_TABLES=raw_event_info,raw_bibliography \
  .venv3.14/bin/python scripts/pg_to_pg_with_tei.py

# Refine TEI
.venv3.14/bin/python scripts/tag_tei_with_dict.py --limit 100 --skip-hitl
.venv3.14/bin/python scripts/tag_tei_with_dict.py --all --skip-hitl

# Preview i815 linking before applying changes
.venv3.14/bin/python scripts/link_persnames_i815.py --dry-run --limit 20
.venv3.14/bin/python scripts/link_persnames_i815.py --apply

# Extract entities and generate CIDOC mappings
.venv3.14/bin/python scripts/extract_all_entities.py
.venv3.14/bin/python scripts/generate_cidoc_mappings.py
```

`upload_data.py` and graph-building scripts modify databases. Verify the target
and make a backup before running them against operational data.

## PHP Docent

[docent.php](./docent.php) provides the web UI/API and applies CSRF, Origin, and
rate-limit checks to AJAX requests. Start a local PHP server from the repository
root:

```bash
php -S 127.0.0.1:8000
```

Open <http://127.0.0.1:8000/docent.php> or
[index.html](./index.html).

The CLI debugging entry point is
[scripts/run_docent_pipeline.php](./scripts/run_docent_pipeline.php):

```bash
php scripts/run_docent_pipeline.php "Suwon Sagang-ri protest process" en
```

Available AJAX actions:

| Action | Purpose |
|---|---|
| `analyze` | Analyze language, focus, search keywords, and intent |
| `graph` | Generate Text-to-Cypher, query Neo4j, and build graph evidence |
| `pg_prefetch` | Retrieve PostgreSQL source records for graph candidates |
| `explain` | Build fact evidence and generate the final explanation |
| `node_detail` | Retrieve source details for a graph node |

The search and generation flow is:

```text
query
  │
  ├─ analyze: Gemini JSON intent/keyword analysis
  │            local token fallback on failure
  │
  ├─ graph: Text-to-Cypher generation, read-only validation, Neo4j execution
  │         namesIndex and CONTAINS fallback when needed
  │
  ├─ pg_prefetch: PostgreSQL source retrieval
  │
  └─ explain:
       evidence budget
       → GraphRAG entity/multi-hop enrichment
       → Stage 1 fact table
       → Stage 2 multilingual historical explanation
```

Generated Cypher is written to the PHP `error_log` with these prefixes:

```text
[Docent][Text-to-Cypher] generated query ...
[Docent][Neo4j] executing dynamic Cypher:
[Docent][Neo4j] dynamic Cypher returned N record(s)
```

Read-only validation accepts queries beginning with `MATCH`, `OPTIONAL MATCH`,
`WITH`, or `CALL` and blocks mutation keywords such as `DELETE`, `CREATE`,
`MERGE`, `SET`, and `DROP`.

### Temporal-query limitation

Event dates are represented by static Event-node properties such as `날짜` and
`발생일자`. Relationship validity intervals such as `valid_from` and `valid_to`
are not implemented. A query such as “membership before March 1919” therefore
cannot be answered as an exact temporal relationship query. The system must
limit the answer to an event-date approximation or state that the information
is not confirmed by the available sources.

## MCP server

[mcp_server.py](./mcp_server.py) provides these read-oriented tools:

- `query_postgres`: execute `SELECT`/`WITH` queries
- `get_postgres_schema`: inspect PostgreSQL tables and columns
- `query_neo4j`: execute a Cypher query
- `get_neo4j_schema`: inspect Neo4j labels and relationship types

Run the server with:

```bash
.venv3.14/bin/python mcp_server.py
```

Register the command from [mcp_client_config.json](./mcp_client_config.json)
after updating absolute paths for your environment. Keep credentials in
environment variables or `.streamlit/secrets.toml`, not in client configuration.

## RAG evaluation

The benchmark dataset is
[evaluation/dataset/multihop_benchmark_dataset.json](./evaluation/dataset/multihop_benchmark_dataset.json).
The compared systems are:

- **Vector RAG**: PostgreSQL source-chunk retrieval
- **Hybrid Vector RAG**: BM25 + dense retrieval, RRF fusion, cross-encoder
  rerank ([evaluation/baselines.py](./evaluation/baselines.py)); retrieves from
  the question text only
- **Standard GraphRAG**: graph triple/context retrieval
- **Microsoft GraphRAG** (opt-in): the reference implementation, queried through
  its CLI
- **Docent Hybrid RAG**: combined graph and source retrieval with strict
  evidence-grounded prompting

Run the complete RAG comparison:

```bash
.venv3.14/bin/python evaluation/eval_rag_comparison.py --delay 15
```

Useful modes:

```bash
# Three representative queries
.venv3.14/bin/python evaluation/eval_rag_comparison.py --quick --delay 0

# One query and full response text
.venv3.14/bin/python evaluation/eval_rag_comparison.py --single --delay 0

# First N queries
.venv3.14/bin/python evaluation/eval_rag_comparison.py --limit 10 --delay 0
```

The unified runner executes NER/relation evaluation and RAG comparison:

```bash
.venv3.14/bin/python evaluation/run_benchmark.py --quick --delay 0
.venv3.14/bin/python evaluation/run_benchmark.py --quick --ragas --delay 0
```

Regenerate the benchmark only when the resulting questions and ground truth
will be manually reviewed:

```bash
.venv3.14/bin/python evaluation/generate_benchmark_dataset.py \
  --target-count 30
```

### RAGAS

[eval_rag_comparison.py](./evaluation/eval_rag_comparison.py) writes questions,
responses, retrieved contexts, and references to
[evaluation/results/ragas_dataset.json](./evaluation/results/ragas_dataset.json).
The RAGAS report is written to
[evaluation/results/ragas_results.json](./evaluation/results/ragas_results.json).
The standard metrics are Faithfulness, Context Precision with Reference, and
Context Recall.

Local Ollama evaluation:

```bash
RAGAS_PROVIDER=ollama \
.venv3.14/bin/python evaluation/eval_ragas.py
```

Gemini evaluation requires `GEMINI_API_KEY`:

```bash
RAGAS_PROVIDER=gemini \
GEMINI_RAGAS_MODEL=gemini-flash-lite-latest \
GEMINI_EMBEDDING_MODEL=models/gemini-embedding-001 \
.venv3.14/bin/python evaluation/eval_ragas.py
```

Items with `evaluation_type=abstention` are evaluated for Faithfulness only,
because Context Precision and Context Recall are not well-defined for negative
verification prompts. A RAGAS parser failure must not be replaced with a zero;
the evaluation should fail explicitly.

### Direct context-precision Judge

The repository also includes a separate Ollama JSON Judge:

```bash
.venv3.14/bin/python evaluation/eval_context_precision_judge.py \
  --input evaluation/results/ragas_dataset.json \
  --output evaluation/results/context_precision_judge_results.json \
  --model gemma4:26b \
  --host http://localhost:11434 \
  --workers 3
```

This is a supplementary metric, not a replacement for the RAGAS score.

### Baselines, categories, and significance

All pipelines run under the same conditions, set by these flags:

| Flag | Default | Meaning |
|---|---|---|
| `--retrieval-input` | `question` | Every pipeline retrieves with entities extracted from the question text (one shared LLM call per question). `entities` gives every pipeline the gold `target_entities` instead. |
| `--temperature`, `--top-p` | `0.0`, `0.8` | Used for every generation call. |
| `--context-chars` | `2400` | Retrieved-context budget shared by all pipelines (`0` restores the old per-pipeline sizes). |
| `--retries` | `2` | Extra attempts when a generation fails (rate limits). |

Docent still issues two generation calls (fact table, then synthesis), so its
total prompt is larger than the baselines' even with the same retrieved
context; `context_chars` and `prompt_tokens` are reported per question.

Select pipelines with `--models` (default `vector,hybrid,graph,docent`). The
hybrid baseline builds its dense index once under `vector_store/baselines/`;
set `BASELINE_EMBED_MODEL` / `BASELINE_RERANK_MODEL` to change models.

Microsoft GraphRAG needs its own index, which is a long, paid LLM job and is
therefore not run automatically:

```bash
.venv3.14/bin/python evaluation/baselines.py export-graphrag --root ./graphrag_root --dry-run
.venv3.14/bin/python evaluation/baselines.py export-graphrag --root ./graphrag_root
graphrag init --root ./graphrag_root && graphrag index --root ./graphrag_root
GRAPHRAG_ROOT=./graphrag_root .venv3.14/bin/python evaluation/eval_rag_comparison.py \
  --models vector,hybrid,graph,msgraphrag,docent
```

Every run also writes per-category results and paired significance tests
(sign-flip permutation test with Holm correction, bootstrap 95% CIs). Questions
where any pipeline failed to generate an answer are excluded from the tests.
To recompute them from an existing CSV:

```bash
.venv3.14/bin/python evaluation/analyze_rag_results.py
```

### Benchmark size and third-party questions

```bash
.venv3.14/bin/python evaluation/generate_benchmark_dataset.py --target-count 120 \
  --third-party path/to/external_questions.json
```

Each question carries `source` (`curated`, `third_party`, `template`).
External authors start from
[evaluation/dataset/third_party_template.json](./evaluation/dataset/third_party_template.json).
Template questions are filled in from `raw_event_info` and their gold facts
must be reviewed before they are reported.

### Human evaluation

Extraction quality, judged by historians on a seeded random sample of records:

```bash
.venv3.14/bin/python evaluation/expert_validation.py export --n 150
.venv3.14/bin/python evaluation/expert_validation.py score \
  evaluation/expert_validation/annotation_A.xlsx evaluation/expert_validation/annotation_B.xlsx
```

Final answers, rated blind for factual accuracy, citation accuracy, and
usefulness:

```bash
.venv3.14/bin/python evaluation/human_answer_eval.py export
.venv3.14/bin/python evaluation/human_answer_eval.py score \
  evaluation/human_eval/rating_A.xlsx evaluation/human_eval/rating_B.xlsx
```

`blinding_key.json` maps the shuffled answer labels back to models; do not
send it to raters.

### Graph sparsity

An `Event` node is one `raw_event_info` row. To report how many `Event` nodes
are isolated and where they come from (read-only; `--cleanup` lists removable
artifact nodes and deletes them only with `--apply`):

```bash
.venv3.14/bin/python scripts/graph_sparsity_report.py --cleanup
```

### Evaluation outputs

| File | Contents |
|---|---|
| `evaluation/results/rag_evaluation_results.csv` | Per-query, per-model details |
| `evaluation/results/table_rag_comparison.tex` | Paper-ready RAG comparison table |
| `evaluation/results/rag_category_results.csv` | Per-category means with 95% CIs |
| `evaluation/results/rag_significance.csv` | Paired significance tests between pipelines |
| `evaluation/results/table_rag_by_category.tex` | Per-category LaTeX table |
| `evaluation/results/expert_validation_results.json` | Expert-judged extraction precision/recall |
| `evaluation/results/human_eval_results.json` | Human answer ratings and citation accuracy |
| `evaluation/results/graph_sparsity_report.json` | Event-layer sparsity report |
| `evaluation/results/ragas_dataset.json` | RAGAS input records |
| `evaluation/results/ragas_results.json` | RAGAS metric results |
| `evaluation/results/context_precision_judge_results.json` | Direct Judge results |
| `evaluation/results/table_ner_performance.tex` | NER/RE LaTeX table |
| `evaluation/results/ner_evaluation_results.csv` | NER/RE details |

`entity_hit_rate_at_k` and `entity_mrr` are LLM-independent diagnostic
retrieval metrics. The current MRR implementation measures the first context
containing at least one target entity; it does not measure complete target
coverage or semantic correctness. An MRR of `1.0000` must therefore be reported
with Context Precision, source/event-ID Hit@K, and query-type analysis.

The current benchmark has `N=30`. This is useful for system verification and
effect-size exploration, but is too small by itself for broad population-level
significance or generalization claims. Report per-query results, bootstrap
confidence intervals, query-type results, and the template/entity-anchor bias.

## Validation

These checks do not intentionally rebuild the databases:

```bash
php -l includes/actions.php
php -l docent.php
.venv3.14/bin/python -m py_compile run_pipeline.py mcp_server.py
.venv3.14/bin/python run_pipeline.py --dry-run
.venv3.14/bin/python scripts/verify_pipeline.py
```

Database connectivity checks require valid `PG_*` and `NEO4J_*` settings. When
working with a remote database, verify the target with read-only queries and
`--dry-run` before running `upload_data.py`, `graph_builder.py`, or
`apply_vectors_to_neo4j.py`.

## Important paths

| Path | Role |
|---|---|
| [data/](./data/) | Source files, i815 index, and mappings |
| [scripts/](./scripts/) | PostgreSQL, TEI, CIDOC, and Neo4j processing |
| [includes/](./includes/) | PHP configuration, database, security, prompts, and actions |
| [evaluation/](./evaluation/) | Benchmarks, RAGAS, and NER/RE evaluation |
| [vector_store/](./vector_store/) | Generated embedding artifacts |
| [Latex/](./Latex/) | Manuscript and LaTeX build artifacts |
| [run_pipeline.py](./run_pipeline.py) | Full preprocessing orchestrator |
| [docent.php](./docent.php) | PHP web/API entry point |
| [mcp_server.py](./mcp_server.py) | PostgreSQL/Neo4j MCP server |

## Operational safety

- `upload_data.py` drops and recreates target tables; use it cautiously in
  operational environments.
- `graph_builder.py` and `apply_vectors_to_neo4j.py` modify Neo4j.
- Confirm the target and take a backup before destructive or reset operations.
- Keep source evidence, TEI, ground truth, and retrieved context conceptually
  separate. Do not modify ground truth to match a model response.
- Do not infer temporal relationship validity from static edges.
- Do not expose API keys, database passwords, or remote connection details in
  logs or commits.

## Korean documentation

See [README_ko.md](./README_ko.md) for the Korean version of this documentation.

## License

See [LICENSE](./LICENSE).
