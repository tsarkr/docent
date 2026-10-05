-- Trigram GIN indexes for the name search used by the deployed app.
--
-- NOT applied automatically. Review, then run by hand against the app database:
--   psql "$DATABASE_URL" -f migrations/001_pg_trgm_name_search.sql
--
-- Why: the app looks names up with unanchored patterns, which a b-tree index
-- cannot serve, so every lookup scans the whole table:
--   includes/database.php   "<col>"::text ILIKE ANY(ARRAY['%이름%', ...])   (tei + mapped text columns)
--   includes/actions.php    raw_event_info."관련인물" LIKE '%이름%'
--   includes/actions.php    tei_cidoc_mappings.mapping_label LIKE '%이름%'
-- gin_trgm_ops indexes support LIKE / ILIKE '%...%' directly.
--
-- Notes
--   * CREATE EXTENSION needs a role allowed to install extensions (superuser, or
--     the database owner on managed services that whitelist pg_trgm).
--   * CREATE INDEX CONCURRENTLY does not block writes but cannot run inside a
--     transaction block: do not wrap this file in BEGIN/COMMIT and do not use
--     psql --single-transaction. If a build is interrupted it leaves an INVALID
--     index; drop it (statements at the bottom) and re-run.
--   * A pattern needs at least 3 characters to use the index. Two-syllable names
--     ('%김구%') still fall back to a sequential scan.
--   * Trigrams of Hangul require a non-C database locale (the current database
--     is en_US.UTF-8, which is fine). Check with:  SELECT show_trgm('김상열');
--     an empty result means the indexes would not help.
--   * The tei indexes are the large ones (full TEI documents). Check sizes after
--     the build:  SELECT indexrelname, pg_size_pretty(pg_relation_size(indexrelid))
--                 FROM pg_stat_user_indexes WHERE indexrelname LIKE '%_trgm';
--   * Verify the planner uses them (expect a Bitmap Index Scan on *_trgm):
--       EXPLAIN (ANALYZE, BUFFERS)
--       SELECT * FROM raw_event_info WHERE "관련인물" LIKE '%김상열%' LIMIT 1;
--       EXPLAIN (ANALYZE, BUFFERS)
--       SELECT * FROM raw_bibliography WHERE "tei"::text ILIKE ANY(ARRAY['%김상열%','%홍기황%']) LIMIT 25;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Person-name columns (includes/actions.php, and text_cols in includes/database.php)
CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_event_info_persons_trgm
    ON raw_event_info USING gin ("관련인물" gin_trgm_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_source_info_persons_trgm
    ON raw_source_info USING gin ("관련인물" gin_trgm_ops);

-- Mapping labels (includes/actions.php)
CREATE INDEX CONCURRENTLY IF NOT EXISTS tei_cidoc_mappings_label_trgm
    ON tei_cidoc_mappings USING gin (mapping_label gin_trgm_ops);

-- TEI text of the source tables (includes/database.php), largest tables first
CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_source_info_tei_trgm
    ON raw_source_info USING gin (tei gin_trgm_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_bibliography_tei_trgm
    ON raw_bibliography USING gin (tei gin_trgm_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_detail_place_tei_trgm
    ON raw_detail_place USING gin (tei gin_trgm_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_event_info_tei_trgm
    ON raw_event_info USING gin (tei gin_trgm_ops);

ANALYZE raw_event_info;
ANALYZE raw_source_info;
ANALYZE raw_bibliography;
ANALYZE raw_detail_place;
ANALYZE tei_cidoc_mappings;

-- Rollback
-- DROP INDEX CONCURRENTLY IF EXISTS raw_event_info_persons_trgm;
-- DROP INDEX CONCURRENTLY IF EXISTS raw_source_info_persons_trgm;
-- DROP INDEX CONCURRENTLY IF EXISTS tei_cidoc_mappings_label_trgm;
-- DROP INDEX CONCURRENTLY IF EXISTS raw_source_info_tei_trgm;
-- DROP INDEX CONCURRENTLY IF EXISTS raw_bibliography_tei_trgm;
-- DROP INDEX CONCURRENTLY IF EXISTS raw_detail_place_tei_trgm;
-- DROP INDEX CONCURRENTLY IF EXISTS raw_event_info_tei_trgm;
