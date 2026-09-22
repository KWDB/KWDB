-- Information Schema metadata regression coverage.

DROP DATABASE IF EXISTS is_rel_db CASCADE;
DROP DATABASE IF EXISTS is_ts_db CASCADE;
CREATE DATABASE is_rel_db;
CREATE TS DATABASE is_ts_db;

CREATE TABLE is_rel_db.rel_table (
    id INT PRIMARY KEY,
    amount DECIMAL(10, 2),
    name VARCHAR(20)
);
COMMENT ON TABLE is_rel_db.rel_table IS 'relational table comment';

CREATE VIEW is_rel_db.rel_view AS
SELECT id, amount FROM is_rel_db.rel_table;
CREATE MATERIALIZED VIEW is_rel_db.rel_mat_view AS
SELECT id, amount FROM is_rel_db.rel_table;
CREATE SEQUENCE is_rel_db.rel_sequence;

CREATE TABLE is_ts_db.ts_table (
    ts TIMESTAMPTZ NOT NULL,
    value INT
) TAGS (host VARCHAR(20) NOT NULL) PRIMARY TAGS (host);

CREATE SPARSE TABLE is_ts_db.sparse_table (
    ts TIMESTAMPTZ NOT NULL,
    value INT
) TAGS (host VARCHAR(20) NOT NULL) PRIMARY TAGS (host);

SET experimental_enable_temp_tables = 'on';
CREATE TEMP TABLE temp_is_table (id INT);

SELECT table_name, table_type, is_insertable_into,
       engine, row_format, table_comment
FROM is_rel_db.information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

SELECT table_name, update_time IS NOT NULL AS has_update_time
FROM is_rel_db.information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN ('rel_table', 'rel_view', 'rel_mat_view')
ORDER BY table_name;

SELECT table_name, table_type, is_insertable_into,
       engine, row_format
FROM is_ts_db.information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

SELECT table_name, table_type, engine, row_format
FROM information_schema.tables
WHERE table_schema LIKE 'pg_temp_%'
  AND table_name = 'temp_is_table';
SET experimental_enable_temp_tables = 'off';

SELECT table_name
FROM is_rel_db.information_schema.tables
WHERE table_name = 'rel_sequence';

SELECT table_name, column_name, data_type, is_tag,
       declared_data_type, declared_numeric_precision,
       declared_numeric_scale, is_system_time_period_start,
       is_system_time_period_end,
       system_time_period_timestamp_generation
FROM is_rel_db.information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'rel_table'
ORDER BY ordinal_position;

SELECT table_name, column_name, data_type, is_tag,
       declared_data_type, declared_numeric_precision,
       declared_numeric_scale
FROM is_ts_db.information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN ('ts_table', 'sparse_table')
ORDER BY table_name, ordinal_position;

SELECT schema_name, engine_type, retentions, partition_interval,
       schema_owner, default_character_set_catalog,
       default_character_set_schema
FROM is_rel_db.information_schema.schemata
WHERE schema_name = 'public';

SELECT schema_name, engine_type, retentions, partition_interval,
       schema_owner, default_character_set_catalog,
       default_character_set_schema
FROM is_ts_db.information_schema.schemata
WHERE schema_name = 'public';

SELECT table_name, insertable_into, is_updatable,
       is_trigger_updatable,
       is_trigger_deletable, is_trigger_insertable_into
FROM is_rel_db.information_schema.views
WHERE table_schema = 'public'
ORDER BY table_name;

SELECT word, reserved
FROM information_schema.keywords
WHERE word IN ('create', 'select', 'table', 'kwdb')
ORDER BY word;

SELECT count(*) > 0 AS has_keywords
FROM information_schema.keywords;

DROP DATABASE is_rel_db CASCADE;
DROP DATABASE is_ts_db CASCADE;
