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

-- QA integration scripts run as root, so cover the new role grant views from
-- the administrator-visible scope. Detailed non-admin visibility stays in
-- the SQL logic tests.
CREATE ROLE is_privilege_role;
CREATE PROCEDURE is_rel_db.is_all_routine() BEGIN SELECT 1; END;
GRANT SELECT ON DATABASE is_rel_db TO is_privilege_role;
GRANT ALL ON PROCEDURE is_rel_db.is_all_routine TO is_privilege_role;
GRANT SELECT ON TABLE is_rel_db.rel_table TO is_privilege_role;

SELECT grantee, table_catalog, table_schema, privilege_type
FROM is_rel_db.information_schema.schema_privileges
WHERE grantee = 'is_privilege_role'
  AND table_catalog = 'is_rel_db'
ORDER BY table_schema, privilege_type;

SELECT grantee, table_schema, table_name, privilege_type
FROM is_rel_db.information_schema.table_privileges
WHERE grantee = 'is_privilege_role'
  AND table_schema = 'public'
  AND table_name = 'rel_table'
ORDER BY privilege_type;

SELECT grantee, table_name, column_name, privilege_type
FROM is_rel_db.information_schema.column_privileges
WHERE grantee = 'is_privilege_role'
  AND table_schema = 'public'
  AND table_name = 'rel_table'
ORDER BY column_name, privilege_type;

SELECT grantee, table_catalog, privilege_type
FROM is_rel_db.information_schema.user_privileges
WHERE table_catalog = 'is_rel_db'
ORDER BY grantee, privilege_type;

SELECT table_name
FROM is_rel_db.information_schema.tables
WHERE table_schema = 'information_schema'
  AND table_name IN ('role_column_grants', 'role_routine_grants')
ORDER BY table_name;

SELECT grantee, specific_catalog, specific_schema, specific_name,
       routine_catalog, routine_schema, routine_name, privilege_type
FROM is_rel_db.information_schema.role_routine_grants
WHERE grantee = 'is_privilege_role'
  AND routine_name = 'is_all_routine';

SELECT grantee, table_catalog, table_schema, table_name, privilege_type
FROM is_rel_db.information_schema.role_table_grants
WHERE grantee = 'is_privilege_role'
  AND table_schema = 'public'
  AND table_name = 'rel_table';

SELECT grantee, column_name, privilege_type
FROM is_rel_db.information_schema.role_column_grants
WHERE grantee = 'is_privilege_role'
  AND table_schema = 'public'
  AND table_name = 'rel_table'
  AND column_name = 'id';

DROP DATABASE is_rel_db CASCADE;
DROP DATABASE is_ts_db CASCADE;
DROP ROLE is_privilege_role;
