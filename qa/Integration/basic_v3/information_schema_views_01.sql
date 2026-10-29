--test_case001 table_constraints_all_types
USE defaultdb;
DROP DATABASE IF EXISTS is_constraint_it CASCADE;
CREATE DATABASE is_constraint_it;
SET DATABASE = is_constraint_it;
CREATE SCHEMA app;

CREATE TABLE app.parent (
    id INT PRIMARY KEY,
    code INT,
    CONSTRAINT parent_code_uq UNIQUE (code)
);

CREATE TABLE app.child (
    id INT PRIMARY KEY,
    parent_id INT,
    value INT NOT NULL,
    CONSTRAINT child_value_check CHECK (value > 0),
    CONSTRAINT child_parent_fk FOREIGN KEY (parent_id)
        REFERENCES app.parent(id)
);

SELECT table_name, constraint_name, constraint_type, enforced, nulls_distinct
FROM information_schema.table_constraints
WHERE constraint_name IN ('parent_code_uq', 'child_value_check', 'child_parent_fk', 'primary')
ORDER BY table_name, constraint_name;

SELECT constraint_name, check_clause
FROM information_schema.check_constraints
WHERE constraint_name = 'child_value_check'
ORDER BY constraint_name;

USE defaultdb;
DROP DATABASE is_constraint_it CASCADE;

--test_case002 statistics_all_index_types
USE defaultdb;
DROP DATABASE IF EXISTS is_statistics_it CASCADE;
CREATE DATABASE is_statistics_it;
SET DATABASE = is_statistics_it;
CREATE SCHEMA app;

CREATE TABLE app.index_table (
    id INT PRIMARY KEY,
    nullable_col INT,
    not_null_col INT NOT NULL,
    payload JSONB,
    INDEX normal_idx (nullable_col),
    UNIQUE INDEX unique_idx (not_null_col),
    INVERTED INDEX inverted_idx (payload)
);

COMMENT ON INDEX app.index_table@normal_idx IS 'normal index comment';

SELECT table_schema, table_name, non_unique, index_name, seq_in_index,
       column_name, storing, implicit, nullable, index_type, index_comment,
       is_visible, expression
FROM information_schema.statistics
WHERE table_schema = 'app' AND table_name = 'index_table'
ORDER BY index_name, seq_in_index;

COMMENT ON INDEX app.index_table@normal_idx IS NULL;

SELECT index_name, index_comment
FROM information_schema.statistics
WHERE table_schema = 'app'
  AND table_name = 'index_table'
  AND index_name = 'normal_idx'
ORDER BY seq_in_index;

USE defaultdb;
DROP DATABASE is_statistics_it CASCADE;

--test_case003 referential_constraints_cross_database_schema
USE defaultdb;
DROP DATABASE IF EXISTS is_parent_it CASCADE;
DROP DATABASE IF EXISTS is_child_it CASCADE;
CREATE DATABASE is_parent_it;
SET DATABASE = is_parent_it;
CREATE SCHEMA parent_s;
CREATE TABLE parent_s.parent (
    id INT PRIMARY KEY,
    code INT,
    CONSTRAINT parent_code_uq UNIQUE (code)
);

CREATE DATABASE is_child_it;
SET DATABASE = is_child_it;
CREATE SCHEMA child_s;
CREATE TABLE child_s.child (
    parent_id INT,
    CONSTRAINT child_parent_fk FOREIGN KEY (parent_id)
        REFERENCES is_parent_it.parent_s.parent(id)
);

SET DATABASE = is_child_it;

SELECT constraint_catalog, constraint_schema, constraint_name,
       unique_constraint_catalog, unique_constraint_schema,
       unique_constraint_name, table_name, referenced_table_name
FROM information_schema.referential_constraints
WHERE constraint_name = 'child_parent_fk';

USE defaultdb;
DROP DATABASE is_child_it CASCADE;
DROP DATABASE is_parent_it CASCADE;
