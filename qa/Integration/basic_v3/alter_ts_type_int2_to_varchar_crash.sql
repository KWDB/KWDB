DROP DATABASE IF EXISTS test_ddl_dml_mix1 cascade;
CREATE ts DATABASE test_ddl_dml_mix1;
CREATE TABLE test_ddl_dml_mix1.test1(ts TIMESTAMPTZ NOT NULL, id INT, e1 INT2) TAGS(code1 INT2 NOT NULL) PRIMARY TAGS(code1);
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1970-01-01 00:00:00+00:00', 1, 0, 20002);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e1 SET DATA TYPE VARCHAR(10);
SELECT * FROM test_ddl_dml_mix1.test1 ORDER BY ts DESC LIMIT 40 OFFSET 0 ;
DROP DATABASE test_ddl_dml_mix1 cascade;