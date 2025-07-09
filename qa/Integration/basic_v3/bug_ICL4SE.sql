DROP DATABASE IF EXISTS test_last_row cascade;
CREATE ts DATABASE test_last_row;
CREATE TABLE test_last_row.test1(ts TIMESTAMPTZ NOT NULL, id INT, e1 INT2) TAGS(code1 INT2 NOT NULL) PRIMARY TAGS(code1);
INSERT INTO test_last_row.test1 VALUES('2010-01-01 00:00:00+00:00', 1, 5, 25);
ALTER TABLE test_last_row.test1 DROP COLUMN e1;
ALTER TABLE test_last_row.test1 ADD COLUMN e1 INT4;
INSERT INTO test_last_row.test1 VALUES('2002-2-22 2:22:22.222', 2, 22222, 30);
SELECT LAST(e1),LAST_ROW(e1) FROM test_last_row.test1;
DROP DATABASE IF EXISTS test_last_row cascade;