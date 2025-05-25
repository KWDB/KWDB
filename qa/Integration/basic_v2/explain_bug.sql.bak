USE defaultdb;
DROP DATABASE IF EXISTS test_select_last_add cascade;
CREATE ts DATABASE test_select_last_add;
CREATE TABLE test_select_last_add.t1(k_timestamp TIMESTAMPTZ NOT NULL, id INT NOT NULL) ATTRIBUTES (code1 INT2 NOT NULL) PRIMARY TAGS(code1);

explain SELECT TIME_BUCKET(k_timestamp,concat('0','10000000000','s')) AS tb FROM (SELECT k_timestamp FROM test_select_last_add.t1 GROUP BY id,k_timestamp) GROUP BY tb;

explain SELECT TIME_BUCKET(k_timestamp,'10000000000s') AS tb FROM (SELECT k_timestamp FROM test_select_last_add.t1 GROUP BY id,k_timestamp) GROUP BY tb;

DROP DATABASE IF EXISTS test_select_last_add cascade;