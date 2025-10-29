CREATE ts DATABASE test_get_rowcount;
CREATE TABLE test_get_rowcount.t1(k_timestamp timestamp not null,e1 int2 not null)
    attributes (t1_attribute int not null) primary tags(t1_attribute) with hash(400);

INSERT INTO test_get_rowcount.t1 VALUES ('2018-10-10 10:00:00', -1, 1);
INSERT INTO test_get_rowcount.t1 VALUES ('2018-10-10 10:00:01', 0, 2);

SELECT get_range_count(40, 1);
SELECT get_range_count(54, 6);
SELECT get_range_count(10000000, 1);

DROP DATABASE test_get_rowcount CASCADE;