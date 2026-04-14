CREATE ts DATABASE test_get_rowcount;
CREATE TABLE test_get_rowcount.t1(k_timestamp timestamp not null,e1 int2 not null)
    attributes (t1_attribute int not null) primary tags(t1_attribute) with hash(400);

INSERT INTO test_get_rowcount.t1 VALUES ('2018-10-10 10:00:00', -1, 1);
INSERT INTO test_get_rowcount.t1 VALUES ('2018-10-10 10:00:01', 0, 2);

SELECT
    get_range_count(r.range_id, replica_id)
FROM
    kwdb_internal.ranges r,
    UNNEST(r.replicas) AS replica_id
WHERE
    r.table_name = 't1'
AND
    r.database_name = 'test_get_rowcount'
ORDER BY
    replica_id, r.range_id
LIMIT 1;
SELECT get_range_count(40, 1);
SELECT get_range_count(54, 6);
SELECT get_range_count(10000000, 1);

DROP DATABASE test_get_rowcount CASCADE;
