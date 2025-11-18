setup
{
	CREATE TABLE IF NOT EXISTS  test_dc(id serial primary key, data int);
	INSERT INTO test_dc(data) SELECT * FROM generate_series(1, 100);
	CREATE INDEX test_dc_data ON test_dc(data);
}

teardown
{
	DROP TABLE test_dc;
}


session s1
step s1      { BEGIN transaction isolation level read committed; }
step exp     { EXPLAIN SELECT data FROM test_dc WHERE data = 34 ORDER BY id,data; }
step s1r     { SELECT data FROM test_dc WHERE data = 34 ORDER BY id,data; }
step s1c     { COMMIT; }

session s2
step s2     { BEGIN transaction isolation level read committed; }
step s2r    { SELECT data FROM test_dc WHERE data = 34 ORDER BY id,data; }
step s2w    { INSERT INTO test_dc(data) SELECT * FROM generate_series(1, 100); }
step s2c    { COMMIT; }

permutation s1 s2 exp s2r s2w s2c exp s1r s1c
