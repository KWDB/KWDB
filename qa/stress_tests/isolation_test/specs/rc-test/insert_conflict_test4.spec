setup
{
  CREATE TABLE IF NOT EXISTS upsert (key text not null, payload text);
  CREATE UNIQUE INDEX ON upsert(key) INCLUDE (payload);
}

teardown
{
  DROP TABLE upsert;
}

session s1
setup       { BEGIN transaction isolation level read committed; }
step s1w    { INSERT INTO upsert(key, payload) VALUES('FooFoo', 'insert1') ON CONFLICT (key) DO UPDATE set key = EXCLUDED.key, payload = upsert.payload || ' updated by insert1'; }
step s1c    { COMMIT; }
step s1a    { ABORT; }

session s2
setup       { BEGIN transaction isolation level read committed; }
step s2w    { INSERT INTO upsert(key, payload) VALUES('FOOFOO', 'insert2') ON CONFLICT (key) DO UPDATE set key = EXCLUDED.key, payload = upsert.payload || ' updated by insert2'; }
step s2r    { SELECT * FROM upsert; }
step s2c    { COMMIT; }

permutation s1w s2w s1c s2r s2c
permutation s1w s2w s1a s2r s2c
