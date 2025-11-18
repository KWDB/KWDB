setup
{
  CREATE TABLE IF NOT EXISTS  ints (key int primary key, val text);
}

teardown
{
  DROP TABLE ints;
}

session s1
setup         { BEGIN transaction isolation level read committed; }
step s1w      { INSERT INTO ints(key, val) VALUES(1, 'donothing1') ON CONFLICT DO NOTHING; }
step s1c      { COMMIT; }
step s1a      { ABORT; }

session s2
setup         { BEGIN transaction isolation level read committed; }
step s2w      { INSERT INTO ints(key, val) VALUES(1, 'donothing2') ON CONFLICT DO NOTHING; }
step s2r      { SELECT * FROM ints; }
step s2c      { COMMIT; }


permutation s1w s2w s1c s2r s2c
permutation s1w s2w s1a s2r s2c
