setup
{
  CREATE TABLE IF NOT EXISTS  ints (key int, val text, PRIMARY KEY (key));
}

teardown
{
  DROP TABLE ints;
}

session s1
step s1     { BEGIN transaction isolation level read committed; }
step s1w    { INSERT INTO ints(key, val) VALUES(1, 'donothing1') ON CONFLICT DO NOTHING; }
step s1c    { COMMIT; }
step show   { SELECT * FROM ints; }

session s2
step s2     { BEGIN transaction isolation level read committed; }
step s2w    { INSERT INTO ints(key, val) VALUES(1, 'donothing2'), (1, 'donothing3') ON CONFLICT DO NOTHING; }
step s2c    { COMMIT; }

permutation s1 s2 s1w s1c s2w s2c show
permutation s1 s2 s2w s2c s1w s1c show
permutation s1 s2 s1w s2w s1c s2c show
permutation s1 s2 s2w s1w s2c s1c show
permutation s2 s1 s1w s1c s2w s2c show
permutation s2 s1 s2w s2c s1w s1c show
permutation s2 s1 s1w s2w s1c s2c show
permutation s2 s1 s2w s1w s2c s1c show
