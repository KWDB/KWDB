setup
{
  CREATE TABLE IF NOT EXISTS  foo (a int PRIMARY KEY, b text);
  CREATE TABLE IF NOT EXISTS  bar (a int NOT NULL REFERENCES foo);
  INSERT INTO foo VALUES (42);
}

teardown
{
  DROP TABLE foo, bar;
}

session s1
step s1		{ BEGIN transaction isolation level read committed; }
step s1w	{ INSERT INTO bar VALUES (42); }
step s1r1   { select * from foo; }
step s1r2   { select * from bar; }
step s1c	{ COMMIT; }

session s2
step s2     { BEGIN transaction isolation level read committed; }
step s2u    { UPDATE foo SET b = 'Hello World'; }
step s2r1   { select * from foo; }
step s2r2   { select * from bar; }
step s2c    { COMMIT; }

permutation s1 s2 s1w s2u s1r1 s1r2 s1c s2r1 s2r2 s2c
permutation s1 s2 s1w s2u s2r1 s2r2 s2c s1r1 s1r2 s1c
permutation s1 s2 s1w s2r1 s2r2 s2u s1r1 s1r2 s1c s2c
permutation s1 s2 s1w s2r1 s2r2 s2u s1r1 s1r2 s2c s1c
