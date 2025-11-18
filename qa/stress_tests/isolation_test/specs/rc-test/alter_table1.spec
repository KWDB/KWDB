setup
{
 CREATE TABLE IF NOT EXISTS  a (i int PRIMARY KEY);
 CREATE TABLE IF NOT EXISTS  b (a_id int);
 CREATE INDEX idx1 on b(a_id);
 INSERT INTO a VALUES (0), (1), (2), (3);
 INSERT INTO b SELECT generate_series(1,1000) % 4;
}

teardown
{
 DROP TABLE a, b;
}

session s1
step s1		{ BEGIN transaction isolation level read committed; }
step at1	{ ALTER TABLE b ADD CONSTRAINT bfk FOREIGN KEY (a_id) REFERENCES a (i) NOT VALID; }
step sc1	{ COMMIT; }
step s2		{ BEGIN; }
step at2	{ ALTER TABLE b VALIDATE CONSTRAINT bfk; }
step sc2	{ COMMIT; }

session s2
setup		{ BEGIN transaction isolation level read committed; }
step rx1	{ SELECT * FROM b WHERE a_id = 1 LIMIT 1; }
step wx		{ INSERT INTO b VALUES (0); }
step rx3	{ SELECT * FROM b WHERE a_id = 3 LIMIT 3; }
step c2		{ COMMIT; }

permutation s1 at1 sc1 s2 at2 sc2 rx1 wx rx3 c2
permutation s1 at1 sc1 s2 at2 rx1 sc2 wx rx3 c2
permutation s1 at1 sc1 s2 at2 rx1 wx sc2 rx3 c2
permutation s1 at1 sc1 s2 at2 rx1 wx rx3 sc2 c2
permutation rx1 wx s1 rx3 c2 at1 sc1 s2 at2 sc2
permutation rx1 wx rx3 s1 at1 c2 sc1 s2 at2 sc2
permutation rx1 wx rx3 s1 c2 at1 sc1 s2 at2 sc2
permutation rx1 wx rx3 c2 s1 at1 sc1 s2 at2 sc2
