# UPDATE and DELETE behavior
setup
{
  CREATE TABLE IF NOT EXISTS kv(k int PRIMARY key, v int);
  delete from kv;
  insert into kv values (0, 5), (1, 5), (2, 5), (3, 5), (4, 1);
}

teardown
{
  drop table kv;
}

session s1
step s1   { BEGIN transaction isolation level read committed; }
step s1u1 { update kv set v = 100 where v >= 5; }
step s1r1 { select * from kv; }
step s1c  { COMMIT; }

session s2
step s2   { BEGIN transaction isolation level read committed; }
step s2w1 { INSERT INTO kv VALUES(5,5); }
step s2u1 { UPDATE kv SET v=10 WHERE k=4; }
step s2d1 { DELETE FROM kv WHERE k=3; }
step s2u2 { UPDATE kv SET v=10 WHERE k=2; }
step s2u3 { update kv set v=1 where k=1; }
step s2u4 { update kv set k=10 where k=0; }
step s2c  { COMMIT; }

permutation s1 s2 s2w1 s2u1 s2d1 s2u2 s2u3 s2u4 s1u1 s2c s1r1 s1c

# expected result after s2c, and before s1u1 takes effect:
#   k  | v
# -----+-----
#    1 |  1
#    2 | 10
#    4 | 10
#    5 |  5
#   10 |  5
# (5 rows)
