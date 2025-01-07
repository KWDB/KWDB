# test select for update

setup
{
  CREATE TABLE t1 (
      id INT PRIMARY KEY,
      balance DECIMAL(10, 2) NOT NULL
  );
  CREATE INDEX idx_value ON t1(balance);


  INSERT INTO t1 (id, balance) VALUES (1, 1000.00);
  INSERT INTO t1 (id, balance) VALUES (2, 2000.00);
  INSERT INTO t1 (id, balance) VALUES (3, 3000.00);

}

teardown
{
  DROP TABLE t1;
}

session s1
step s1		{ BEGIN transaction isolation level read committed; }
step s1r	{ SELECT * FROM t1 WHERE id IN (1, 2) FOR UPDATE; }
step s1u	{ UPDATE t1 SET balance = balance + 150 WHERE id = 2; }
step s1c	{ COMMIT; }

session s2
step s2		{ BEGIN transaction isolation level read committed; }
step s2r	{ SELECT * FROM t1 WHERE id = 2 FOR UPDATE;; }
step s2u	{ UPDATE t1 SET balance = balance - 100 WHERE id = 2; }
step s2c	{ COMMIT; }

permutation s1 s2 s1r s1u s2r s1c s2u s2c