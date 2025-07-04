drop database if EXISTS procedure_test_db;
create database procedure_test_db;

use procedure_test_db;

CREATE TABLE employees(id int64 PRIMARY KEY, name VARCHAR(100), age INT, salary decimal(10,2));
create table test_t1(a int);

-- explicit transaction
-- commit
DROP PROCEDURE IF EXISTS process_txn_example1;
CREATE PROCEDURE process_txn_example1() BEGIN START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT * FROM employees; COMMIT; END;
call process_txn_example1();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example1;

-- rollback
DROP PROCEDURE IF EXISTS process_txn_example2;
CREATE PROCEDURE process_txn_example2() BEGIN START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', 111, 3.2); SELECT * FROM employees; ROLLBACK; END;
call process_txn_example2();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example2;

-- no start
DROP PROCEDURE IF EXISTS process_txn_example3;
CREATE PROCEDURE process_txn_example3() BEGIN INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', 111, 3.2); SELECT * FROM employees; COMMIT; END;
call process_txn_example3();
DROP PROCEDURE process_txn_example3;

-- no commit/rollback
DROP PROCEDURE IF EXISTS process_txn_example4;
CREATE PROCEDURE process_txn_example4() BEGIN START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (4, 'test111', 111, 3.2); SELECT * FROM employees; END;
call process_txn_example4();
DROP PROCEDURE process_txn_example4;

-- duplicate start
DROP PROCEDURE IF EXISTS process_txn_example5;
CREATE PROCEDURE process_txn_example5() BEGIN START TRANSACTION; START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (5, 'test111', 555, 5.2); SELECT * FROM employees; COMMIT; END;
call process_txn_example5();
DROP PROCEDURE process_txn_example5;

-- duplicate commit/rollback
DROP PROCEDURE IF EXISTS process_txn_example6;
CREATE PROCEDURE process_txn_example6() BEGIN START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (6, 'test111', 66, 3.2); SELECT * FROM employees; COMMIT; ROLLBACK; END;
call process_txn_example6();
DROP PROCEDURE process_txn_example6;

-- execute procedure in explicit transaction
DROP PROCEDURE IF EXISTS process_txn_example7;
CREATE PROCEDURE process_txn_example7() BEGIN START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (7, 'test111', 77, 3.2); SELECT * FROM employees; COMMIT; END;
START TRANSACTION;
call process_txn_example7();
COMMIT;
COMMIT;
DROP PROCEDURE process_txn_example7;

-- error rollback
DROP PROCEDURE IF EXISTS process_txn_example8;
CREATE PROCEDURE process_txn_example8() BEGIN START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (8, 'test111', 111, 3.2); INSERT INTO employees (id, name, age, salary) VALUES (8, 'test111', 111, 3.2); SELECT * FROM employees; COMMIT; END;
call process_txn_example8();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example8;

-- HANDLER commit
DROP PROCEDURE IF EXISTS process_txn_example9;
CREATE PROCEDURE process_txn_example9() BEGIN DECLARE err int DEFAULT 0; DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err = -1; COMMIT; ENDHANDLER; START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (9, 'test111', 111, 3.2); SELECT * FROM test_t1; SELECT err; END;
call process_txn_example9();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example9;

-- HANDLER rollback
create table test_t1(a int);
DROP PROCEDURE IF EXISTS process_txn_example10;
CREATE PROCEDURE process_txn_example10() BEGIN DECLARE err int DEFAULT 0; DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err = -2; ROLLBACK; ENDHANDLER; START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (10, 'test111', 111, 3.2); SELECT * FROM test_t1; SELECT err; END;
call process_txn_example10();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example10;

-- HANDLER error rollback
DROP PROCEDURE IF EXISTS process_txn_example11;
CREATE PROCEDURE process_txn_example11() BEGIN DECLARE err int DEFAULT 0; DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err=-1; INSERT INTO employees (id, name, age, salary) VALUES (11, 'test111', 111, 3.2); COMMIT; ENDHANDLER; START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (11, 'test111', 111, 3.2); SELECT * FROM test_t1; SELECT err; END;
call process_txn_example11();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example11;

-- duplicate transaction
DROP PROCEDURE IF EXISTS process_txn_example12;
CREATE PROCEDURE process_txn_example12() BEGIN DECLARE i int DEFAULT 0; START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (12, 'test111', 111, 3.2); WHILE i < 3 DO if i = 2 THEN START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (12, 'test111', 111, 3.2); COMMIT; ENDIF; SET i=i+1; ENDWHILE; COMMIT; END;
call process_txn_example12();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example12;

-- start a transaction within a WHILE loop
DROP PROCEDURE IF EXISTS process_txn_example13;
CREATE PROCEDURE process_txn_example13() BEGIN DECLARE i int DEFAULT 14; INSERT INTO employees (id, name, age, salary) VALUES (13, 'test111', 111, 3.2); WHILE i < 20 DO START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (i, 'test111', 111, 3.2); COMMIT; SET i=i+1; ENDWHILE; END;
call process_txn_example13();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_txn_example13;

-- HANDLER start transaction, main block commit
DROP PROCEDURE IF EXISTS process_txn_example14;
CREATE PROCEDURE process_txn_example14() BEGIN DECLARE err int DEFAULT 0; DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN START TRANSACTION; SET err = -1; INSERT INTO employees (id, name, age, salary) VALUES (15, 'test111', 111, 3.2); ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (14, 'test111', 111, 3.2); SELECT * FROM test_t1; SELECT err; COMMIT; END;
call process_txn_example14();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example14;

-- HANDLER start transaction, main block rollback
DROP PROCEDURE IF EXISTS process_txn_example15;
CREATE PROCEDURE process_txn_example15() BEGIN DECLARE err int DEFAULT 0; DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN START TRANSACTION; SET err = -1; INSERT INTO employees (id, name, age, salary) VALUES (17, 'test111', 111, 3.2); ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (16, 'test111', 111, 3.2); SELECT * FROM test_t1; SELECT err; ROLLBACK; END;
call process_txn_example15();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example15;

-- HANDLER start transaction, main block error rollback
DROP PROCEDURE IF EXISTS process_txn_example16;
CREATE PROCEDURE process_txn_example16() BEGIN DECLARE err int DEFAULT 0; DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN START TRANSACTION; SET err = -1; INSERT INTO employees (id, name, age, salary) VALUES (18, 'test111', 111, 3.2); ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (18, 'test111', 111, 3.2); SELECT * FROM test_t1; SELECT err; COMMIT; END;
call process_txn_example16();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example16;

-- main block start transaction，commit in IF
DROP PROCEDURE IF EXISTS process_txn_example17;
CREATE PROCEDURE process_txn_example17() BEGIN DECLARE err int DEFAULT 0; START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (19, 'test111', 111, 3.2); IF err=0 THEN COMMIT; ENDIF; END;
call process_txn_example17();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example17;

--  main block start transaction，rollback in IF
DROP PROCEDURE IF EXISTS process_txn_example18;
CREATE PROCEDURE process_txn_example18() BEGIN DECLARE err int DEFAULT 0; START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (20, 'test111', 111, 3.2); IF err=0 THEN ROLLBACK; ENDIF; END;
call process_txn_example18();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example18;


--  main block start transaction，error rollback in IF
DROP PROCEDURE IF EXISTS process_txn_example19;
CREATE PROCEDURE process_txn_example19() BEGIN DECLARE err int DEFAULT 0; START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (21, 'test111', 111, 3.2); IF err=0 THEN INSERT INTO employees (id, name, age, salary) VALUES (21, 'test111', 111, 3.2); ENDIF; COMMIT; END;
call process_txn_example19();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example19;


-- IF start transaction，commit in main block
DROP PROCEDURE IF EXISTS process_txn_example20;
CREATE PROCEDURE process_txn_example20() BEGIN DECLARE err int DEFAULT 0; IF err=0 THEN START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (22, 'test111', 111, 3.2); ENDIF; COMMIT; END;
call process_txn_example20();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example20;


-- IF start transaction，rollback in main block
DROP PROCEDURE IF EXISTS process_txn_example21;
CREATE PROCEDURE process_txn_example21() BEGIN DECLARE err int DEFAULT 0; IF err=0 THEN START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (23, 'test111', 111, 3.2); ENDIF; ROLLBACK; END;
call process_txn_example21();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example21;


-- IF start transaction，error rollback in main block
DROP PROCEDURE IF EXISTS process_txn_example22;
CREATE PROCEDURE process_txn_example22() BEGIN DECLARE err int DEFAULT 0; IF err=0 THEN START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (24, 'test111', 111, 3.2); ENDIF; INSERT INTO employees (id, name, age, salary) VALUES (24, 'test111', 111, 3.2); COMMIT; END;
call process_txn_example22();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example22;


-- explicit transaction do not affect custom variables
DROP PROCEDURE IF EXISTS process_txn_example23;
CREATE PROCEDURE process_txn_example23() BEGIN DECLARE err int DEFAULT 0; START TRANSACTION; INSERT INTO employees (id, name, age, salary) VALUES (25, 'test111', 111, 3.2); SET err=1; ROLLBACK; select err; END;
call process_txn_example23();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example23;


--- implicit transaction
DROP PROCEDURE IF EXISTS process_txn_example24;
CREATE PROCEDURE process_txn_example24() BEGIN DECLARE i INT DEFAULT 26; WHILE i < 30 DO INSERT INTO employees (id, name, age, salary) VALUES (i, 'test111', 111, 3.2); SET i=i+1; ENDWHILE; INSERT INTO employees (id, name, age, salary) VALUES (26, 'test111', 111, 3.2); END;
call process_txn_example24();
SELECT * FROM employees;
DROP PROCEDURE process_txn_example24;

create table t1(a int, b int);
create table t2(a int, b int, c int);
insert into t1 values (1, 2), (3, 4);
insert into t2 values (1, 2, 3), (4, 5, 6);
create table t3(a int, b int, c int);

DROP PROCEDURE IF EXISTS txn_test;
create procedure txn_test() label test: begin declare a int default 0; declare b int default 0; declare err int default 0; declare exit HANDLER FOR NOT FOUND,SQLEXCEPTION BEGIN SET err = -1; SELECT a,b; ROLLBACK; ENDHANDLER; START TRANSACTION; SET a = 10; select a, b FROM t1; UPDATE t1 SET a =  a + 1 WHERE b > 0; insert into t1 values (a, b); label my_loop: WHILE b <= 10 DO declare d int; SET d = b + 2; if d > 9 then select * FROM t1; leave my_loop; elsif b > 5 then select * FROM t2; endif; SET b = b + 1; ENDWHILE; IF err = 0 THEN SELECT a,b; COMMIT; ENDIF; end;
call txn_test();
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE txn_test;

-- HANDLER
-- NOT FOUND CONTINUE WITH SQL
DROP PROCEDURE IF EXISTS process_handler_example1;
CREATE PROCEDURE process_handler_example1()
BEGIN DECLARE err int DEFAULT 0; DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err = -1; ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT * FROM employees; SELECT * FROM test_t1; SELECT err; END;
call process_handler_example1();
DROP PROCEDURE process_handler_example1;

-- NOT FOUND EXIT WITH SQL
DROP PROCEDURE IF EXISTS process_handler_example2;
CREATE PROCEDURE process_handler_example2()
BEGIN DECLARE err int DEFAULT 0; DECLARE EXIT HANDLER FOR NOT FOUND BEGIN SET err = -1; ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', 111, 3.2); SELECT * FROM employees;SELECT * FROM test_t1; SELECT err; END;
call process_handler_example2();
DROP PROCEDURE process_handler_example2;


-- SQLEXCEPTION CONTINUE WITH SQL
DROP PROCEDURE IF EXISTS process_handler_example3;
CREATE PROCEDURE process_handler_example3()
BEGIN DECLARE err int DEFAULT 0; DECLARE i INT DEFAULT 3;DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET err = -1; INSERT INTO employees (id, name, age, salary) VALUES (5, 'test111', 111, 3.2); WHILE i < 8 DO INSERT INTO employees (id, name, age, salary) VALUES (i, 'test111', 111, 3.2); SET i=i+1; ENDWHILE; SELECT * FROM employees; SELECT err; END;
call process_handler_example3();
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example3;


-- SQLEXCEPTION EXIT WITH SQL
DROP PROCEDURE IF EXISTS process_handler_example4;
CREATE PROCEDURE process_handler_example4()
BEGIN DECLARE err int DEFAULT 0; DECLARE i INT DEFAULT 3; DECLARE EXIT HANDLER FOR SQLEXCEPTION SET err = -1; INSERT INTO employees (id, name, age, salary) VALUES (5, 'test111', 111, 3.2); WHILE i < 8 DO INSERT INTO employees (id, name, age, salary) VALUES (i, 'test111', 111, 3.2); SET i=i+1; ENDWHILE; SELECT * FROM employees; SELECT err; END;
call process_handler_example4();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example4;


-- NOT FOUND AND SQLEXCEPTION CONTINUE WITH SQL
DROP PROCEDURE IF EXISTS process_handler_example5;
CREATE PROCEDURE process_handler_example5()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3; DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); ENDHANDLER; DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (7, 'test111', err2, 3.2); ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT * FROM test_t1; SELECT err1; INSERT INTO employees (id, name, age, salary) VALUES (5, 'test111', 5, 3.2); WHILE i <= 6 DO INSERT INTO employees (id, name, age, salary) VALUES (i, 'test111', 111, 3.2); SET i=i+1; ENDWHILE; SELECT * FROM employees; SELECT err2; END;
call process_handler_example5();
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example5;

DROP PROCEDURE IF EXISTS process_handler_example6;
CREATE PROCEDURE process_handler_example6()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3; DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (7, 'test111', err2, 3.2); ENDHANDLER; DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT * FROM test_t1; SELECT err1; INSERT INTO employees (id, name, age, salary) VALUES (5, 'test111', 5, 3.2); INSERT INTO employees (id, name, age, salary) VALUES (5, 'test111', 111, 3.2); SELECT * FROM employees; SELECT err2; END;
call process_handler_example6();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example6;


-- NOT FOUND AND SQLEXCEPTION EXIT WITH SQL
DROP PROCEDURE IF EXISTS process_handler_example7;
CREATE PROCEDURE process_handler_example7()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3; DECLARE EXIT HANDLER FOR NOT FOUND BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); ENDHANDLER; DECLARE EXIT HANDLER FOR SQLEXCEPTION  BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (7, 'test111', err2, 3.2); ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT * FROM test_t1; SELECT err1; INSERT INTO employees (id, name, age, salary) VALUES (5, 'test111', 5, 3.2); WHILE i <= 6 DO INSERT INTO employees (id, name, age, salary) VALUES (i, 'test111', 111, 3.2); SET i=i+1; ENDWHILE; SELECT * FROM employees; SELECT err2; END;
call process_handler_example7();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example7;


DROP PROCEDURE IF EXISTS process_handler_example8;
CREATE PROCEDURE process_handler_example8()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (7, 'test111', err2, 3.2); ENDHANDLER; DECLARE EXIT HANDLER FOR NOT FOUND  BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT * FROM test_t1; SELECT err1; SELECT * FROM employees; SELECT err2; END;
call process_handler_example8();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example8;


-- NOT FOUND CONTINUE AND EXIT WITH SQL
DROP PROCEDURE IF EXISTS process_handler_example9;
CREATE PROCEDURE process_handler_example9()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3; DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; DECLARE EXIT HANDLER FOR NOT FOUND BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (7, 'test111', err2, 3.2); SELECT err2; ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT * FROM test_t1; SELECT * FROM employees; END;
call process_handler_example9();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example9;


-- SQLEXCEPTION CONTINUE AND EXIT WITH SQL
DROP PROCEDURE IF EXISTS process_handler_example10;
CREATE PROCEDURE process_handler_example10()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3; DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (7, 'test111', err2, 3.2); SELECT err2; ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT * FROM employees; END;
call process_handler_example10();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example10;


-- SQLEXCEPTION CONTINUE AND NOT FOUND EXIT WITH SQL
DROP PROCEDURE IF EXISTS process_handler_example11;
CREATE PROCEDURE process_handler_example11()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3; DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; DECLARE EXIT HANDLER FOR NOT FOUND BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', err2, 3.2); SELECT err2; ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT * FROM test_t1; SELECT 'procedure end'; END;
call process_handler_example11();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example11;


-- SQLEXCEPTION CONTINUE AND NOT FOUND EXIT WITH SQL
DROP PROCEDURE IF EXISTS process_handler_example12;
CREATE PROCEDURE process_handler_example12()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', err2, 3.2); SELECT err2; ENDHANDLER; SELECT * FROM test_t1; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT 'procedure end'; END;
call process_handler_example12();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example12;


-- declare handler in IF，exec handler in main block
DROP PROCEDURE IF EXISTS process_handler_example13;
CREATE PROCEDURE process_handler_example13()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3; IF i > 0 THEN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', err2, 3.2); SELECT err2; ENDHANDLER; ENDIF; SELECT * FROM test_t1; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT 'procedure end'; END;
call process_handler_example13();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example13;


-- declare handler in IF，exec handler in IF
DROP PROCEDURE IF EXISTS process_handler_example14;
CREATE PROCEDURE process_handler_example14()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3; IF i > 0 THEN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', err2, 3.2); SELECT err2; ENDHANDLER; SELECT * FROM test_t1; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT 'procedure end'; ENDIF; END;
call process_handler_example14();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example14;


-- declare handler in main block，exec handler in IF
DROP PROCEDURE IF EXISTS process_handler_example15;
CREATE PROCEDURE process_handler_example15()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', err2, 3.2); SELECT err2; ENDHANDLER; IF i > 0 THEN SELECT * FROM test_t1; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT 'procedure end'; ENDIF; END;
call process_handler_example15();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example15;


-- declare handler in main block and IF, NOT FOUND CONTINUE AND EXIT WITH SQL
DROP PROCEDURE IF EXISTS process_handler_example16;
CREATE PROCEDURE process_handler_example16()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3; DECLARE EXIT HANDLER FOR NOT FOUND BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', err2, 3.2); SELECT err2; ENDHANDLER; IF i > 0 THEN DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; SELECT * FROM test_t1; ENDIF; END;
call process_handler_example16();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example16;

-- declare handler in main block and IF，SQLEXCEPTION CONTINUE AND EXIT WITH SQL
DROP PROCEDURE IF EXISTS process_handler_example17;
CREATE PROCEDURE process_handler_example17()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', err2, 3.2); SELECT err2; ENDHANDLER; IF i > 0 THEN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); ENDIF; END;
call process_handler_example17();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example17;


-- declare handler in main block and IF，EXIT WITH SQL
DROP PROCEDURE IF EXISTS process_handler_example18;
CREATE PROCEDURE process_handler_example18()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', err2, 3.2); SELECT err2; ENDHANDLER; IF i > 0 THEN DECLARE EXIT HANDLER FOR NOT FOUND BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; SELECT * FROM test_t1; ENDIF; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); END;
call process_handler_example18();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example18;


-- declare handler in main block and IF，CONTINUE WITH SQL
DROP PROCEDURE IF EXISTS process_handler_example19;

CREATE PROCEDURE process_handler_example19()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3; DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', err2, 3.2); SELECT err2; ENDHANDLER; IF i > 0 THEN DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; SELECT * FROM test_t1; SELECT 'IF END'; ENDIF; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT * FROM employees; END;
call process_handler_example19();
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example19;


-- declare handler in main block and IF，two NOT FOUND CONTINUE WITH SQL
DROP PROCEDURE IF EXISTS process_handler_example20;
CREATE PROCEDURE process_handler_example20()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3;DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', err2, 3.2); SELECT err2; ENDHANDLER; IF i > 0 THEN DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; SELECT * FROM test_t1; SELECT 'IF END'; ENDIF; SELECT * FROM test_t1; SELECT * FROM employees; END;
call process_handler_example20();
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example20;


-- declare handler in main block and IF，two SQLEXCEPTION CONTINUE WITH SQL
DROP PROCEDURE IF EXISTS process_handler_example21;
CREATE PROCEDURE process_handler_example21()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3;DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', err2, 3.2); SELECT err2; ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); IF i > 0 THEN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT 'IF END'; ENDIF; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT * FROM employees; END;
call process_handler_example21();
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example21;


-- CONTINUE HANDLER contains CONTINUE HANDLER
DROP PROCEDURE IF EXISTS process_handler_example22;
CREATE PROCEDURE process_handler_example22()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3;DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; SET err2 = -2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', err2, 3.2); SELECT * FROM test_t1; SELECT err2; ENDHANDLER; SELECT * FROM test_t1; SELECT * FROM employees; END;
call process_handler_example22();
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example22;

-- CONTINUE HANDLER contains EXIT HANDLER
DROP PROCEDURE IF EXISTS process_handler_example23;
CREATE PROCEDURE process_handler_example23()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3;DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN DECLARE EXIT HANDLER FOR NOT FOUND BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; SET err2 = -2; SELECT * FROM test_t1; SELECT err2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', err2, 3.2); ENDHANDLER; SELECT * FROM test_t1; SELECT * FROM employees; END;
call process_handler_example23();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example23;


-- EXIT HANDLER contains EXIT HANDLER
DROP PROCEDURE IF EXISTS process_handler_example24;
CREATE PROCEDURE process_handler_example24()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3;DECLARE EXIT HANDLER FOR NOT FOUND BEGIN DECLARE EXIT HANDLER FOR NOT FOUND BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; SET err2 = -2; SELECT * FROM test_t1; SELECT err2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', err2, 3.2); ENDHANDLER; SELECT * FROM test_t1; SELECT * FROM employees; END;
call process_handler_example24();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example24;


-- EXIT HANDLER contains CONTINUE HANDLER
DROP PROCEDURE IF EXISTS process_handler_example25;
CREATE PROCEDURE process_handler_example25()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 3;DECLARE EXIT HANDLER FOR NOT FOUND BEGIN DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET err1 = -1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', err1, 3.2); SELECT err1; ENDHANDLER; SET err2 = -2; SELECT * FROM test_t1; SELECT err2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', err2, 3.2); ENDHANDLER; SELECT * FROM test_t1; SELECT * FROM employees; END;
call process_handler_example25();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example25;

-- CONTINUE HANDLER complex
DROP PROCEDURE IF EXISTS process_handler_example26;
CREATE PROCEDURE process_handler_example26()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 2;DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN SET err2 = -2; SELECT err2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', 122, 3.2); UPDATE employees SET age = err2 WHERE id = 3;  DELETE FROM employees WHERE id = 3; LABEL my_loop: WHILE i > 0 DO IF i = 6 THEN LEAVE my_loop; ENDIF; INSERT INTO employees (id, name, age, salary) VALUES (i, 'test111', i+10, 3.2); SET i=i+1; ENDWHILE; SELECT * FROM employees; ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT 'procedure end'; END;
call process_handler_example26();
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example26;


-- EXIT HANDLER complex
DROP PROCEDURE IF EXISTS process_handler_example27;
CREATE PROCEDURE process_handler_example27()
BEGIN DECLARE err1 int DEFAULT 0; DECLARE err2 int DEFAULT 0; DECLARE i INT DEFAULT 2;DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN SET err2 = -2; SELECT err2; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', 122, 3.2); UPDATE employees SET age = err2 WHERE id = 3;  DELETE FROM employees WHERE id = 3; LABEL my_loop: WHILE i > 0 DO IF i = 6 THEN LEAVE my_loop; ENDIF; INSERT INTO employees (id, name, age, salary) VALUES (i, 'test111', i+10, 3.2); SET i=i+1; ENDWHILE; SELECT * FROM employees; ENDHANDLER; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 111, 3.2); SELECT 'procedure end'; END;
call process_handler_example27();
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example27;


insert into test_t1 values(2);
insert into test_t1 values(1);
insert into test_t1 values(0);
-- DELETE FROM test_t1 WHERE 1=1;

-- NOUNT FOUND CONTINUE WITH CURSOR
DROP PROCEDURE IF EXISTS process_handler_example28;
CREATE PROCEDURE process_handler_example28()
BEGIN DECLARE done int DEFAULT 0; DECLARE var_a int DEFAULT 1; DECLARE CONTINUE HANDLER FOR NOT FOUND  BEGIN SET done = 1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', 122, 3.2); ENDHANDLER;DECLARE cur CURSOR FOR SELECT * FROM test_t1; OPEN cur; LABEL read_loop: WHILE var_a >= 0 DO IF done=1 THEN LEAVE read_loop; ENDIF; FETCH cur INTO var_a; SELECT var_a; ENDWHILE read_loop; CLOSE cur; END;
call process_handler_example28();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example28;

-- NOUNT FOUND EXIT WITH CURSOR
DROP PROCEDURE IF EXISTS process_handler_example29;

CREATE PROCEDURE process_handler_example29()
BEGIN DECLARE done int DEFAULT 0; DECLARE var_a int DEFAULT 1; DECLARE EXIT HANDLER FOR NOT FOUND  BEGIN SET done = 1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', 122, 3.2); ENDHANDLER;DECLARE cur CURSOR FOR SELECT * FROM test_t1; OPEN cur; LABEL read_loop: WHILE var_a >= 0 DO IF done=1 THEN LEAVE read_loop; ENDIF; FETCH cur INTO var_a; SELECT var_a; ENDWHILE read_loop; CLOSE cur; END;
call process_handler_example29();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example29;


-- NOUNT FOUND CURSOR WITH CURSOR IN IF
DROP PROCEDURE IF EXISTS process_handler_example31;

CREATE PROCEDURE process_handler_example31()
BEGIN DECLARE done int DEFAULT 0; DECLARE var_a int DEFAULT 1; DECLARE i int DEFAULT 1; DECLARE cur CURSOR FOR SELECT * FROM test_t1; IF i > 0 THEN DECLARE CONTINUE HANDLER FOR NOT FOUND  BEGIN SET done = 1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', 122, 3.2); ENDHANDLER; ENDIF; OPEN cur; LABEL read_loop: WHILE var_a >= 0 DO IF done=1 THEN LEAVE read_loop; ENDIF; FETCH cur INTO var_a; SELECT var_a; ENDWHILE read_loop; CLOSE cur; END;
call process_handler_example31();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example31;

DROP PROCEDURE IF EXISTS process_handler_example32;
CREATE PROCEDURE process_handler_example32()
BEGIN DECLARE done int DEFAULT 0; DECLARE var_a int DEFAULT 1; DECLARE i int DEFAULT 1; DECLARE cur CURSOR FOR SELECT * FROM test_t1; IF i > 0 THEN DECLARE CONTINUE HANDLER FOR NOT FOUND  BEGIN SET done = 1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', 122, 3.2); ENDHANDLER; OPEN cur; LABEL read_loop: WHILE var_a >= 0 DO IF done=1 THEN LEAVE read_loop; ENDIF; FETCH cur INTO var_a; SELECT var_a; ENDWHILE read_loop; CLOSE cur; ENDIF; END;
call process_handler_example32();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example32;


-- NOUNT FOUND CURSOR WITH CURSOR IN If AND Main Block
DROP PROCEDURE IF EXISTS process_handler_example33;
CREATE PROCEDURE process_handler_example33()
BEGIN DECLARE done int DEFAULT 0; DECLARE var_a int DEFAULT 1; DECLARE i int DEFAULT 1; DECLARE CONTINUE HANDLER FOR NOT FOUND  BEGIN SET done = 1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', 122, 3.2); ENDHANDLER;DECLARE cur CURSOR FOR SELECT * FROM test_t1; IF i > 0 THEN DECLARE CONTINUE HANDLER FOR NOT FOUND  BEGIN SET done = 1; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', 122, 3.2); ENDHANDLER; OPEN cur; LABEL read_loop: WHILE var_a >= 0 DO IF done=1 THEN LEAVE read_loop; ENDIF; FETCH cur INTO var_a; SELECT var_a; ENDWHILE read_loop; CLOSE cur; ENDIF; END;
call process_handler_example33();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example33;

DROP PROCEDURE IF EXISTS process_handler_example34;
CREATE PROCEDURE process_handler_example34()
BEGIN DECLARE done int DEFAULT 0; DECLARE var_a int DEFAULT 1; DECLARE i int DEFAULT 1; DECLARE CONTINUE HANDLER FOR NOT FOUND  BEGIN SET done = 1; INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', 122, 3.2); ENDHANDLER; IF i > 0 THEN DECLARE CONTINUE HANDLER FOR NOT FOUND  BEGIN SET done = 1; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', 122, 3.2); ENDHANDLER; ENDIF;DECLARE cur CURSOR FOR SELECT * FROM test_t1; OPEN cur; LABEL read_loop: WHILE var_a >= 0 DO IF done=1 THEN LEAVE read_loop; ENDIF; FETCH cur INTO var_a; SELECT var_a; ENDWHILE read_loop; CLOSE cur; END;
call process_handler_example34();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example34;

-- Define a HANDLER inside another HANDLER.
DROP PROCEDURE IF EXISTS process_handler_example35;
CREATE PROCEDURE process_handler_example35()
BEGIN DECLARE done int DEFAULT 0; DECLARE var_a int DEFAULT 1; DECLARE i int DEFAULT 1; DECLARE CONTINUE HANDLER FOR NOT FOUND  BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION  BEGIN INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', 122, 3.2); ENDHANDLER; SET done = 1; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 122, 3.2); INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 122, 3.2); ENDHANDLER;DECLARE cur CURSOR FOR SELECT * FROM test_t1; OPEN cur; LABEL read_loop: WHILE var_a >= 0 DO IF done=1 THEN LEAVE read_loop; ENDIF; FETCH cur INTO var_a; SELECT var_a; ENDWHILE read_loop; CLOSE cur; END;
call process_handler_example35();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example35;

DROP PROCEDURE IF EXISTS process_handler_example36;
CREATE PROCEDURE process_handler_example36()
BEGIN DECLARE done int DEFAULT 0; DECLARE var_a int DEFAULT 1; DECLARE i int DEFAULT 1; DECLARE CONTINUE HANDLER FOR NOT FOUND  BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION  BEGIN INSERT INTO employees (id, name, age, salary) VALUES (2, 'test111', 122, 3.2); ENDHANDLER; SET done = 1; INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 122, 3.2); INSERT INTO employees (id, name, age, salary) VALUES (1, 'test111', 122, 3.2); ENDHANDLER;DECLARE cur CURSOR FOR SELECT * FROM test_t1; OPEN cur; LABEL read_loop: WHILE var_a >= 0 DO IF done=1 THEN LEAVE read_loop; ENDIF; FETCH cur INTO var_a; SELECT var_a; ENDWHILE read_loop; CLOSE cur; END;
call process_handler_example36();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example36;

-- handler contains complex instructions
DROP PROCEDURE IF EXISTS process_handler_example37;
CREATE PROCEDURE process_handler_example37()
BEGIN DECLARE done int DEFAULT 0; DECLARE var_a int DEFAULT 1; DECLARE i int DEFAULT 1; DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN SET done = 1; INSERT INTO employees (id, name, age, salary) VALUES (3, 'test111', 122, 3.2); UPDATE employees SET age = id+3 WHERE id = 3;  DELETE FROM employees WHERE id = 3; LABEL my_loop: WHILE i > 0 DO IF i = 6 THEN LEAVE my_loop; ENDIF; INSERT INTO employees (id, name, age, salary) VALUES (i, 'test111', i+10, 3.2); SET i=i+1; ENDWHILE; SELECT * FROM employees; ENDHANDLER; DECLARE cur CURSOR FOR SELECT * FROM test_t1; OPEN cur; LABEL read_loop: WHILE var_a >= 0 DO IF done=1 THEN LEAVE read_loop; ENDIF; FETCH cur INTO var_a; select var_a; ENDWHILE read_loop; CLOSE cur; END;
call process_handler_example37();
SELECT * FROM employees;
DELETE FROM employees WHERE 1=1;
DROP PROCEDURE process_handler_example37;

-- clear
use defaultdb;
drop database procedure_test_db cascade;

