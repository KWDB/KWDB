drop database if exists procedure_test;
create database procedure_test;
use procedure_test;
create table t1(a int, b int);
create table t2(a int, b int, c int);
insert into t1 values (1, 2), (3, 4);
insert into t2 values (1, 2, 3), (4, 5, 6);
create table t3(a int, b int, c int);
insert into t3 values (7, 8, 9);

create procedure test_dec1() begin declare a int; select a; end;
call test_dec1();
call test_dec1();
drop procedure test_dec1;

create procedure test_dec2() begin declare a int; declare a varchar; end;
call test_dec2();
call test_dec2();
drop procedure test_dec2;

create procedure test_dec3() begin declare a int default 5; select a;end;
call test_dec3();
call test_dec3();
drop procedure test_dec3;

create procedure test_dec4() begin declare x int;declare continue handler for not found set x=1;declare cur cursor for select 1;end;
call test_dec4();
call test_dec4();
drop procedure test_dec4;

CREATE PROCEDURE test_dec5() BEGIN DECLARE x INT;SELECT x;END;
CALL test_dec5();
CALL test_dec5();
drop procedure test_dec5;

CREATE PROCEDURE test_dec6() BEGIN DECLARE x INT;SELECT y;END;
CALL test_dec6();
CALL test_dec6();
drop procedure test_dec6;

CREATE PROCEDURE test_dec7() BEGIN DECLARE x INT;DECLARE cur CURSOR FOR SELECT a FROM t1;DECLARE y INT;END;
CALL test_dec7();
CALL test_dec7();
drop procedure test_dec7;

CREATE PROCEDURE test_dec8()BEGIN DECLARE a INT DEFAULT 5;SET @a = 10;SELECT a, @a; END;
CALL test_dec8();
CALL test_dec8();
drop procedure test_dec8;

CREATE PROCEDURE test_dec9()BEGIN DECLARE x INVALID_TYPE;END;
CALL test_dec9();
CALL test_dec9();
drop procedure test_dec9;

CREATE PROCEDURE test_dec10()BEGIN DECLARE cur CURSOR FOR SELECT a FROM t1;OPEN cur;CLOSE cur;OPEN cur;CLOSE cur;END;
CALL test_dec10();
CALL test_dec10();
drop procedure test_dec10;

CREATE PROCEDURE test_dec11()BEGIN DECLARE ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP;DECLARE dec_val DECIMAL(10,2) DEFAULT 123.45;SELECT dec_val;END;
CALL test_dec11();
CALL test_dec11();
drop procedure test_dec11;

CREATE PROCEDURE test_dec12()BEGIN DECLARE x INT;DECLARE x VARCHAR(10);END;
CALL test_dec12();
CALL test_dec12();
DROP PROCEDURE test_dec12;

CREATE PROCEDURE test_dec13() BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION SELECT 'Handler1';DECLARE EXIT HANDLER FOR SQLEXCEPTION SELECT 'Handler2';END;
CALL test_dec13();
CALL test_dec13();
DROP PROCEDURE test_dec13;

CREATE PROCEDURE test_dec14()BEGIN DECLARE x INT DEFAULT (5+3)*2;SELECT x;END;
CALL test_dec14();
CALL test_dec14();
DROP PROCEDURE test_dec14;

CREATE PROCEDURE test_dec15() BEGIN DECLARE x INT DEFAULT 'abc';END;
CALL test_dec15();
CALL test_dec15();
DROP PROCEDURE test_dec15;

CREATE PROCEDURE test_dec16()BEGIN DECLARE x INT2 DEFAULT (5+3)*2;SELECT x;END;
CALL test_dec16();
CALL test_dec16();
DROP PROCEDURE test_dec16;

CREATE PROCEDURE test_dec17()BEGIN DECLARE x INT4 DEFAULT (5+3)*2;SELECT x;END;
CALL test_dec17();
CALL test_dec17();
DROP PROCEDURE test_dec17;

CREATE PROCEDURE test_dec18()BEGIN DECLARE x INT8 DEFAULT (5+3)*2;SELECT x;END;
CALL test_dec18();
CALL test_dec18();
DROP PROCEDURE test_dec18;

CREATE PROCEDURE test_dec19()BEGIN DECLARE x float4 DEFAULT (5+3)*2;SELECT x;END;
CALL test_dec19();
CALL test_dec19();
DROP PROCEDURE test_dec19;

CREATE PROCEDURE test_dec20()BEGIN DECLARE x float8 DEFAULT (5+3)*2;SELECT x;END;
CALL test_dec20();
CALL test_dec20();
DROP PROCEDURE test_dec20;

CREATE PROCEDURE test_dec21()BEGIN DECLARE x string DEFAULT 'test_procedure';SELECT x;END;
CALL test_dec21();
CALL test_dec21();
DROP PROCEDURE test_dec21;

CREATE PROCEDURE test_dec22()BEGIN DECLARE x text DEFAULT 'test_procedure';SELECT x;END;
CALL test_dec22();
CALL test_dec22();
DROP PROCEDURE test_dec22;

CREATE PROCEDURE test_dec23()BEGIN DECLARE x char DEFAULT 'test_procedure';SELECT x;END;
CALL test_dec23();
CALL test_dec23();
DROP PROCEDURE test_dec23;

CREATE PROCEDURE test_dec24()BEGIN DECLARE x varchar(64) DEFAULT 'test_procedure';SELECT x;END;
CALL test_dec24();
CALL test_dec24();
DROP PROCEDURE test_dec24;

CREATE PROCEDURE test_dec25()BEGIN DECLARE x timestamp(6) DEFAULT '2000-1-1 10:00:00.000001';SELECT x;END;
CALL test_dec25();
CALL test_dec25();
DROP PROCEDURE test_dec25;

CREATE PROCEDURE test_dec26()BEGIN DECLARE x timestamp(3) DEFAULT '2000-1-1 10:00:00.000001';SELECT x;END;
CALL test_dec26();
CALL test_dec26();
DROP PROCEDURE test_dec26;

CREATE PROCEDURE test_dec27()BEGIN DECLARE x timestamp(9) DEFAULT '2000-1-1 10:00:00.000001';SELECT x;END;
CALL test_dec27();
CALL test_dec27();
DROP PROCEDURE test_dec27;

CREATE PROCEDURE test_dec28()BEGIN DECLARE x timestamp(3);SELECT x;END;
CALL test_dec28();
CALL test_dec28();
DROP PROCEDURE test_dec28;

CREATE PROCEDURE test_dec29()BEGIN DECLARE x timestamp(6);SELECT x;END;
CALL test_dec29();
CALL test_dec29();
DROP PROCEDURE test_dec29;

CREATE PROCEDURE test_dec30()BEGIN DECLARE x timestamp(9);SELECT x;END;
CALL test_dec30();
CALL test_dec30();
DROP PROCEDURE test_dec30;

create procedure test_cur1() begin declare cur cursor for select * from t1;open cur;close cur;end;
call test_cur1();
call test_cur1();
drop procedure test_cur1;

create procedure test_cur2() begin open cur;end;
call test_cur2();
call test_cur2();
drop procedure test_cur2;

create procedure test_cur3() begin declare a int;declare cur cursor for select a,b from t1;open cur;fetch cur into a;close cur;end;
call test_cur3();
call test_cur3();
drop procedure test_cur3;

create procedure test_cur4() begin declare a int;declare b int;declare cur cursor for select a,b from t1;open cur;fetch cur into a,b;close cur;select a,b;end;
call test_cur4();
call test_cur4();
drop procedure test_cur4;

create procedure test_cur4_2() begin declare a int;declare b int;declare cur cursor for select a,b from t3;open cur;fetch cur into a,b;close cur;select a,b;end;
call test_cur4_2();
call test_cur4_2();
drop procedure test_cur4_2;

create procedure test_cur5()begin declare a int;declare cur cursor for select a from t1;fetch cur into a;end;
call test_cur5();
call test_cur5();
drop procedure test_cur5;

create procedure test_cur6()begin declare cur cursor for select 1;open cur;close cur;close cur;end;
call test_cur6();
call test_cur6();
drop procedure test_cur6;

create procedure test_cur7()begin declare done int default 1;declare a int;declare b int;declare cur cursor for select * from t1;declare continue handler for not found set done=1;declare flag int; set flag = 1;open cur;LABEL my_loop: WHILE flag > 0 DO fetch cur into a,b;if done!=0 then select a,b; endif;if a=3 then set flag = -1; endif;ENDWHILE my_loop;close cur;end;
call test_cur7();
call test_cur7();
drop procedure test_cur7;

create procedure test_cur8()begin declare done1 int default 0;declare done2 int default 0;declare a int;declare b int;declare cur1 cursor for select * from t1;declare cur2 cursor for select * from t2;declare continue handler for not found set done1=1;open cur1;open cur2;fetch cur1 into a,b;fetch cur2 into a,b,b;close cur1;close cur2;end;
call test_cur8();
call test_cur8();
drop procedure test_cur8;

create procedure test_cur9(val int)begin declare a int;declare cur cursor for select a from t1 where a > val;open cur;fetch cur into a;select a;close cur;end;
call test_cur9(0);
call test_cur9(0);
drop procedure test_cur9;

create procedure test_cur10()begin declare sql varchar(100);set sql = 'select * from t1';declare cur cursor for sql;end;
call test_cur10();

CREATE PROCEDURE test_cur11() BEGIN CLOSE non_exist_cur;END;
CALL test_cur11();
CALL test_cur11();
DROP PROCEDURE test_cur11;

CREATE PROCEDURE test_cur12()BEGIN DECLARE a INT;DECLARE cur CURSOR FOR SELECT a,b FROM t1;OPEN cur;FETCH cur INTO a;  CLOSE cur;END;
CALL test_cur12();
CALL test_cur12();
DROP PROCEDURE test_cur12;

CREATE PROCEDURE test_cur13()BEGIN DECLARE a INT;DECLARE cur CURSOR FOR SELECT a FROM t1;FETCH cur INTO a;END;
CALL test_cur13();
CALL test_cur13();
DROP PROCEDURE test_cur13;

CREATE PROCEDURE test_cur14()BEGIN DECLARE a INT;DECLARE cur CURSOR FOR SELECT a FROM t1;OPEN cur;FETCH cur INTO a;END;
CALL test_cur14();
CALL test_cur14();
DROP PROCEDURE test_cur14;

CREATE PROCEDURE test_cur15()BEGIN DECLARE done INT DEFAULT 0;DECLARE cur CURSOR FOR SELECT a FROM t1;DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;OPEN cur;FETCH cur INTO done;CLOSE cur;END;
CALL test_cur15();
CALL test_cur15();
DROP PROCEDURE test_cur15;

CREATE PROCEDURE test_cur16()BEGIN SET @sql = 'SELECT a FROM t1';DECLARE cur CURSOR FOR @sql;END;
CALL test_cur16();
CALL test_cur16();
DROP PROCEDURE test_cur16;

CREATE PROCEDURE test_cur17()BEGIN DECLARE cur CURSOR FOR SELECT 1;SET @cur = 5;OPEN cur;CLOSE cur;END;
CALL test_cur17();
CALL test_cur17();
DROP PROCEDURE test_cur17;

CREATE PROCEDURE test_cur18()BEGIN DECLARE done INT DEFAULT 0;DECLARE a INT;DECLARE cur CURSOR FOR SELECT a FROM t1;DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;OPEN cur;FETCH cur INTO a;select a;FETCH cur INTO a;select a;FETCH cur INTO a;select a;FETCH cur INTO a;select a;CLOSE cur;END;
CALL test_cur18();
CALL test_cur18();
DROP PROCEDURE test_cur18;

CREATE PROCEDURE test_cur19() BEGIN DECLARE a INT;DECLARE b INT;DECLARE c INT;DECLARE cur CURSOR FOR SELECT * FROM t2;OPEN cur;FETCH cur INTO a,b;CLOSE cur;END;
CALL test_cur19();
CALL test_cur19();
DROP PROCEDURE test_cur19;

CREATE PROCEDURE test_cur20() BEGIN  DECLARE x INT;DECLARE cur CURSOR FOR SELECT 1;DECLARE CONTINUE HANDLER FOR NOT FOUND SET x=1;END;
CALL test_cur20();
CALL test_cur20();
DROP PROCEDURE test_cur20;

CREATE PROCEDURE test_cur21()BEGIN DECLARE cur CURSOR FOR SELECT non_exist_col FROM t1;OPEN cur;CLOSE cur;END;
CALL test_cur21();
CALL test_cur21();
DROP PROCEDURE test_cur21;

CREATE PROCEDURE test_cur22()BEGIN DECLARE a INT;DECLARE cur CURSOR FOR SELECT a FROM t3;OPEN cur;FETCH cur INTO a;CLOSE cur;END;
CALL test_cur22();
CALL test_cur22();
DROP PROCEDURE test_cur22;

CREATE PROCEDURE test_cur23()BEGIN DECLARE done INT DEFAULT 0;DECLARE a INT;DECLARE b INT;DECLARE cur CURSOR FOR SELECT * FROM t1;DECLARE CONTINUE HANDLER FOR NOT FOUND SET done=1;OPEN cur;FETCH cur INTO a,b;SELECT done;FETCH cur INTO a,b;SELECT done;FETCH cur INTO a,b;SELECT done;CLOSE cur;SELECT done;END;
CALL test_cur23();
CALL test_cur23();
DROP PROCEDURE test_cur23;

CREATE PROCEDURE test_cur24()BEGIN DECLARE a1 INT;DECLARE a2 INT;DECLARE cur1 CURSOR FOR SELECT a FROM t1;DECLARE cur2 CURSOR FOR SELECT a FROM t2;OPEN cur1;FETCH cur1 INTO a1;OPEN cur2;FETCH cur2 INTO a2;CLOSE cur1;CLOSE cur2;END;
CALL test_cur24();
CALL test_cur24();
DROP PROCEDURE test_cur24;

CREATE PROCEDURE test_cur25()BEGIN DECLARE a INT; DECLARE cur CURSOR FOR SELECT * FROM t1;ALTER TABLE t1 ADD COLUMN c INT; OPEN cur;FETCH cur INTO a;CLOSE cur;END;
CALL test_cur25();
CALL test_cur25();
DROP PROCEDURE test_cur25;

CREATE PROCEDURE test_cur26()BEGIN DECLARE done INT DEFAULT 0;DECLARE cur CURSOR FOR SELECT a FROM t1;DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN CLOSE cur;SET done=1;ENDHANDLER;OPEN cur;FETCH cur INTO done;FETCH cur INTO done;END;
CALL test_cur26();
CALL test_cur26();
DROP PROCEDURE test_cur26;

CREATE PROCEDURE test_cur27()BEGIN DECLARE a INT;DECLARE cur CURSOR FOR SELECT 1;OPEN cur;FETCH cur INTO a;select a;CLOSE cur;END;
CALL test_cur27();
CALL test_cur27();
DROP PROCEDURE test_cur27;

CREATE PROCEDURE test_cur28(cur CURSOR)BEGIN FETCH cur INTO x;END;
CALL test_cur28();
CALL test_cur28();
DROP PROCEDURE test_cur28;

CREATE PROCEDURE test_cur29() BEGIN DECLARE x INT;DECLARE cur CURSOR FOR SELECT a FROM t1;OPEN cur;FETCH cur INTO x;UPDATE t1 SET b=10;CLOSE cur;end;
CALL test_cur29();
CALL test_cur29();
DROP PROCEDURE test_cur29;

CREATE PROCEDURE test_cur30()BEGIN START TRANSACTION;DECLARE cur CURSOR FOR SELECT a FROM t1;OPEN cur;COMMIT; CLOSE cur;END;
CALL test_cur30();
CALL test_cur30();
DROP PROCEDURE test_cur30;

CREATE PROCEDURE test_cur31()BEGIN DECLARE a VARCHAR(10);DECLARE cur CURSOR FOR SELECT a FROM t1;OPEN cur;FETCH cur INTO a;CLOSE cur;END;
CALL test_cur31();
CALL test_cur31();
DROP PROCEDURE test_cur31;

CREATE PROCEDURE test_cur32()BEGIN DECLARE "select" CURSOR FOR SELECT 1;OPEN "select";CLOSE "select";END;
CALL test_cur32();
CALL test_cur32();
DROP PROCEDURE test_cur32;

CREATE PROCEDURE test_cur33()BEGIN DECLARE a INT;DECLARE b INT;DECLARE cnt INT DEFAULT 0;DECLARE cur CURSOR FOR SELECT * FROM t1;DECLARE CONTINUE HANDLER FOR NOT FOUND SET cnt=-1;OPEN cur;FETCH cur INTO a,b;SET cnt=cnt+1;CLOSE cur;SELECT cnt;END;
CALL test_cur33();
CALL test_cur33();
DROP PROCEDURE test_cur33;

CREATE PROCEDURE test_cur34()BEGIN PREPARE stmt FROM 'SELECT a FROM t1';DECLARE cur CURSOR FOR EXECUTE stmt;OPEN cur;CLOSE cur;END;
CALL test_cur34();
CALL test_cur34();
DROP PROCEDURE test_cur34;

CREATE PROCEDURE test_cur35() BEGIN DECLARE cur CURSOR;END;
CALL test_cur35();
CALL test_cur35();
DROP PROCEDURE test_cur35;

CREATE PROCEDURE test_cur36()BEGIN DECLARE cur CURSOR FOR SELECT abs(0.1);DECLARE i INT DEFAULT 0;label myloop: WHILE i < 5 DO OPEN cur;CLOSE cur;SET i = i + 1;ENDWHILE myloop;select i;END;
CALL test_cur36();
CALL test_cur36();
DROP PROCEDURE test_cur36;

CREATE PROCEDURE test_cur37()BEGIN DECLARE cur CURSOR FOR SELECT 1;OPEN cur;CLOSE cur;END;
CALL test_cur37();
CALL test_cur37();
DROP PROCEDURE test_cur37;

CREATE PROCEDURE test_cur38()BEGIN CREATE VIEW v1 AS SELECT a FROM t1;DECLARE cur CURSOR FOR SELECT * FROM v1;OPEN cur;CLOSE cur;DROP VIEW v1;END;
CALL test_cur38();
CALL test_cur38();
DROP PROCEDURE test_cur38;

CREATE PROCEDURE test_cur39()BEGIN DECLARE cur CURSOR FOR SELECT * FROM system.user_defined_routine;OPEN cur;CLOSE cur;END;
CALL test_cur39();
CALL test_cur39();
DROP PROCEDURE test_cur39;

CREATE PROCEDURE test_cur40()BEGIN DECLARE a INT;DECLARE cur CURSOR FOR SELECT a FROM t1 ORDER BY a DESC;OPEN cur;FETCH cur INTO a;CLOSE cur;SELECT a; END;
CALL test_cur40();
CALL test_cur40();
DROP PROCEDURE test_cur40;

create procedure test_set1() begin declare a int;set a = 5;select a;end;
call test_set1();
call test_set1();
drop procedure test_set1;

create procedure test_set2()begin declare a int;set a = (10 + 5) * 2;select a;end;
call test_set2();
call test_set2();
drop procedure test_set2;

create procedure test_set3()begin set a = 5;end;
call test_set3();
call test_set3();
drop procedure test_set3;

create procedure test_set4()begin declare a int;select count(*) into a from t1;select a;end;
call test_set4();
call test_set4();
drop procedure test_set4;

create procedure test_set5() begin declare a float;set a = 'invalid';end;
call test_set5();

create procedure test_set6() begin declare a int default 1;LABEL my_loop: WHILE 1 DO declare a int default 2;select a; LEAVE my_loop; ENDWHILE my_loop;select a;end;
call test_set6();
call test_set6();
drop procedure test_set6;

CREATE PROCEDURE test_set7()BEGIN DECLARE a INT DEFAULT 10;SET @a = 20;SELECT a, @a; END;
CALL test_set7();
CALL test_set7();
DROP PROCEDURE test_set7;

CREATE PROCEDURE test_set8()BEGIN DECLARE dt timestamp; SET dt = NOW();SELECT dt = NOW();END;
CALL test_set8();
CALL test_set8();
DROP PROCEDURE test_set8;

CREATE PROCEDURE test_set9()BEGIN DECLARE x INT;SET x = (SELECT a FROM t1 limit 1);END;
CALL test_set9();
CALL test_set9();
DROP PROCEDURE test_set9;

CREATE PROCEDURE test_set10() BEGIN DECLARE a TINYINT;SET a = 255 + 1; END;
CALL test_set10();
CALL test_set10();
DROP PROCEDURE test_set10;

CREATE PROCEDURE test_set11()BEGIN DECLARE res VARCHAR(10);SET res = CASE WHEN (SELECT COUNT(*) FROM t1) > 1 THEN 'MULTI' ELSE 'SINGLE' END;SELECT res;END;
CALL test_set11();
CALL test_set11();
DROP PROCEDURE test_set11;

CREATE PROCEDURE test_set12()BEGIN DECLARE cnt INT;SET cnt = (SELECT COUNT(*) FROM t1 JOIN t2 ON t1.a=t2.a);SELECT cnt;END;
CALL test_set12();
CALL test_set12();
DROP PROCEDURE test_set12;

CREATE PROCEDURE test_set13()BEGIN DECLARE s VARCHAR(10);SET s = 123.45;SELECT s;END;
CALL test_set13();
CALL test_set13();
DROP PROCEDURE test_set13;

CREATE PROCEDURE test_set14()BEGIN DECLARE x INT DEFAULT 0;START TRANSACTION;SET x = 5;ROLLBACK;SELECT x; END;
CALL test_set14();
CALL test_set14();
DROP PROCEDURE test_set14;

CREATE PROCEDURE test_set15()BEGIN DECLARE cur_val INT;DECLARE cur CURSOR FOR SELECT a FROM t1;OPEN cur;FETCH cur INTO cur_val;SET cur_val = cur_val * 2;SELECT cur_val;CLOSE cur;END;
CALL test_set15();
CALL test_set15();
DROP PROCEDURE test_set15;

CREATE PROCEDURE test_set16()BEGIN DECLARE sys_var INT;SELECT sys_var > 0;END;
CALL test_set16();
CALL test_set16();
DROP PROCEDURE test_set16;

CREATE PROCEDURE test_set17()BEGIN DECLARE x INT;SET x = 1;SET x = x + 1;SELECT x;END;
CALL test_set17();
CALL test_set17();
DROP PROCEDURE test_set17;

-- CREATE PROCEDURE test_set18()BEGIN DECLARE "table" INT;SET "table" = 10;SELECT "table";END;
-- CALL test_set18();
-- DROP PROCEDURE test_set18;

CREATE PROCEDURE test_set19()BEGIN DECLARE cnt INT;SET cnt = (SELECT COUNT(*) FROM mysql.user);SELECT cnt > 0;END;
CALL test_set19();
CALL test_set19();
DROP PROCEDURE test_set19;

CREATE PROCEDURE test_set20()BEGIN DECLARE sql string;SET sql = 'SELECT 1';select sql;END;
CALL test_set20();
CALL test_set20();
DROP PROCEDURE test_set20;

CREATE PROCEDURE test_set21()BEGIN DECLARE err INT DEFAULT 0;DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET err = 1;SET err = 0;SELECT 1/0;SELECT err; END;
CALL test_set21();
CALL test_set21();
DROP PROCEDURE test_set21;

CREATE PROCEDURE test_set22()BEGIN DECLARE lim INT DEFAULT 1;SELECT a FROM t1 LIMIT lim;END;
CALL test_set22();
CALL test_set22();
DROP PROCEDURE test_set22;

CREATE PROCEDURE test_set23()BEGIN DECLARE len INT;SET len = CHAR_LENGTH('test');SELECT len;END;
CALL test_set23();
CALL test_set23();
DROP PROCEDURE test_set23;

CREATE PROCEDURE test_set24()BEGIN DECLARE j JSON;SET j = JSON_OBJECT('key', 'value');END;
CALL test_set24();
CALL test_set24();
DROP PROCEDURE test_set24;

CREATE PROCEDURE test_set25()BEGIN DECLARE bin VARBINARY(10);SET bin = UNHEX('4D7953514C');SELECT HEX(bin);END;
CALL test_set25();
CALL test_set25();
DROP PROCEDURE test_set25;

CREATE PROCEDURE test_set26()BEGIN DECLARE x INT;SET x = NULL;SELECT x;END;
CALL test_set26();
CALL test_set26();
DROP PROCEDURE test_set26;

CREATE PROCEDURE test_set27()BEGIN DECLARE x INT DEFAULT 5;SET x = DEFAULT;SELECT x;END;
CALL test_set27();
CALL test_set27();
DROP PROCEDURE test_set27;

CREATE PROCEDURE test_set28()BEGIN DECLARE x INT;DECLARE y INT;SET x = 1;set  y = x + 1;SELECT x, y; END;
CALL test_set28();
CALL test_set28();
DROP PROCEDURE test_set28;

CREATE PROCEDURE test_set29()BEGIN DECLARE ts1 TIMESTAMP;DECLARE ts2 TIMESTAMP;SET ts1 = now();SET ts2 = ts1 + 1s; END;
CALL test_set29();
CALL test_set29();
DROP PROCEDURE test_set29;

CREATE PROCEDURE test_set30()BEGIN DECLARE tbl_name VARCHAR(64);DECLARE sql VARCHAR(64);DECLARE cnt INT;SET tbl_name = 't1';SET sql = CONCAT('SELECT COUNT(*) INTO cnt FROM ', tbl_name);SELECT sql;END;
CALL test_set30();
CALL test_set30();
DROP PROCEDURE test_set30;

CREATE TABLE test (a INT,b int);
INSERT INTO test values(1,1),(1,2),(1,3),(1,4),(1,5);
CREATE PROCEDURE if_test1(c INT) begin IF c is null THEN INSERT INTO test VALUES (0,0); ELSIF c < 5 THEN INSERT INTO test VALUES(1,c); ELSE INSERT INTO test VALUES(2,c); ENDIF; END;
call if_test1(null);
SELECT * from test;
call if_test1(0);
SELECT * from test;
call if_test1(1);
SELECT * from test;
call if_test1(5);
SELECT * from test;
call if_test1(6);
SELECT * from test;
call if_test1(6+1);
SELECT * from test;
call if_test1('6+2');
SELECT * from test;
DROP TABLE test;
DROP PROCEDURE if_test1;

drop database procedure_test cascade;

set cluster setting sql.pg_encode_short_circuit.enabled=true;
DROP DATABASE IF EXISTS test_procedure_my_ts CASCADE;
CREATE TS DATABASE test_procedure_my_ts;

DROP DATABASE IF EXISTS test_procedure_my_rel CASCADE;
CREATE DATABASE test_procedure_my_rel;

CREATE TABLE test_procedure_my_ts.t1(
                                        k_timestamp TIMESTAMPTZ NOT NULL,
                                        id INT NOT NULL,
                                        e1 INT2,
                                        e2 INT2,
                                        e3 VARCHAR)
    TAGS (
    code1 INT2 NOT NULL,
    code2 VARCHAR)
PRIMARY TAGS(code1);

INSERT INTO test_procedure_my_ts.t1 VALUES('2024-1-1 08:00:00.1'  ,1,1,1,'a',1,'a');
INSERT INTO test_procedure_my_ts.t1 VALUES('2024-2-1 16:00:00.01' ,2,2,2,'b',2,'b');
INSERT INTO test_procedure_my_ts.t1 VALUES('2024-3-1 23:59:59.999',3,3,3,'c',3,'c');

CREATE TABLE test_procedure_my_rel.t2(
                                         k_timestamp TIMESTAMPTZ NOT NULL,
                                         id INT NOT NULL,
                                         e1 INT2,
                                         e2 INT2,
                                         e3 VARCHAR,
                                         code1 INT2 NOT NULL,
                                         code2 VARCHAR) ;


DROP PROCEDURE IF EXISTS test_procedure_my_rel.test_limit;
CREATE PROCEDURE test_procedure_my_rel.test_limit(i1 INT8) BEGIN DECLARE i2 INT ;DECLARE val1 TIMESTAMP;SELECT count(*) INTO i2 FROM test_procedure_my_ts.t1;DECLARE cur1 CURSOR FOR SELECT k_timestamp FROM test_procedure_my_ts.t1;OPEN cur1; WHILE i1 <= i2  do fetch cur1 into val1;SET i1 = i1 + 1;ENDWHILE;END;
CALL test_procedure_my_rel.test_limit(1);
CALL test_procedure_my_rel.test_limit(1);
drop database test_procedure_my_ts cascade;
drop database test_procedure_my_rel cascade;

set cluster setting sql.pg_encode_short_circuit.enabled=true;

drop database if exists ts_db cascade;
create ts database ts_db;
CREATE TABLE ts_db.st(k_timestamp TIMESTAMP(9) not null,ts TIMESTAMPTZ, e1 DOUBLE) tags (code1 INT not null) primary tags(code1);
INSERT INTO ts_db.st values(123100000, 123 ,-1.98, 1);
INSERT INTO ts_db.st values(1000000, 1 ,-1.98, 1);

set time zone 0;
SELECT ts, k_timestamp, ts - k_timestamp FROM ts_db.st WHERE ts - k_timestamp BETWEEN '-60y' AND '60y' ORDER BY k_timestamp;
select ln(e1) from ts_db.st;

drop database ts_db cascade;

drop database if exists test_alter cascade;
create ts database test_alter;
create table test_alter.t15(ts timestamp not null, a varchar(20)) tags(ptag int not null, attr1 varchar(100)) primary tags(ptag);
insert into test_alter.t15 values(1000000007, NULL, 3, 'c');
insert into test_alter.t15 values(1000000008, '-1.23', 3, 'c');
insert into test_alter.t15 values(1000000009, '3.4e+100', 3, 'c');
insert into test_alter.t15 values(1000000010, '3.4e+38', 4, 'd');
select * from test_alter.t15 order by ts,ptag;;
select * from test_alter.t15 where cast_check(a as float4)=false order by ts,ptag;
alter table test_alter.t15 alter column a type float4;
select * from test_alter.t15 order by ts,ptag;

drop database test_alter cascade;
set cluster setting sql.pg_encode_short_circuit.enabled=false;