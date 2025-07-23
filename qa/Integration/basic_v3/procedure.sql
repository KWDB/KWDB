drop database if exists procedure_db;
create database procedure_db;
use procedure_db;
create table t1(a int, b int);
create table t2(a int, b int, c int);
insert into t1 values (1, 2), (3, 4);
insert into t2 values (1, 2, 3), (4, 5, 6);
create table t3(a int, b int, c int);
create table t4(a int);

delimiter \\
create procedure p1() 
begin 
	select * from t1; 
	select * from t2; 
	select * from t3; 
	declare out_o_entry_d timestamp(3); 
	SELECT CURRENT_TIMESTAMP(3) INTO out_o_entry_d;
	select out_o_entry_d; 
end \\
delimiter ;

call p1();
drop procedure p1;

create procedure p2() begin declare out_o_entry_d timestamp(3); SELECT CURRENT_TIMESTAMP(3) INTO out_o_entry_d;select out_o_entry_d;end ;

call p2();
drop procedure p2;

create procedure p4() begin select * from t1; select * from t2;declare out_o_entry_d timestamp(3); SELECT CURRENT_TIMESTAMP(3) INTO out_o_entry_d; select out_o_entry_d; end ;

call p4();
drop procedure p4;

create procedure p5() begin select * from t3; select * from t4;end ;

call p5();
drop procedure p5;

create procedure p6() begin select * from t3; declare out_o_entry_d timestamp(3); SELECT CURRENT_TIMESTAMP(3) INTO out_o_entry_d; select out_o_entry_d; end;

call p6();
drop procedure p6;

create procedure p7() begin select * from t3; select * from t1;declare out_o_entry_d timestamp(3);SELECT CURRENT_TIMESTAMP(3) INTO out_o_entry_d;select out_o_entry_d;end ;

call p7();
drop procedure p7;

create procedure p8() begin select * from t3; select * from t1; end ;

call p8();
drop procedure p8;

create procedure p9() begin select * from t2; select * from t1; end;

call p9();
drop procedure p9;


create procedure p2() begin declare a int;declare d int; set d = 10; select a,b,d from t1; end;
call p2();
drop procedure p2;

create procedure p2() begin declare d int; set d = 10; insert into t1  (a) values (d); end;
call p2();
drop procedure p2;


create procedure p3() begin declare a int;declare d int; set d = 10; select a,b,d from t1 where d < 10; end;
call p3();
drop procedure p3;

create procedure p4() begin declare a int;declare d int; set d = 5; select a,b,d from t1 where d < 10; end;
call p4();
drop procedure p4;

CREATE PROCEDURE p3(a INT4)  BEGIN DECLARE b INT4; declare c int4;declare c1 cursor for select a from t1 ; open c1;delete from t1 where a = 1;fetch c1 into b;fetch c1 into c;close c1 ;select b;select c;END;
call p3(1);
drop procedure p3;

CREATE PROCEDURE p5(a INT4)  BEGIN update t1  set a = b -1 where a <4;END;
call p5(1);
drop procedure p5;

create procedure test10(a int, b int) begin select * from t1;select * from t2; end;
call test10(1,2);
drop procedure test10;

create procedure test20(a int, b int) begin select 1;select 2; end;
call test20(1,2);
drop procedure test20;
create procedure test() label test: begin declare a int default 0; declare b int default 0; declare err int default 0; declare exit HANDLER FOR NOT FOUND,SQLEXCEPTION BEGIN SET err = -1;SELECT a,b;ROLLBACK;ENDHANDLER;START TRANSACTION;set a = 10;select a, b, 'this is t1 values' from t1;update t1 set a =  a + 1 where b > 0;insert into t1 values (a, b);label my_loop:WHILE b <= 10 DO declare d int;set d = b + 2;if d > 9 then select *, 'this is t1 values in while' from t1; leave my_loop;elsif b > 5 then select *, 'this is t2 values in while' from t2; endif; set b = b + 1; ENDWHILE; IF err = 0 THEN SELECT a,b , 'this is procedure values'; select *, 'this is t1 values' from t1;ENDIF;COMMIT; end;

call test();
drop procedure test;


create procedure p5() begin declare d int; set d = 10; insert into t1(a) values (d); if row_count() > 1 then select * from t1; endif; end;

call p5();
drop procedure p5;

create procedure test8() begin declare a int; declare d int default 0; set a =1; LABEL my_loop: while a<3 do select d; set a=a+1;LEAVE my_loop;endwhile my_loop;end;
call test8();
drop procedure test8;


create procedure test4() begin declare a int;declare b int default 0;set a = 10; declare continue HANDLER FOR NOT FOUND set b = 9; WHILE b <= 10 DO declare d int;set d = b + 2;if d > 9 then select * from t1; elsif b > 5 then select * from t2; elsif b <= 4 then select * from t3; endif;set b = b + 1; ENDWHILE; end;
call test4();
drop procedure test4;

create procedure test6() begin declare a int;declare b int default 0;set a = 10; declare continue HANDLER FOR NOT FOUND set b = 9; WHILE b <= 10 DO declare d int;set d = b + 2;if d > 9 then select * from t1; elsif b > 5 then select * from t2; elsif b <= 4 then select * from t3; endif;set b = b + 1; ENDWHILE; end;
call test6();
drop procedure test6;

create procedure test7() begin declare a int;declare b int default 0;set a = 10; declare continue HANDLER FOR SQLEXCEPTION set b = 9; WHILE b <= 10 DO declare d int;set d = b + 2;if d > 9 then select * from t1; elsif b > 5 then select * from t2; elsif b <= 4 then select * from t3; endif;set b = b + 1; ENDWHILE; end;
call test7();
drop procedure test7;

create procedure p1() begin declare a int;declare b int; set b = 10; select b; end;
call p1();
drop procedure p1;

create procedure test91() begin declare a int default 0;declare b int default 0; WHILE b <= 5 do declare d int;set d = b + 2; select a,b,d from t1; set b = b +1;  ENDWHILE; end;
call test91();
drop procedure test91;

create procedure test92() begin declare a int default 0;declare d int default 0; set d = 10; select a,b,d from t1; end;
call test92();
drop procedure test92;

create procedure test30() begin declare a int default 0;declare b int default 0;set a = 10;WHILE b <= 10 DO declare d int;set d = b + 2;if d > 9 then select a,b,d from t1; elsif b > 5 then select a,b,c,d from t2; endif;set b = b + 1; ENDWHILE; end;
call test30();
drop procedure test30;

create procedure test31() begin declare a int default 0;declare b int default 0;set a = 10;WHILE b <= 10 DO declare d int;set d = b + 2;if d > 9 then select a,b,d from t1; elsif b > 5 then select a,b,c,d from t2; endif;set b = b + 1; ENDWHILE; end;
call test31();
drop procedure test31;


CREATE PROCEDURE test_variables() BEGIN DECLARE var1 INT default 0;DECLARE var2 VARCHAR(20); SET var1 = 10; SET var2 = 'Hello'; SELECT var1 + 5, CONCAT(var2, ' World'); end;

call test_variables();
drop procedure test_variables;


create procedure test_cursor() label test: begin declare a int; DECLARE done INT DEFAULT 0; DECLARE cur CURSOR FOR SELECT a FROM t1; declare continue HANDLER FOR NOT FOUND BEGIN SET done = 1; select 'result get all' as print; ENDHANDLER; OPEN cur; label read_loop: while done = 0 DO declare b int; FETCH cur INTO b; SELECT b; ENDWHILE read_loop; CLOSE cur; end ;

call test_cursor();
drop procedure test_cursor;


CREATE TABLE employees(id SERIAL PRIMARY KEY, name VARCHAR(100),age INT, salary decimal(10,2));

INSERT INTO employees (name, age, salary) VALUES ('test1', 1, 2.2);
INSERT INTO employees (name, age, salary) VALUES ('test2', 2, 2.2);
INSERT INTO employees (name, age, salary) VALUES ('test3', 3, 2.2);

CREATE PROCEDURE add1(emp_age INT, age int) BEGIN declare var_done int; declare var_age int; set var_age = 1; DECLARE cur CURSOR FOR SELECT age FROM employees; DECLARE CONTINUE HANDLER FOR NOT FOUND SET var_done = 1; OPEN cur; LABEL my_loop: WHILE var_age > 0 DO FETCH cur INTO var_age; IF var_done = 1 THEN LEAVE my_loop; ENDIF; select var_age; ENDWHILE; CLOSE cur; END;
call add1(1,2);
drop procedure add1;

CREATE PROCEDURE add2(emp_age INT, age int) BEGIN declare var_done int; declare var_age int; set var_age = 1; DECLARE cur CURSOR FOR SELECT age FROM employees; DECLARE CONTINUE HANDLER FOR NOT FOUND SET var_done = 1; OPEN cur; LABEL my_loop: WHILE var_age > 0 DO FETCH cur INTO var_age; IF var_done = 1 THEN LEAVE my_loop; ENDIF; select var_age; ENDWHILE; CLOSE cur; END;
call add2(1,2);
drop procedure add2;

CREATE PROCEDURE add3(emp_age INT, age int) BEGIN declare var_done int; declare var_age int; set var_age = 1; DECLARE cur CURSOR FOR SELECT age FROM employees; DECLARE CONTINUE HANDLER FOR NOT FOUND SET var_done = 1; OPEN cur; LABEL my_loop: WHILE var_age > 0 DO FETCH cur INTO var_age; IF var_done = 1 THEN LEAVE my_loop; ENDIF; select var_age; ENDWHILE; CLOSE cur; END;
call add3(1,2);
drop procedure add3;

-- error
CREATE PROCEDURE add4(emp_age INT, age int) BEGIN declare var_done int; declare var_age int; set var_age = 1; declare var_name int; DECLARE cur CURSOR FOR SELECT age FROM employees; DECLARE CONTINUE HANDLER FOR NOT FOUND SET var_done = 1; OPEN cur; LABEL my_loop: WHILE var_age > 0 DO FETCH cur INTO var_age, var_name; IF var_done = 1 THEN LEAVE my_loop; ENDIF; select var_age; ENDWHILE my_loop; CLOSE cur; INSERT INTO employees (name, age, salary) VALUES ('qiliang111', 111, 3.2); END;
call add4(1,2);
drop procedure add4;

CREATE PROCEDURE add5(emp_age INT, age int) BEGIN declare var_done int; declare var_age int; set var_age = 1; declare var_name int; DECLARE cur CURSOR FOR SELECT age FROM employees; DECLARE CONTINUE HANDLER FOR NOT FOUND SET var_done = 1; OPEN cur; LABEL my_loop: WHILE var_age > 0 DO FETCH cur INTO var_age; IF var_done = 1 THEN LEAVE my_loop; ENDIF; select var_age; ENDWHILE my_loop; CLOSE cur; INSERT INTO employees (name, age, salary) VALUES ('qiliang111', 111, 3.2); END;
call add5(1,2);
drop procedure add5;

create procedure add6() label test:begin  declare a int default 0; declare b int default 0; declare err int default 0; declare exit HANDLER FOR NOT FOUND,SQLEXCEPTION BEGIN SET err = -1; SELECT a,b; ROLLBACK; ENDHANDLER; START TRANSACTION; set a = 10; select a, b from t1; update t1 set a =  a + 1 where b > 0; insert into t1 values (a, b); label my_loop: WHILE b <= 10 DO declare d int; set d = b + 2; if d > 9 then select * from t1; leave my_loop; elsif b > 5 then select * from t2; endif; set b = b + 1; ENDWHILE; IF err = 0 THEN SELECT a,b; COMMIT; ENDIF; end;
call add6();
drop procedure add6;

drop table employees;

-- error
create procedure p1() begin declare exit HANDLER FOR NOT FOUND,SQLEXCEPTION BEGIN SET err = -1; SELECT err,out_ol_supply_w_ids, out_i_ids, out_ol_quantities, out_item_names, out_supply_quantities, out_brand_generic, out_prices, out_amounts,out_total_amount,out_c_credit,out_c_discount,out_c_last, out_o_id,out_d_tax,out_o_entry_d,out_w_tax; ROLLBACK; ENDHANDLER; end;
drop procedure p1;

CREATE PROCEDURE p3(a INT4) BEGIN DECLARE b INT4; declare c int4;declare c1 cursor for select a from t1 ; open c1;delete from t1 where a = 1;fetch c1 into b;fetch c1 into c;close c1 ;select b;select c;END;
call p3(1);
drop procedure p3;

drop table if exists bmsql_item;
create table bmsql_item (
i_id integer not null,
i_name varchar(24),
i_price decimal(5,2),
i_data varchar(50),
i_im_id integer,
primary key (i_id)
);

insert into bmsql_item values(23793, 'EYgGREs95keggHoKSWZYp', 96.63, 'abmFWK6MwPyDUSWgp0furfZurJP', 4567);

DROP PROCEDURE IF EXISTS bmsql_proc_new_order;

delimiter \\
CREATE PROCEDURE bmsql_proc_new_order(
	in_w_id int,
	in_d_id int,
	in_c_id int,
	in_o_ol_cnt int,
	in_o_all_local int,
	in_ol_i_id_array varchar(512),
	in_ol_supply_w_id_array varchar(512),
	in_ol_quantity varchar(512)) 
BEGIN 
	declare var_i_price decimal(5,2) default NULL; 
	declare var_i_name varchar(24) default NULL; 
	declare var_ol_number int default 1; 
	START TRANSACTION; 
	WHILE var_ol_number <= in_o_ol_cnt DO 
		SELECT i_price, i_name AS bg INTO var_i_price, var_i_name FROM bmsql_item WHERE i_id = 23793;
		SET var_ol_number= var_ol_number+1;
	ENDWHILE;
	COMMIT;
END \\
delimiter ;

call bmsql_proc_new_order(1, 3, 236, 2, 1, '23793,33231,34317,42513,42513,49887,50697,58887,66991,67565,70673,87404,96719', '1,1,1,1,1,1,1,1,1,1,1,1,1', '7,8,4,8,7,2,7,6,5,2,8,4,1');
drop procedure bmsql_proc_new_order;
drop table bmsql_item;

drop table if exists bmsql_stock;

create table bmsql_stock (
s_i_id integer not null,
s_w_id integer not null,
s_quantity integer,
s_ytd integer,
s_order_cnt integer,
s_remote_cnt integer,
s_data varchar(50),
s_dist_01 char(24),
s_dist_02 char(24),
s_dist_03 char(24),
s_dist_04 char(24),
s_dist_05 char(24),
s_dist_06 char(24),
s_dist_07 char(24),
s_dist_08 char(24),
s_dist_09 char(24),
s_dist_10 char(24),
primary key (s_w_id, s_i_id),
index bmsql_stock_idx1(s_i_id)
);

insert into bmsql_stock(s_i_id, s_w_id, s_order_cnt) values(23793, 1, 0);

delimiter \\
CREATE PROCEDURE bmsql_proc_new_order() 
BEGIN 
	declare var_s_quantity smallint default NULL; 
	declare var_ol_number int default 1; 
	WHILE var_ol_number <= 2 DO 
		UPDATE bmsql_stock SET s_order_cnt = s_order_cnt + 1 WHERE s_i_id = 23793 AND s_w_id = 1 RETURNING s_order_cnt;
		SET var_ol_number= var_ol_number+1;
	ENDWHILE;
END \\
delimiter ;

call bmsql_proc_new_order();
drop procedure bmsql_proc_new_order;
drop table bmsql_stock;


create table test_order (
no_w_id integer not null,
no_d_id integer not null,
no_o_id integer not null
);

DROP PROCEDURE IF EXISTS bmsql_proc_del;
delimiter \\
CREATE PROCEDURE bmsql_proc_del() 
BEGIN 
	declare i int default 1; 
	declare del_o_id int; 
	delete from test_order where 1=1; 
	WHILE i < 3 DO 
		INSERT INTO test_order(no_o_id, no_d_id, no_w_id) VALUES (i, 1, 1) RETURNING no_o_id;
		DELETE FROM test_order WHERE no_w_id = 1 AND no_d_id = 1 AND no_o_id = i RETURNING no_o_id into del_o_id;
		select del_o_id;
		set i = i+1;
	ENDWHILE;
END \\
delimiter ;
call bmsql_proc_del();
drop procedure bmsql_proc_del;
drop table test_order;

drop database procedure_db cascade;


DROP DATABASE IF EXISTS db cascade;

-- declare and assignment
create PROCEDURE prc_declare() begin declare counts1 string;declare counts2 int default 0; declare counts3 int default 0; set counts1 ='10a'; set counts2 ='123'; set counts3 ='10'; SELECT counts1, (counts2+counts3); end;
CALL prc_declare();
DROP PROCEDURE prc_declare;

-- not declare and not assignment
create PROCEDURE prc_declare() begin SELECT counts1; end;

-- not declare and not assignment and not use
create PROCEDURE prc_declare() begin SELECT '0'; end;
CALL prc_declare();
DROP PROCEDURE prc_declare;


-- not declare and assignment and not use
create PROCEDURE prc_declare() begin set counts1 ='10a'; set counts2 ='tesr'; set counts3 ='10'; SELECT counts1 ; SELECT counts2 ; SELECT counts3 ; end;

-- not declare and assignment and use
create PROCEDURE prc_declare() begin set counts1 ='10a'; set counts2 ='tesr'; set counts3 ='10'; SELECT counts1, (counts2+counts3);end;

-- not declare and assignment and replace and not use
create PROCEDURE prc_declare() begin set counts1 ='10a';set counts1 ='10a';set counts3 ='10';SELECT 1; end;


-- declare and not assignment and not use
create PROCEDURE prc_declare() begin declare counts1 int;declare counts2 int;declare counts3 int; SELECT counts1*(counts2+counts3); SELECT '10', 999, 'abc'; SELECT counts1*(counts2+counts3-1)-counts1%counts3 - counts2*1+1; end;
CALL prc_declare();
DROP PROCEDURE prc_declare;

create PROCEDURE prc_declare(a INT) begin declare counts int; set counts = 1; set a =10; set a =11; SELECT a+counts ;end;
CALL prc_declare(1);
DROP PROCEDURE prc_declare;

create PROCEDURE prc_declare(a INT) begin declare counts int; set counts = 1; SELECT a+counts ; end;
CALL prc_declare(1);
DROP PROCEDURE prc_declare;

create PROCEDURE prc_declare(a INT) begin declare counts int;declare a int;set counts = 1; SELECT a+counts; end;
DROP PROCEDURE prc_declare;

create PROCEDURE prc_declare() begin declare a1 INT default 9223372036854775807; SELECT a1 ;end;
CALL prc_declare();
DROP PROCEDURE prc_declare;

-- error expected DEFAULT expression to have type timestamp, but '9223372036854775807' has type int
create PROCEDURE prc_declare() begin declare a1 timestamp default 9223372036854775807; SELECT a1 ;end;

create PROCEDURE prc_declare() begin declare a1 timestamp default '2020-01-02 10:00:00'; SELECT a1 ;end;
CALL prc_declare();
DROP PROCEDURE prc_declare;

create PROCEDURE prc_declare() begin declare a1 timestamp(9) default '2020-01-02 10:00:00.123456789'; SELECT a1 ;end;
CALL prc_declare();
DROP PROCEDURE prc_declare;

create PROCEDURE prc_declare() begin declare a1 timestamp(3) default '2020-01-02 10:00:00.123456789'; SELECT a1 ;end;
CALL prc_declare();
DROP PROCEDURE prc_declare;

-- ERROR: variable sub-expressions are not allowed in DEFAULT
create PROCEDURE prc_declare(b timestamp) begin declare a1 TIMESTAMP default now()+b; SELECT a1 ;end;

-- ERROR: numeric constant out of int64 range
create PROCEDURE prc_declare() begin declare a1 INT default 9223372036854775808; SELECT a1 ;end;

create PROCEDURE prc_declare() begin declare a1 DECIMAL(1,1)  default 3.21; SELECT a1 ;end;
call prc_declare();
DROP PROCEDURE prc_declare;

-- not null attribute
create PROCEDURE prc_declare() begin declare a1 INT default null; SELECT a1 ;end;
CALL prc_declare();
DROP PROCEDURE prc_declare;



DROP PROCEDURE IF EXISTS fun_LOOP_TEST2;
DROP TABLE IF EXISTS prc_tab;

CREATE TABLE prc_tab(loop_test2 decimal(10,3));
create PROCEDURE LOOP_TEST2(m INT) BEGIN DECLARE n NUMERIC default 1; DECLARE l decimal(10,3) default 1.0; WHILE l < 10 DO set n = n+1;  select n; set l = m*n; select l;  ENDWHILE;  INSERT INTO prc_tab VALUES(l); END;

CALL LOOP_TEST2(1);
SELECT * FROM prc_tab;
CALL LOOP_TEST2(100);
SELECT * FROM prc_tab;
DROP TABLE prc_tab;
DROP PROCEDURE LOOP_TEST2;
DROP TABLE prc_tab;



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
DROP TABLE test;
DROP PROCEDURE if_test1;
DROP TABLE test;


DROP TABLE IF EXISTS test;
DROP PROCEDURE IF EXISTS if_test1;
CREATE TABLE test (a INT,b int);
INSERT INTO test values(1,1),(1,2),(1,3),(1,4),(1,5);
CREATE PROCEDURE if_test1(c INT) BEGIN IF c is null THEN INSERT INTO test VALUES (0,0); ELSIF c = 3 THEN INSERT INTO test VALUES(1,c); ELSIF c = 3 THEN INSERT INTO test VALUES(1,c); ENDIF; END;
call if_test1(null);
SELECT * from test;
call if_test1(0);
SELECT * from test;
call if_test1(1);
SELECT * from test;
call if_test1(3);
SELECT * from test;
call if_test1(5);
SELECT * from test;
DROP TABLE test;
DROP PROCEDURE if_test1;
DROP TABLE test;


DROP TABLE IF EXISTS test;
DROP PROCEDURE IF EXISTS loop_test1;
CREATE TABLE test (a INT);
CREATE PROCEDURE loop_test1() BEGIN DECLARE c INT default 1; label my_loop: WHILE 1 DO INSERT INTO test VALUES(c); IF c>5 THEN leave my_loop; ENDIF; set c=c+1; ENDWHILE; END;
call loop_test1();
select * from test;
DROP TABLE test;
DROP PROCEDURE loop_test1;
DROP TABLE test;

create table test1 (id int,a int); 
CREATE procedure pro_12 (x int8, y int8) BEGIN DECLARE re int8;  set re=x+y;   insert into test1(id,a) values (1,re); END;
call pro_12(6,4);
select * from test1;
drop table if exists test1;
drop procedure if exists pro_12;
drop table if exists test1;

create table test1 (id int,a int); 
CREATE procedure pro_13 (x int8, y int8) BEGIN DECLARE re int8;  insert into test1(id,a) values (1,x+y); END;
call pro_13(6,4);
select * from test1;
drop table if exists test1;
drop procedure if exists pro_13;
drop table if exists test1;


DROP TABLE IF EXISTS CP_TEST;
CREATE TABLE cp_test (a int, b text);
DROP PROCEDURE IF EXISTS  ptest1;
CREATE PROCEDURE ptest1(x text) BEGIN INSERT INTO cp_test VALUES (1, x); END;
SELECT ptest1('x'); 
CALL ptest1('a'); 
CALL ptest1('xy' || 'zzy');  
CALL ptest1(substring(random()::numeric(20,15)::text, 1, 1));
SELECT * FROM cp_test ORDER BY b ;
DROP TABLE IF EXISTS CP_TEST;
DROP PROCEDURE IF EXISTS  ptest1;
DROP TABLE IF EXISTS CP_TEST;

drop table if exists test;
create table test (old text, new text, mod_time timestamp);
insert into test values ('old', 'new', '20201013');
select old,new from test ;
drop procedure if exists upreturn2;
create procedure upreturn2(a int,X text)  begin update test set new = 'bill', old = new, mod_time = '20250513' returning new into X; select X; end;
CALL upreturn2(1,'text');
select * from test;
drop procedure if exists upreturn2;
drop table if exists test;

drop table if exists test;
create table test (old text, new text, mod_time timestamp);
insert into test values ('old', 'new', '20201013');
select old,new from test ;
drop procedure if exists delreturn2;
create procedure delreturn2(X TEXT) begin delete from test returning new into X; select X; end;
select * from test;
call delreturn2('NEW');
drop procedure if exists delreturn2;
drop table if exists test;


drop table if exists test;
create table test (old text, new text, mod_time timestamp);
insert into test values ('old', 'new', '20201013');
select old,new from test ;
drop procedure if exists insreturn2;
create procedure insreturn2(X text) begin insert into test values('insert','insert','20201010') returning new into X; select X; end;
call insreturn2('insert');
drop procedure if exists insreturn2;
select old,new from test ;
drop table if exists test;


drop table if exists test;
create table test(id int);
insert into test select generate_series(1,20);
delimiter \\
CREATE procedure fun_affect_rows() 
begin 
	declare v_count1 int; 
	declare v_count2 int; 
	declare v_count3 int; 
	insert into test values(99),(98); 
	set v_count1 = ROW_COUNT(); 
	select v_count1; 
	delete from test where id < 15; 
	set v_count2 = ROW_COUNT(); 
	select v_count2; 
	update test set id = 100 where id >90; 
	set v_count3 = ROW_COUNT(); 
	select v_count3; 
end \\
delimiter ;
call fun_affect_rows();
drop procedure fun_affect_rows;
drop table if exists test;


drop table if  exists test1;
create table test1(a int);
drop PROCEDURE if exists transaction_test1;
delimiter \\
CREATE PROCEDURE transaction_test1()
BEGIN    
	declare i INT default 0;
	
	while i < 9 do
		START TRANSACTION;
		INSERT INTO test1 (a) VALUES (i);
		IF i % 2 = 0 THEN
			COMMIT;
		ELSE
			ROLLBACK;
		ENDIF;
		set i = i + 1;
	ENDwhile; 
END \\
delimiter ;
CALL transaction_test1();
select * from test1;--expected 0 2 4 6 8

drop table if  exists test1;
drop PROCEDURE if exists transaction_test1;
drop table if  exists test1;

create table test1(a int);
drop PROCEDURE if exists transaction_test1;
delimiter \\
CREATE PROCEDURE transaction_test1() 
BEGIN     
	declare i INT default 0;
	
	while i < 9 do
		START TRANSACTION;
		INSERT INTO test1 (a) VALUES (i);
		IF i % 2 = 0 THEN
			ROLLBACK;
		ELSE
			commit;
		ENDIF; 
		set i = i + 1;
	ENDWHILE; 
END \\
delimiter ;
CALL transaction_test1();
select * from test1;--expected 1 3 5 7 9
drop PROCEDURE if exists transaction_test1;

DROP DATABASE IF EXISTS db cascade;

DROP DATABASE IF EXISTS db1 cascade;
create database db1;
create table db1.t1(a int);
create table db1.t2(b int);
insert into db1.t1 values(111);
insert into db1.t2 values(222);

create procedure db1.p1() begin select *from db1.t1,db1.t2; end;
call db1.p1();
DROP DATABASE IF EXISTS db1 cascade;

drop PROCEDURE if exists duplicate_para_test1;
create procedure duplicate_para_test1(a int, a string) begin select a; select a!=0; end;
create procedure duplicate_para_test1(a int) begin declare b int; declare c string; declare b string; end;

drop PROCEDURE if exists overwrite_para_test1;
create procedure overwrite_para_test1(a int) begin select a; select a!=0; declare b int; set b=12; declare a string; set a ='zx'; select a; select a!='zx'; end;
call overwrite_para_test1(123);
drop PROCEDURE if exists overwrite_para_test1;

create database test_db;
use test_db;
create table t1(a int2);
insert into t1 values(1);

DROP PROCEDURE IF EXISTS test_redeclare;
CREATE PROCEDURE test_redeclare(test1 STRING) BEGIN DECLARE test1 int2; SELECT a INTO test1 FROM test_db.public.t1 limit 1; SELECT test1; END;
call test_redeclare('test');

DROP PROCEDURE IF EXISTS test_declare;
CREATE PROCEDURE test_declare() BEGIN DECLARE "table" INT; SET "table" = 1; SELECT "table"; END;
call test_declare();

DROP PROCEDURE IF EXISTS test_into;
CREATE PROCEDURE test_into() BEGIN DECLARE a1 INT; DECLARE a2 INT; select * into a1, a2 from t1; END;

use defaultdb;
drop database test_db cascade;

--不赋值时，各类型数据的默认值应该为NULL
DROP PROCEDURE IF EXISTS test_declare_6;

CREATE PROCEDURE test_declare_6() BEGIN DECLARE test1 INT2 ;DECLARE test2 CHAR(10);DECLARE test3 FLOAT4;DECLARE test4 FLOAT4;DECLARE test5 DECIMAL;DECLARE test6 TIMESTAMP;SELECT test1,test2,test3,test4,test5,test6,length(test2);END;

CALL test_declare_6();
DROP PROCEDURE IF EXISTS test_declare_6;

--无法用存储过程为表插入变量值为NULL的数据
DROP DATABASE IF EXISTS test_procedure_my_rel_null CASCADE;
DROP DATABASE IF EXISTS test_procedure_my_ts_null CASCADE;

CREATE DATABASE test_procedure_my_rel_null ;
CREATE TS DATABASE test_procedure_my_ts_null ;

CREATE TABLE test_procedure_my_ts_null.test1(---更改
k_ts TIMESTAMPTZ NOT NULL, ----TIMESTAMP 、TIMESTAMPTZ、TIMESTAMP(3)
id INT NOT NULL,e1 INT)
    TAGS (
code1 INT2 NOT NULL)
PRIMARY TAGS(code1);

INSERT INTO test_procedure_my_ts_null.test1 VALUES('2024-1-1 08:00:00.1' ,1,NULL,1);
INSERT INTO test_procedure_my_ts_null.test1 VALUES('2024-2-1 16:00:00.01' ,2,NULL,2);
INSERT INTO test_procedure_my_ts_null.test1 VALUES('2024-3-1 23:59:59.999',3,NULL,3);

CREATE TABLE test_procedure_my_ts_null.test2(---更改
k_ts TIMESTAMPTZ NOT NULL, ----TIMESTAMP 、TIMESTAMPTZ、TIMESTAMP(3)
id INT NOT NULL,e1 INT)
    TAGS (
code1 INT2 NOT NULL)
PRIMARY TAGS(code1);

DROP PROCEDURE IF EXISTS test_procedure_my_ts_null.test_null;

CREATE PROCEDURE test_procedure_my_ts_null.test_null(i1 INT) BEGIN DECLARE test1 TIMESTAMPTZ;DECLARE test2 INT;DECLARE test3 INT;DECLARE test4 INT; SELECT  k_ts,id,e1,code1 INTO test1,test2,test3,test4 FROM test_procedure_my_ts_null.test1 WHERE id = i1; INSERT INTO test_procedure_my_ts_null.test2 values(test1,test2,test3,test4); SELECT 1; END;

CALL test_procedure_my_ts_null.test_null(1);
select *from test_procedure_my_ts_null.test2;
DROP DATABASE IF EXISTS test_procedure_my_rel_null CASCADE;
DROP DATABASE IF EXISTS test_procedure_my_ts_null CASCADE;

CREATE DATABASE test_procedure_my_cursor_cursor_rel;
DROP TABLE IF EXISTS test_procedure_my_cursor_cursor_rel.test1 CASCADE;
CREATE TABLE test_procedure_my_cursor_cursor_rel.test1 (t1 INT,t2 VARCHAR);
INSERT INTO test_procedure_my_cursor_cursor_rel.test1 VALUES(1,'A');
INSERT INTO test_procedure_my_cursor_cursor_rel.test1 VALUES(2,'B');
INSERT INTO test_procedure_my_cursor_cursor_rel.test1 VALUES(3,'C');

DROP PROCEDURE IF EXISTS test_procedure_my_cursor_cursor_rel.test_cursor_2;
CREATE PROCEDURE test_procedure_my_cursor_cursor_rel.test_cursor_2() BEGIN DECLARE val1 INT;DECLARE val2 VARCHAR;DECLARE val3 VARCHAR;DECLARE cur1 CURSOR FOR SELECT * FROM test_procedure_my_cursor_cursor_rel.test1 WHERE t1 IN(1,2) ORDER BY t1 desc;OPEN cur1;FETCH cur1 INTO val1,val2;DECLARE cur1 CURSOR FOR SELECT * FROM test_procedure_my_cursor_cursor_rel.test1 WHERE t1 NOT IN(1,2) ORDER BY t1 desc;SELECT val1,val2;CLOSE cur1;END;
DROP PROCEDURE test_procedure_my_cursor_cursor_rel.test_cursor_2;


CREATE PROCEDURE test_procedure_my_cursor_cursor_rel.test_cursor_3() BEGIN DECLARE val1 INT;DECLARE val2 int;DECLARE val3 int;DECLARE cur1 CURSOR FOR SELECT * FROM test_procedure_my_cursor_cursor_rel.test1 WHERE t1 IN(1,2) ORDER BY t1 desc;DECLARE cur1 CURSOR FOR SELECT * FROM test_procedure_my_cursor_cursor_rel.test1 WHERE t1 NOT IN(1,2) ORDER BY t1 desc;OPEN cur1;FETCH cur1 INTO val1,val2;SELECT val1,val2;CLOSE cur1;END;
DROP PROCEDURE test_procedure_my_cursor_cursor_rel.test_cursor_3;

DROP PROCEDURE IF EXISTS test_procedure_my_cursor_cursor_rel.test_cursor_4;
CREATE PROCEDURE test_procedure_my_cursor_cursor_rel.test_cursor_4()BEGIN DECLARE val1 INT DEFAULT 100000;OPEN cur1;FETCH cur1 INTO val1;SELECT val1;END;
CALL test_procedure_my_cursor_cursor_rel.test_cursor_4();
CALL test_procedure_my_cursor_cursor_rel.test_cursor_4();

CREATE PROCEDURE test_procedure_my_cursor_cursor_rel.test_cursor_5()BEGIN DECLARE val1 INT DEFAULT 100000;FETCH cur1 INTO val1;SELECT val1;END;
CALL test_procedure_my_cursor_cursor_rel.test_cursor_5();
CALL test_procedure_my_cursor_cursor_rel.test_cursor_5();

DROP DATABASE IF EXISTS test_procedure_my_cursor_cursor_rel CASCADE;

DROP DATABASE IF EXISTS test_procedure_my_limit_ts CASCADE;
CREATE TS DATABASE test_procedure_my_limit_ts;

CREATE TABLE test_procedure_my_limit_ts.test2(---更改
k_timestamp TIMESTAMPTZ NOT NULL, ----TIMESTAMP 、TIMESTAMPTZ、TIMESTAMP(3)
id INT NOT NULL,
e1 INT2,
e2 INT2,
e3 VARCHAR)
TAGS (
code1 INT2 NOT NULL,
code2 VARCHAR,
code3 VARCHAR)
PRIMARY TAGS(code1);

INSERT INTO test_procedure_my_limit_ts.test2 VALUES('2024-1-1 08:00:00.1'  ,1,1,1,'a',1,'a','a');
INSERT INTO test_procedure_my_limit_ts.test2 VALUES('2024-2-1 16:00:00.01' ,2,2,2,'b',2,'b','b');
INSERT INTO test_procedure_my_limit_ts.test2 VALUES('2024-3-1 23:59:59.999',3,3,3,'c',3,'c','c');

---建立存储过程依赖此库
DROP PROCEDURE IF EXISTS test_procedure_my_limit_ts.test_limit;
CREATE PROCEDURE test_procedure_my_limit_ts.test_limit(i1 INT) BEGIN DELETE FROM test_procedure_my_limit_ts.test2 WHERE code1 = i1; END;
---校验是否成功
select *from test_procedure_my_limit_ts.test2 order by id;
CALL test_procedure_my_limit_ts.test_limit(1);
select *from test_procedure_my_limit_ts.test2 order by id;
CALL test_procedure_my_limit_ts.test_limit(3);
select *from test_procedure_my_limit_ts.test2 order by id;
DROP PROCEDURE IF EXISTS test_procedure_my_limit_ts.test_limit;
CREATE PROCEDURE test_procedure_my_limit_ts.test_limit(i1 timestamp) BEGIN DELETE FROM test_procedure_my_limit_ts.test2 WHERE k_timestamp = i1; END;


DROP PROCEDURE IF EXISTS test_procedure_my_limit_ts.test_limit;
CREATE PROCEDURE test_procedure_my_limit_ts.test_limit(i1 INT) BEGIN UPDATE test_procedure_my_limit_ts.test2 set code2='test' WHERE code1 = i1; END;
DROP DATABASE IF EXISTS test_procedure_my_limit_ts CASCADE;

--ZDP-48292 【ent-0708dev20】【存储过程】更新时序表，对依赖tag列限制有误
DROP DATABASE IF EXISTS test_procedure_my_limit_ts CASCADE;
DROP DATABASE IF EXISTS test_procedure_my_limit_rel CASCADE;
CREATE TS DATABASE test_procedure_my_limit_ts;
CREATE DATABASE test_procedure_my_limit_rel;
DROP PROCEDURE IF EXISTS test_procedure_my_limit_rel.test_limit;
CREATE TABLE test_procedure_my_limit_ts.test2(
k_timestamp TIMESTAMPTZ NOT NULL,
id INT NOT NULL,
e1 INT2,
e2 INT2,
e3 VARCHAR)
TAGS (
code1 INT2 NOT NULL,
code2 VARCHAR,
code3 VARCHAR)
PRIMARY TAGS(code1);
INSERT INTO test_procedure_my_limit_ts.test2 VALUES('2024-1-1 08:00:00.1'  ,1,1,1,'a',1,'a','a');
INSERT INTO test_procedure_my_limit_ts.test2 VALUES('2024-2-1 16:00:00.01' ,2,2,2,'b',2,'b','b');
INSERT INTO test_procedure_my_limit_ts.test2 VALUES('2024-3-1 23:59:59.999',3,3,3,'c',3,'c','c');

CREATE PROCEDURE test_procedure_my_limit_ts.test_limit(i1 INT) $$BEGIN UPDATE test_procedure_my_limit_ts.test2 SET code2 = 'test' WHERE code1 = 1; END$$;

ALTER TABLE test_procedure_my_limit_ts.test2 RENAME TAG code2  TO code222;
CALL test_procedure_my_limit_ts.test_limit(1);
ALTER TABLE test_procedure_my_limit_ts.test2 RENAME TAG code222 TO code2;
DROP DATABASE IF EXISTS test_procedure_my_limit_ts CASCADE;
DROP DATABASE IF EXISTS test_procedure_my_limit_rel CASCADE;

drop table if exists t1 cascade;
create table t1(a int, b int);
create procedure p1() begin update t1 set a =1 where b=0; end;
alter table t1 rename column a to aa;
alter table t1 rename column b to bb;
drop table if exists t1 cascade;

create procedure p1();
create procedure p1(a int);
create procedure p1(a int) $$ begin declare b int; declare c int; end; select b,c;$$;
create procedure p1(a int) $$ begin declare b int; declare c int; end;$$ begin select b,c; end;
create procedure p1(a int) $$ begin declare b int; declare c int; select a,b,c; end$$;
call p1(111);
drop procedure p1;

--【存储过程】类型不匹配时调用存储过程返回结果随机
-- https://e.gitee.com/kaiwuDB/issues/table?issue=ICMUUI
DROP DATABASE IF EXISTS test_procedure_my_rel CASCADE;
create database test_procedure_my_rel;
DROP PROCEDURE IF EXISTS test_procedure_my_rel.test_input_5;

CREATE PROCEDURE test_procedure_my_rel.test_input_5(a INT2,b INT4,c INT8)
    $$ BEGIN SELECT a,b,c;END $$ ;
CALL test_procedure_my_rel.test_input_5(99.99,-1981.389,1982.193);
CALL test_procedure_my_rel.test_input_5(99.99,-1981.389,1982.193);
DROP DATABASE IF EXISTS test_procedure_my_rel CASCADE;