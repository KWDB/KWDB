drop database if exists procedure_ts_db;
create ts database procedure_ts_db;
use procedure_ts_db;
create table t1(k_timestamp timestamp not null,a int, b int)tags(size int not null)primary tags(size);
create table t2(k_timestamp timestamp not null,a int, b int, c int)tags(size int not null)primary tags(size);
insert into t1 values ('2000-1-1',1, 2, 1), ('2000-1-2',3, 4,2);
insert into t2 values ('2000-1-1',1, 2, 3,1), ('2000-1-2',4, 5, 6,2);
create table t3(k_timestamp timestamp not null,a int, b int, c int)tags(size int not null)primary tags(size);
create table t4(k_timestamp timestamp not null,a int)tags(size int not null)primary tags(size);
insert into t3 values ('2000-1-10',7, 8, 9, 1);

create procedure test_cur1() begin declare cur cursor for select * from t1 order by k_timestamp;open cur;close cur;end;
call test_cur1();
call test_cur1();
drop procedure test_cur1;

create procedure test_cur2() begin open cur;end;
call test_cur2();
call test_cur2();
drop procedure test_cur2;

create procedure test_cur3() begin declare a int;declare cur cursor for select a,b from t1 order by k_timestamp;open cur;fetch cur into a;close cur;end;
call test_cur3();
call test_cur3();
drop procedure test_cur3;

create procedure test_cur4() begin declare a int;declare b int;declare cur cursor for select a,b from t1 order by k_timestamp;open cur;fetch cur into a,b;close cur;select a,b;end;
call test_cur4();
call test_cur4();
drop procedure test_cur4;

create procedure test_cur4_2() begin declare a int;declare b int;declare cur cursor for select a,b from t3 order by k_timestamp;open cur;fetch cur into a,b;close cur;select a,b;end;
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

create procedure test_cur7()begin declare done int default 1;declare a int;declare b int;declare cur cursor for select a,b from t1 order by k_timestamp;declare continue handler for not found set done=1;declare flag int; set flag = 1;open cur;LABEL my_loop: WHILE flag > 0 DO fetch cur into a,b;if done!=0 then select a,b; endif;if a=3 then set flag = -1; endif;ENDWHILE my_loop;close cur;end;
call test_cur7();
call test_cur7();
drop procedure test_cur7;

create procedure test_cur8()begin declare done1 int default 0;declare done2 int default 0;declare a int;declare b int;declare cur1 cursor for select a,b from t1 order by k_timestamp;declare cur2 cursor for select a,b,c from t2 order by k_timestamp;declare continue handler for not found set done1=1;open cur1;open cur2;fetch cur1 into a,b;fetch cur2 into a,b,b;close cur1;close cur2;end;
call test_cur8();
call test_cur8();
drop procedure test_cur8;

create procedure test_cur9(val int)begin declare a int;declare cur cursor for select a from t1 where a > val order by k_timestamp;open cur;fetch cur into a;select a;close cur;end;
call test_cur9(0);
call test_cur9(0);
drop procedure test_cur9;

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

CREATE PROCEDURE test_cur15()BEGIN DECLARE done INT DEFAULT 0;DECLARE cur CURSOR FOR SELECT a FROM t1 order by k_timestamp;DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;OPEN cur;FETCH cur INTO done;CLOSE cur;END;
CALL test_cur15();
CALL test_cur15();
DROP PROCEDURE test_cur15;


CREATE PROCEDURE test_cur18()BEGIN DECLARE done INT DEFAULT 0;DECLARE a INT;DECLARE cur CURSOR FOR SELECT a FROM t1 order by k_timestamp;DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;OPEN cur;FETCH cur INTO a;select a;FETCH cur INTO a;select a;FETCH cur INTO a;select a;FETCH cur INTO a;select a;CLOSE cur;END;
CALL test_cur18();
CALL test_cur18();
DROP PROCEDURE test_cur18;

CREATE PROCEDURE test_cur19() BEGIN DECLARE a INT;DECLARE b INT;DECLARE c INT;DECLARE cur CURSOR FOR SELECT a,b FROM t2 order by k_timestamp;OPEN cur;FETCH cur INTO a,b;CLOSE cur;END;
CALL test_cur19();
CALL test_cur19();
DROP PROCEDURE test_cur19;

CREATE PROCEDURE test_cur20() BEGIN  DECLARE x INT;DECLARE cur CURSOR FOR SELECT 1;DECLARE CONTINUE HANDLER FOR NOT FOUND SET x=1;END;
CALL test_cur20();
CALL test_cur20();
DROP PROCEDURE test_cur20;

CREATE PROCEDURE test_cur22()BEGIN DECLARE a INT;DECLARE cur CURSOR FOR SELECT a FROM t3;OPEN cur;FETCH cur INTO a;CLOSE cur;END;
CALL test_cur22();
CALL test_cur22();
DROP PROCEDURE test_cur22;

CREATE PROCEDURE test_cur23()BEGIN DECLARE done INT DEFAULT 0;DECLARE a INT;DECLARE b INT;DECLARE cur CURSOR FOR SELECT a,b FROM t1 order by k_timestamp;DECLARE CONTINUE HANDLER FOR NOT FOUND SET done=1;OPEN cur;FETCH cur INTO a,b;SELECT done;FETCH cur INTO a,b;SELECT done;FETCH cur INTO a,b;SELECT done;CLOSE cur;SELECT done;END;
CALL test_cur23();
CALL test_cur23();
DROP PROCEDURE test_cur23;

CREATE PROCEDURE test_cur24()BEGIN DECLARE a1 INT;DECLARE a2 INT;DECLARE cur1 CURSOR FOR SELECT a FROM t1 order by k_timestamp;DECLARE cur2 CURSOR FOR SELECT a FROM t2  order by k_timestamp;OPEN cur1;FETCH cur1 INTO a1;OPEN cur2;FETCH cur2 INTO a2;CLOSE cur1;CLOSE cur2;select a1,a2;END;
CALL test_cur24();
CALL test_cur24();
DROP PROCEDURE test_cur24;

CREATE PROCEDURE test_cur25()BEGIN DECLARE a INT; DECLARE cur CURSOR FOR SELECT a FROM t1 order by k_timestamp;OPEN cur;FETCH cur INTO a;CLOSE cur;END;
CALL test_cur25();
CALL test_cur25();
DROP PROCEDURE test_cur25;

CREATE PROCEDURE test_cur26()BEGIN DECLARE done INT DEFAULT 0;DECLARE cur CURSOR FOR SELECT a FROM t1 order by k_timestamp;DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN CLOSE cur;SET done=1;ENDHANDLER;OPEN cur;FETCH cur INTO done;FETCH cur INTO done;END;
CALL test_cur26();
CALL test_cur26();
DROP PROCEDURE test_cur26;

CREATE PROCEDURE test_cur27()BEGIN DECLARE a INT;DECLARE cur CURSOR FOR SELECT 1;OPEN cur;FETCH cur INTO a;select a;CLOSE cur;END;
CALL test_cur27();
CALL test_cur27();
DROP PROCEDURE test_cur27;

CREATE PROCEDURE test_cur29() BEGIN DECLARE x INT;DECLARE cur CURSOR FOR SELECT a FROM t1 order by k_timestamp;OPEN cur;FETCH cur INTO x;CLOSE cur;select x;end;
CALL test_cur29();
CALL test_cur29();
DROP PROCEDURE test_cur29;

CREATE PROCEDURE test_cur30()BEGIN START TRANSACTION;DECLARE cur CURSOR FOR SELECT a FROM t1 order by k_timestamp;OPEN cur;COMMIT; CLOSE cur;END;
CALL test_cur30();
CALL test_cur30();
DROP PROCEDURE test_cur30;

CREATE PROCEDURE test_cur31()BEGIN DECLARE a VARCHAR(10);DECLARE cur CURSOR FOR SELECT a FROM t1 order by k_timestamp;OPEN cur;FETCH cur INTO a;CLOSE cur;END;
CALL test_cur31();
CALL test_cur31();
DROP PROCEDURE test_cur31;

CREATE PROCEDURE test_cur32()BEGIN DECLARE "select" CURSOR FOR SELECT 1;OPEN "select";CLOSE "select";END;
CALL test_cur32();
CALL test_cur32();
DROP PROCEDURE test_cur32;

CREATE PROCEDURE test_cur33()BEGIN DECLARE a INT;DECLARE b INT;DECLARE cnt INT DEFAULT 0;DECLARE cur CURSOR FOR SELECT a,b FROM t1 order by k_timestamp;DECLARE CONTINUE HANDLER FOR NOT FOUND SET cnt=-1;OPEN cur;FETCH cur INTO a,b;SET cnt=cnt+1;CLOSE cur;SELECT cnt;END;
CALL test_cur33();
CALL test_cur33();
DROP PROCEDURE test_cur33;

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

CREATE PROCEDURE test_cur39()BEGIN DECLARE cur CURSOR FOR SELECT * FROM system.user_defined_routine;OPEN cur;CLOSE cur;END;
CALL test_cur39();
CALL test_cur39();
DROP PROCEDURE test_cur39;

CREATE PROCEDURE test_cur40()BEGIN DECLARE a INT;DECLARE cur CURSOR FOR SELECT a FROM t1 ORDER BY a DESC;OPEN cur;FETCH cur INTO a;CLOSE cur;SELECT a; END;
CALL test_cur40();
CALL test_cur40();
DROP PROCEDURE test_cur40;

delimiter \\
create procedure p1()
begin
select * from t1 order by k_timestamp;
select * from t2 order by k_timestamp;
select * from t3 order by k_timestamp;
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

create procedure p4() begin select * from t1 order by k_timestamp; select * from t2 order by k_timestamp;declare out_o_entry_d timestamp(3); SELECT CURRENT_TIMESTAMP(3) INTO out_o_entry_d; select out_o_entry_d; end ;

call p4();
drop procedure p4;

create procedure p5() begin select * from t3; select * from t4;end ;

call p5();
drop procedure p5;

create procedure p6() begin select * from t3; declare out_o_entry_d timestamp(3); SELECT CURRENT_TIMESTAMP(3) INTO out_o_entry_d; select out_o_entry_d; end;

call p6();
drop procedure p6;

create procedure p7() begin select * from t3 order by k_timestamp; select * from t1 order by k_timestamp;declare out_o_entry_d timestamp(3);SELECT CURRENT_TIMESTAMP(3) INTO out_o_entry_d;select out_o_entry_d;end ;

call p7();
drop procedure p7;

create procedure p8() begin select * from t3 order by k_timestamp; select * from t1 order by k_timestamp; end ;

call p8();
drop procedure p8;

create procedure p9() begin select * from t2 order by k_timestamp; select * from t1 order by k_timestamp; end;

call p9();
drop procedure p9;


create procedure p2() begin declare a int;declare d int; set d = 10; select a,b,d from t1 order by k_timestamp; end;
call p2();
drop procedure p2;

create procedure p3() begin declare a int;declare d int; set d = 10; select a,b,d from t1 where d < 10 order by k_timestamp; end;
call p3();
drop procedure p3;

create procedure p4() begin declare a int;declare d int; set d = 5; select a,b,d from t1 where d < 10 order by k_timestamp; end;
call p4();
drop procedure p4;

-- crash
CREATE PROCEDURE p3(a INT4)  BEGIN DECLARE b INT4; declare c int4;declare c1 cursor for select a from t1 order by k_timestamp; open c1;fetch c1 into b;fetch c1 into c;close c1 ;select b;select c;END;
call p3(1);
drop procedure p3;

create procedure test10(a int, b int) begin select * from t1 order by k_timestamp;select * from t2 order by k_timestamp; end;
call test10(1,2);
drop procedure test10;

create procedure test20(a int, b int) begin select 1;select 2; end;
call test20(1,2);
drop procedure test20;
create procedure test() label test: begin declare a int default 0; declare b int default 0; declare err int default 0; declare exit HANDLER FOR NOT FOUND,SQLEXCEPTION BEGIN SET err = -1;SELECT a,b;ROLLBACK;ENDHANDLER;START TRANSACTION;set a = 10;select a, b, 'this is t1 values' from t1 order by k_timestamp;insert into t1 values ('2000-1-3',5, 6,3);label my_loop:WHILE b <= 10 DO declare d int;set d = b + 2;if d > 9 then select *, 'this is t1 values in while' from t1 order by k_timestamp; leave my_loop;elsif b > 5 then select *, 'this is t2 values in while' from t2 order by k_timestamp; endif; set b = b + 1; ENDWHILE; IF err = 0 THEN SELECT a,b , 'this is procedure values'; select *, 'this is t1 values' from t1 order by k_timestamp;ENDIF;COMMIT; end;

call test();
drop procedure test;


create procedure p5() begin declare d int; set d = 10; insert into t1 values ('2000-1-4',1,1,1); if row_count() > 0 then select * from t1 order by k_timestamp;endif; end;
call p5();
drop procedure p5;

create procedure p5() begin declare d int; set d = 10; insert into t1 values ('2000-1-4',1,1,1); select * from t1 order by k_timestamp; end;
call p5();
drop procedure p5;

create procedure test8() begin declare a int; declare d int; set a =1; LABEL my_loop: while a<3 do select d; set a=a+1;LEAVE my_loop;endwhile my_loop;end;
call test8();
drop procedure test8;


create procedure test4() begin declare a int;declare b int default 0;set a = 10; declare continue HANDLER FOR NOT FOUND set b = 9; WHILE b <= 10 DO declare d int;set d = b + 2;if d > 9 then select * from t1 order by k_timestamp; elsif b > 5 then select * from t2 order by k_timestamp; elsif b <= 4 then select * from t3 order by k_timestamp; endif;set b = b + 1; ENDWHILE; end;
call test4();
drop procedure test4;

create procedure test6() begin declare a int;declare b int default 0;set a = 10; declare continue HANDLER FOR NOT FOUND set b = 9; WHILE b <= 10 DO declare d int;set d = b + 2;if d > 9 then select * from t1 order by k_timestamp; elsif b > 5 then select * from t2 order by k_timestamp; elsif b <= 4 then select * from t3 order by k_timestamp; endif;set b = b + 1; ENDWHILE; end;
call test6();
drop procedure test6;

create procedure test7() begin declare a int;declare b int default 0;set a = 10; declare continue HANDLER FOR SQLEXCEPTION set b = 9; WHILE b <= 10 DO declare d int;set d = b + 2;if d > 9 then select * from t1 order by k_timestamp; elsif b > 5 then select * from t2 order by k_timestamp; elsif b <= 4 then select * from t3 order by k_timestamp; endif;set b = b + 1; ENDWHILE; end;
call test7();
drop procedure test7;

create procedure p1() begin declare a int;declare b int; set b = 10; select b; end;
call p1();
drop procedure p1;

create procedure test91() begin declare a int default 0;declare b int default 0; WHILE b <= 5 do declare d int;set d = b + 2; select a,b,d from t1 order by k_timestamp; set b = b +1;  ENDWHILE; end;
call test91();
drop procedure test91;

create procedure test92() begin declare a int;declare d int; set d = 10; select a,b,d from t1 order by k_timestamp; end;
call test92();
drop procedure test92;

create procedure test30() begin declare a int;declare b int default 0;set a = 10;WHILE b <= 10 DO declare d int;set d = b + 2;if d > 9 then select a,b,d from t1 order by k_timestamp; elsif b > 5 then select a,b,c,d from t2 order by k_timestamp; endif;set b = b + 1; ENDWHILE; end;
call test30();
drop procedure test30;

create procedure test31() begin declare a int;declare b int default 0;set a = 10;WHILE b <= 10 DO declare d int;set d = b + 2;if d > 9 then select a,b,d from t1 order by k_timestamp; elsif b > 5 then select a,b,c,d from t2 order by k_timestamp; endif;set b = b + 1; ENDWHILE; end;
call test31();
drop procedure test31;


CREATE PROCEDURE test_variables() BEGIN DECLARE var1 INT;DECLARE var2 VARCHAR(20); SET var1 = 10; SET var2 = 'Hello'; SELECT var1 + 5, CONCAT(var2, ' World'); end;

call test_variables();
drop procedure test_variables;


create procedure test_cursor() label test: begin declare a int; DECLARE done INT DEFAULT 0; DECLARE cur CURSOR FOR SELECT a FROM t1 order by k_timestamp; declare continue HANDLER FOR NOT FOUND BEGIN SET done = 1; select 'result get all' as print; ENDHANDLER; OPEN cur; label read_loop: while done = 0 DO declare b int; FETCH cur INTO b; SELECT b; ENDWHILE read_loop; CLOSE cur; end ;

call test_cursor();
drop procedure test_cursor;


CREATE TABLE employees(k_timestamp timestamp not null, id int, name VARCHAR(100),age INT, salary float8)tags(size int not null)primary tags(size);

INSERT INTO employees (k_timestamp,name, age, salary,size) VALUES ('2000-1-1','test1', 1, 2.2, 1);
INSERT INTO employees (k_timestamp,name, age, salary,size) VALUES ('2000-1-2','test2', 2, 2.2, 2);
INSERT INTO employees (k_timestamp,name, age, salary,size) VALUES ('2000-1-3','test3', 3, 2.2, 3);

CREATE PROCEDURE add1(emp_age INT, age int) BEGIN declare var_done int; declare var_age int; set var_age = 1; DECLARE cur CURSOR FOR SELECT age FROM employees order by k_timestamp; DECLARE CONTINUE HANDLER FOR NOT FOUND SET var_done = 1; OPEN cur; LABEL my_loop: WHILE var_age > 0 DO FETCH cur INTO var_age; IF var_done = 1 THEN LEAVE my_loop; ENDIF; select var_age; ENDWHILE; CLOSE cur; END;
call add1(1,2);
drop procedure add1;

CREATE PROCEDURE add2(emp_age INT, age int) BEGIN declare var_done int; declare var_age int; set var_age = 1; DECLARE cur CURSOR FOR SELECT age FROM employees order by k_timestamp; DECLARE CONTINUE HANDLER FOR NOT FOUND SET var_done = 1; OPEN cur; LABEL my_loop: WHILE var_age > 0 DO FETCH cur INTO var_age; IF var_done = 1 THEN LEAVE my_loop; ENDIF; select var_age; ENDWHILE; CLOSE cur; END;
call add2(1,2);
drop procedure add2;

CREATE PROCEDURE add3(emp_age INT, age int) BEGIN declare var_done int; declare var_age int; set var_age = 1; DECLARE cur CURSOR FOR SELECT age FROM employees order by k_timestamp; DECLARE CONTINUE HANDLER FOR NOT FOUND SET var_done = 1; OPEN cur; LABEL my_loop: WHILE var_age > 0 DO FETCH cur INTO var_age; IF var_done = 1 THEN LEAVE my_loop; ENDIF; select var_age; ENDWHILE; CLOSE cur; END;
call add3(1,2);
drop procedure add3;

-- crash
CREATE PROCEDURE add4(emp_age INT, age int) BEGIN declare var_done int; declare var_age int; set var_age = 1; declare var_name int; DECLARE cur CURSOR FOR SELECT age FROM employees order by k_timestamp; DECLARE CONTINUE HANDLER FOR NOT FOUND SET var_done = 1; OPEN cur; LABEL my_loop: WHILE var_age > 0 DO FETCH cur INTO var_age, var_name; IF var_done = 1 THEN LEAVE my_loop; ENDIF; select var_age; ENDWHILE my_loop; CLOSE cur; INSERT INTO employees VALUES ('2000-1-4',5,'test4', 111, 3.2,5); END;
call add4(1,2);
drop procedure add4;

CREATE PROCEDURE add5(emp_age INT, age int) BEGIN declare var_done int; declare var_age int; set var_age = 1; declare var_name int; DECLARE cur CURSOR FOR SELECT age FROM employees order by k_timestamp; DECLARE CONTINUE HANDLER FOR NOT FOUND SET var_done = 1; OPEN cur; LABEL my_loop: WHILE var_age > 0 DO FETCH cur INTO var_age; IF var_done = 1 THEN LEAVE my_loop; ENDIF; select var_age; ENDWHILE my_loop; CLOSE cur; INSERT INTO employees VALUES ('2000-1-5',6,'test5', 111, 3.2,5); END;
call add5(1,2);
drop procedure add5;

create procedure add6() label test:begin  declare a int default 0; declare b int default 0; declare err int default 0; declare exit HANDLER FOR NOT FOUND,SQLEXCEPTION BEGIN SET err = -1; SELECT a,b; ROLLBACK; ENDHANDLER; START TRANSACTION; set a = 10; select a, b from t1 order by k_timestamp; label my_loop: WHILE b <= 10 DO declare d int; set d = b + 2; if d > 9 then select * from t1 order by k_timestamp; leave my_loop; elsif b > 5 then select * from t2 order by k_timestamp; endif; set b = b + 1; ENDWHILE; IF err = 0 THEN SELECT a,b; COMMIT; ENDIF; end;
call add6();
drop procedure add6;

drop table employees;

create procedure p1() begin declare exit HANDLER FOR NOT FOUND,SQLEXCEPTION BEGIN SET err = -1; SELECT err,out_ol_supply_w_ids, out_i_ids, out_ol_quantities, out_item_names, out_supply_quantities, out_brand_generic, out_prices, out_amounts,out_total_amount,out_c_credit,out_c_discount,out_c_last, out_o_id,out_d_tax,out_o_entry_d,out_w_tax; ROLLBACK; ENDHANDLER; end;
drop procedure p1;

CREATE PROCEDURE p3(a INT4) BEGIN DECLARE b INT4 default 0; declare c int4 default 0;declare c1 cursor for select a from t1 order by k_timestamp; open c1;fetch c1 into b;fetch c1 into c;close c1 ;select b;select c;END;
call p3(1);
drop procedure p3;

drop table if exists bmsql_item;
create table bmsql_item (
                            k_timestamp timestamp not null,
                            i_id integer not null,
                            i_name varchar(24),
                            i_price float8,
                            i_data varchar(50),
                            i_im_id integer)tags(size int not null)primary tags(size);

insert into bmsql_item values('2000-1-1',23793, 'EYgGREs95keggHoKSWZYp', 96.63, 'abmFWK6MwPyDUSWgp0furfZurJP', 4567,1);

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
	declare var_i_price float default NULL;
	declare var_i_name varchar(24) default NULL;
	declare var_ol_number int default 1;
START TRANSACTION;
WHILE var_ol_number <= in_o_ol_cnt DO
SELECT i_price, i_name AS bg INTO var_i_price, var_i_name FROM bmsql_item WHERE i_id = 23793 order by k_timestamp;
SET var_ol_number= var_ol_number+1;
	ENDWHILE;
COMMIT;
END \\
delimiter ;

call bmsql_proc_new_order(1, 3, 236, 2, 1, '23793,33231,34317,42513,42513,49887,50697,58887,66991,67565,70673,87404,96719', '1,1,1,1,1,1,1,1,1,1,1,1,1', '7,8,4,8,7,2,7,6,5,2,8,4,1');
drop procedure bmsql_proc_new_order;
drop table bmsql_item;

create table test_order (
                            k_timestamp timestamp not null,
                            no_w_id integer not null,
                            no_d_id integer not null,
                            no_o_id integer not null
)tags(size int not null) primary tags(size);

DROP PROCEDURE IF EXISTS bmsql_proc_del;
delimiter \\
CREATE PROCEDURE bmsql_proc_del()
BEGIN
	declare i int default 1;
	declare del_o_id int;
delete from test_order where 1=1;
WHILE i < 3 DO
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
create PROCEDURE prc_declare() begin declare counts1 string;declare counts2 int; declare counts3 int; set counts1 ='10a'; set counts2 ='123'; set counts3 ='10'; SELECT counts1, (counts2+counts3); end;
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

-- ERROR: duplicate variable name: "a"
create PROCEDURE prc_declare(a INT) begin declare counts int;declare a int;set counts = 1; SELECT a+counts; end;
CALL prc_declare(2);
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

CREATE TABLE prc_tab(k_timestamp timestamp not null,loop_test2 float)tags(size int not null)primary tags(size);
create PROCEDURE LOOP_TEST2(m INT) BEGIN DECLARE n float default 1; DECLARE l float default 1.0; WHILE l < 10 DO set n = n+1;  select n; set l = m*n; select l;  ENDWHILE;  END;

CALL LOOP_TEST2(1);
SELECT * FROM prc_tab;
CALL LOOP_TEST2(100);
SELECT * FROM prc_tab;
DROP TABLE prc_tab;
DROP PROCEDURE LOOP_TEST2;


CREATE TABLE test (k_timestamp timestamp not null,a INT,b int)tags(size int not null)primary tags(size);
INSERT INTO test values('2000-1-1',1,1,1),('2000-1-2',1,2,2),('2000-1-3',1,3,3),('2000-1-4',1,4,4),('2000-1-5',1,5,5);
CREATE PROCEDURE if_test1(c INT) begin IF c is null THEN select a from test order by k_timestamp; ELSIF c < 5 THEN select b from test order by k_timestamp; ELSE select a,b from test order by k_timestamp; ENDIF; END;
call if_test1(null);
SELECT * from test order by k_timestamp;
call if_test1(0);
SELECT * from test order by k_timestamp;
call if_test1(1);
SELECT * from test order by k_timestamp;
call if_test1(5);
SELECT * from test order by k_timestamp;
call if_test1(6);
SELECT * from test order by k_timestamp;
DROP PROCEDURE if_test1;
DROP TABLE test;


DROP TABLE IF EXISTS test;
DROP PROCEDURE IF EXISTS if_test1;
CREATE TABLE test (k_timestamp timestamp not null,a INT,b int)tags(size int not null)primary tags(size);
INSERT INTO test values('2000-1-1',1,1,1),('2000-1-2',1,2,2),('2000-1-3',1,3,3),('2000-1-4',1,4,4),('2000-1-5',1,5,5);
CREATE PROCEDURE if_test1(c INT) BEGIN IF c is null THEN select a from test order by k_timestamp; ELSIF c = 3 THEN select b from test order by k_timestamp; ELSIF c = 3 THEN select a,b from test order by k_timestamp; ENDIF; END;
call if_test1(null);
SELECT * from test order by k_timestamp;
call if_test1(0);
SELECT * from test order by k_timestamp;
call if_test1(1);
SELECT * from test order by k_timestamp;
call if_test1(3);
SELECT * from test order by k_timestamp;
call if_test1(5);
SELECT * from test order by k_timestamp;
DROP PROCEDURE if_test1;
DROP TABLE test;


DROP TABLE IF EXISTS test;
DROP PROCEDURE IF EXISTS loop_test1;
CREATE TABLE test (k_timestamp timestamp not null,a INT)tags(size int not null)primary tags(size);
CREATE PROCEDURE loop_test1() BEGIN DECLARE c INT default 1; label my_loop: WHILE 1 DO SELECT * from test order by k_timestamp; IF c>5 THEN leave my_loop; ENDIF; set c=c+1; ENDWHILE; END;
call loop_test1();
select * from test;
DROP PROCEDURE loop_test1;
DROP TABLE test;

DROP DATABASE IF EXISTS db cascade;


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
CREATE PROCEDURE test_procedure_my_rel.test_limit(i1 INT8) BEGIN DECLARE i2 INT ;DECLARE val1 TIMESTAMP; DECLARE val2 INT2; DECLARE val3 VARCHAR; DECLARE val4 INT2; DECLARE val5 VARCHAR; SELECT count(*) INTO i2 FROM test_procedure_my_ts.t1;DECLARE cur1 CURSOR FOR SELECT k_timestamp FROM test_procedure_my_ts.t1;DECLARE cur2 CURSOR FOR SELECT e1          FROM test_procedure_my_ts.t1;DECLARE cur3 CURSOR FOR SELECT e3          FROM test_procedure_my_ts.t1;DECLARE cur4 CURSOR FOR SELECT code1       FROM test_procedure_my_ts.t1;DECLARE cur5 CURSOR FOR SELECT code2       FROM test_procedure_my_ts.t1;OPEN cur1;OPEN cur2;OPEN cur3;OPEN cur4;OPEN cur5; WHILE i1 <= i2  do fetch cur1 into val1;fetch cur2 into val2;fetch cur3 into val3;fetch cur4 into val4; fetch cur5 into val5; SET i1 = i1 + 1; ENDWHILE; END;

CALL test_procedure_my_rel.test_limit(1);
--error
ALTER TABLE test_procedure_my_ts.t1 RENAME TO test_procedure_my_ts.t2;
DROP TABLE test_procedure_my_ts.t1;

DROP PROCEDURE test_procedure_my_rel.test_limit;

use defaultdb;
DROP DATABASE test_procedure_my_ts CASCADE;
DROP DATABASE test_procedure_my_rel CASCADE;

DROP DATABASE IF EXISTS test_procedure_my_load_rel CASCADE;
CREATE DATABASE test_procedure_my_load_rel;
DROP DATABASE IF EXISTS test_procedure_my_load_ts CASCADE;
CREATE TS DATABASE test_procedure_my_load_ts;

CREATE TABLE test_procedure_my_load_ts.test1(
                                                k_timestamp TIMESTAMPTZ NOT NULL,
                                                id INT NOT NULL,
                                                e1 INT2,
                                                e2 INT2,
                                                e3 VARCHAR)
    TAGS(
code1 INT2 NOT NULL,
code2 VARCHAR,
code3 VARCHAR)
PRIMARY TAGS (code1);

INSERT INTO test_procedure_my_load_ts.test1 VALUES('2024-1-1 08:00:00.1' ,1,1,1,'a',1,'a','a');
INSERT INTO test_procedure_my_load_ts.test1 VALUES('2024-2-1 16:00:00.01' ,2,2,2,'b',2,'b','b');
INSERT INTO test_procedure_my_load_ts.test1 VALUES('2024-3-1 23:59:59.999',3,3,3,'c',3,'c','c');

CREATE TABLE test_procedure_my_load_ts.test2(
                                                k_timestamp TIMESTAMPTZ NOT NULL,
                                                id INT NOT NULL,
                                                e1 INT2,
                                                e2 INT2,
                                                e3 VARCHAR)
    TAGS(
code1 INT2 NOT NULL,
code2 VARCHAR,
code3 VARCHAR)
PRIMARY TAGS (code1);

DROP PROCEDURE IF EXISTS test_procedure_my_load_rel.test_load;
CREATE PROCEDURE test_procedure_my_load_rel.test_load(i1 INT8)BEGIN DECLARE i2 INT ;DECLARE val1 TIMESTAMPTZ;DECLARE val2 INT2;DECLARE val3 VARCHAR;DECLARE val4 INT2;DECLARE val5 VARCHAR;SELECT count(*) INTO i2 FROM test_procedure_my_load_ts.test1;DECLARE cur1 CURSOR FOR SELECT k_timestamp FROM test_procedure_my_load_ts.test1 ORDER BY id;DECLARE cur2 CURSOR FOR SELECT e1          FROM test_procedure_my_load_ts.test1 ORDER BY id;DECLARE cur3 CURSOR FOR SELECT e3          FROM test_procedure_my_load_ts.test1 ORDER BY id;DECLARE cur4 CURSOR FOR SELECT code1       FROM test_procedure_my_load_ts.test1 ORDER BY id;DECLARE cur5 CURSOR FOR SELECT code2       FROM test_procedure_my_load_ts.test1 ORDER BY id;OPEN cur1;OPEN cur2;OPEN cur3;OPEN cur4;OPEN cur5;WHILE i1 <= i2  do fetch cur1 into val1;fetch cur2 into val2;fetch cur3 into val3;fetch cur4 into val4;fetch cur5 into val5;INSERT INTO test_procedure_my_load_ts.test2 VALUES(val1,i1,val2,val2,val3,val4,val5,val5);SET i1 = i1 + 1;ENDWHILE;END;
CALL test_procedure_my_load_rel.test_load(1);

SHOW CREATE PROCEDURE test_procedure_my_load_rel.test_load;
drop procedure test_procedure_my_load_rel.test_load;
SELECT * FROM test_procedure_my_load_ts.test1 ORDER BY id;
SELECT * FROM test_procedure_my_load_ts.test2 ORDER BY id;

DROP DATABASE test_procedure_my_load_rel CASCADE;
DROP DATABASE test_procedure_my_load_ts CASCADE;


CREATE DATABASE test_procedure_dxy;
CREATE TABLE test_procedure_dxy.t1(
                                      tp  TIMESTAMPTZ NOT NULL,
                                      id  INT NOT NULL,
                                      e1  INT2,
                                      e2  INT,
                                      e3  INT8,
                                      e4  FLOAT4,
                                      e5  FLOAT8,
                                      e6  BOOL,
                                      e7  TIMESTAMP,
                                      e8  CHAR(1023),
                                      e9  NCHAR(255),
                                      e10 VARCHAR(4096),
                                      e11 CHAR,
                                      e12 NCHAR,
                                      e13 NVARCHAR(4096),
                                      e14 VARBYTES,
                                      e15 VARCHAR,
                                      e16 NVARCHAR,
                                      e17 BLOB,
                                      e18 CLOB
);
INSERT INTO test_procedure_dxy.t1 VALUES('1970-01-01 00:00:00+00:00'    ,1 , 0     ,0          ,0                   ,0                      ,0                          ,true ,'1970-01-01 00:00:00+00:00'    ,''                                       ,''                                     ,''                                      ,''   ,''                                      ,''   ,''                                       ,''                                       ,''                                    ,''                                    ,''                                    );
INSERT INTO test_procedure_dxy.t1 VALUES('1970-1-1 00:00:00.001'        ,2 , 0     ,0          ,0                   ,0                      ,0                          ,true ,'1970-01-01 00:16:39.999+00:00','          '                             ,'          '                           ,'          '                            ,' '  ,' '                            ,' '  ,' '                                      ,'          '                             ,'          '                          ,'          '                          ,'          '                          );
INSERT INTO test_procedure_dxy.t1 VALUES('1976-10-20 12:00:12.123+00:00',3 , 10001 ,10000001   ,100000000001        ,-1047200.00312001      ,-1109810.113011921         ,true ,'2021-03-01 12:00:00.909+00:00','test数据库语法查询测试！！！@TEST3-8'   ,'test数据库语法查询测试！！！@TEST3-9' ,'test数据库语法查询测试！！！@TEST3-10' ,'t'  ,'2' ,'中' ,'test数据库语法查询测试！！！@TEST3-14'  ,'test数据库语法查询测试！！！@TEST3-15'  ,'test数据库语法查询测试！TEST3-16xaa' ,'test数据库语法查询测试！TEST3-16xaa' ,'test数据库语法查询测试！TEST3-16xaa' );
INSERT INTO test_procedure_dxy.t1 VALUES('1979-2-28 11:59:01.999'       ,4 , 20002 ,20000002   ,200000000002        ,-20873209.0220322201   ,-22012110.113011921        ,false,'1970-01-01 00:00:00.123+00:00','test数据库语法查询测试！！！@TEST4-8'   ,'test数据库语法查询测试！！！@TEST4-9' ,'test数据库语法查询测试！！！@TEST4-10' ,'t'  ,'2' ,'中' ,'test数据库语法查询测试！！！@TEST4-14'  ,'test数据库语法查询测试！！！@TEST4-15'  ,'test数据库语法查询测试！TEST4-16xaa' ,'test数据库语法查询测试！TEST4-16xaa' ,'test数据库语法查询测试！TEST4-16xaa' );
INSERT INTO test_procedure_dxy.t1 VALUES('1980-01-31 19:01:01+00:00'    ,5 , 30003 ,30000003   ,300000000003        ,-33472098.11312001     ,-39009810.333011921        ,true ,'2015-3-12 10:00:00.234'       ,'test数据库语法查询测试！！！@TEST5-8'   ,'test数据库语法查询测试！！！@TEST5-9' ,'test数据库语法查询测试！！！@TEST5-10' ,'t'  ,'2' ,'中' ,'test数据库语法查询测试！！！@TEST5-14'  ,'test数据库语法查询测试！！！@TEST5-15'  ,'test数据库语法查询测试！TEST5-16xaa' ,'test数据库语法查询测试！TEST5-16xaa' ,'test数据库语法查询测试！TEST5-16xaa' );
INSERT INTO test_procedure_dxy.t1 VALUES('1980-02-10 01:14:51.09+00:00' ,6 , -10001,10000001   ,-100000000001       ,1047200.00312001       ,1109810.113011921          ,false,'2023-6-23 05:00:00.55'        ,'test数据库语法查询测试！！！@TEST6-8'   ,'test数据库语法查询测试！！！@TEST6-9' ,'test数据库语法查询测试！！！@TEST6-10' ,'t'  ,'2' ,'中' ,'test数据库语法查询测试！！！@TEST6-14'  ,'test数据库语法查询测试！！！@TEST6-15'  ,'test数据库语法查询测试！TEST6-16xaa' ,'xaaxbbxcc'                           ,'xaaxbbxcc'                           );
INSERT INTO test_procedure_dxy.t1 VALUES('1980-02-10 01:48:11.029+00:00',7 , -20002,20000002   ,-200000000002       ,20873209.0220322201    ,22012110.113011921         ,true ,'2016-07-17 20:12:00.12+00:00' ,'test数据库语法查询测试！！！@TEST7-8'   ,'test数据库语法查询测试！！！@TEST7-9' ,'test数据库语法查询测试！！！@TEST7-10' ,'t'  ,'2' ,'中' ,'test数据库语法查询测试！！！@TEST7-14'  ,'test数据库语法查询测试！！！@TEST7-15'  ,'test数据库语法查询测试！TEST7-16xaa' ,'010101010101011010101010101010101010','010101010101011010101010101010101010');
INSERT INTO test_procedure_dxy.t1 VALUES('1980-02-10 01:48:22.501+00:00',8 , -30003,30000003   ,-300000000003       ,33472098.11312001      ,39009810.333011921         ,false,'1970-01-01 01:16:05.476+00:00','test数据库语法查询测试！！！@TEST8-8'   ,'test数据库语法查询测试！！！@TEST8-9' ,'test数据库语法查询测试！！！@TEST8-10' ,'t'  ,'2' ,'中' ,'test数据库语法查询测试！！！@TEST8-14'  ,'test数据库语法查询测试！！！@TEST8-15'  ,'test数据库语法查询测试！TEST8-16xaa' ,'test数据库语法查询测试！TEST8-16xaa' ,'test数据库语法查询测试！TEST8-16xaa' );
INSERT INTO test_procedure_dxy.t1 VALUES('2001-12-09 09:48:12.3+00:00'  ,9 , null  ,null       ,null                ,null                   ,null                       ,null ,null                           ,null                                     ,null                                   ,null                                    ,null ,null                                    ,null ,null                                     ,null                                     ,null                                  ,null                                  ,null                                  );
INSERT INTO test_procedure_dxy.t1 VALUES('2002-2-22 10:48:12.899'       ,10,32767  ,-2147483648,9223372036854775807 ,-99999999991.9999999991,9999999999991.999999999991 ,true ,'2020-10-01 12:00:01+00:00'    ,'test数据库语法查询测试！！！@TEST10-8'  ,'test数据库语法查询测试！！！@TEST10-9','test数据库语法查询测试！！！@TEST10-10','t'  ,'2','中' ,'test数据库语法查询测试！！！@TEST10-14' ,'test数据库语法查询测试！！！@TEST10-15' ,'test数据库语法查询测试！TEST10-16xaa','110101010101011010101010101010101010','110101010101011010101010101010101010');
INSERT INTO test_procedure_dxy.t1 VALUES('2003-10-1 11:48:12.1'         ,11,-32768 ,2147483647 ,-9223372036854775808,99999999991.9999999991 ,-9999999999991.999999999991,false,'1970-11-25 09:23:07.421+00:00','test数据库语法查询测试！！！@TEST11-8'  ,'test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t'  ,'2','中' ,'test数据库语法查询测试！！！@TEST11-14' ,'test数据库语法查询测试！！！@TEST11-15' ,'test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！TEST11-16xaa');
INSERT INTO test_procedure_dxy.t1 VALUES('2004-09-09 00:00:00.9+00:00'  ,12,12000  ,12000000   ,120000000000        ,-12000021.003125       ,-122209810.1131921         ,true ,'2129-3-1 12:00:00.011'        ,'aaaaaabbbbbbcccccc'                     ,'aaaaaabbbbbbcccccc'                   ,'aaaaaabbbbbbcccccc'                    ,'t'  ,'c'                    ,'z'  ,'aaaaaabbbbbbcccccc'                     ,'aaaaaabbbbbbcccccc'                     ,'aaaaaabbbbbbcccccc'                  ,'aaaaaabbbbbbcccccc'                  ,'aaaaaabbbbbbcccccc'                  );
INSERT INTO test_procedure_dxy.t1 VALUES('2004-12-31 12:10:10.911+00:00',13,23000  ,23000000   ,230000000000        ,-23000088.665120604    ,-122209810.1131921         ,true ,'2020-12-31 23:59:59.999'      ,'SSSSSSDDDDDDKKKKKK'                     ,'SSSSSSDDDDDDKKKKKK'                   ,'SSSSSSDDDDDDKKKKKK'                    ,'T'  ,'K'                    ,'B'  ,'SSSSSSDDDDDDKKKKKK'                     ,'SSSSSSDDDDDDKKKKKK'                     ,'SSSSSSDDDDDDKKKKKK'                  ,'SSSSSSDDDDDDKKKKKK'                  ,'SSSSSSDDDDDDKKKKKK'                  );
INSERT INTO test_procedure_dxy.t1 VALUES('2008-2-29 2:10:10.111'        ,14,32767  ,34000000   ,340000000000        ,-43000079.07812032     ,-122209810.1131921         ,true ,'1975-3-11 00:00:00.0'         ,'1234567890987654321'                    ,'1234567890987654321'                  ,'1234567890987654321'                   ,'1'  ,'1'                   ,'2'  ,'1234567890987654321'                    ,'1234567890987654321'                    ,'1234567890987654321'                 ,'1234567890987654321'                 ,'1234567890987654321'                 );
INSERT INTO test_procedure_dxy.t1 VALUES('2012-02-29 1:10:10.000'       ,15,-32767 ,-34000000  ,-340000000000       ,43000079.07812032      ,122209810.1131921          ,true ,'2099-9-1 11:01:00.111'        ,'数据库语法查询测试'                     ,'数据库语法查询测试'                   ,'数据库语法查询测试'                    ,'1'  ,'数'                    ,'2'  ,'数据库语法查询测试'                     ,'数据库语法查询测试'                     ,'数据库语法查询测试'                  ,'数据库语法查询测试'                  ,'数据库语法查询测试'                  );
SELECT COUNT(*) FROM test_procedure_dxy.t1;

CREATE PROCEDURE test_procedure_dxy.pro4(a CHAR(1)) BEGIN UPDATE test_procedure_dxy.t1 SET e1=1234 WHERE e8='test数据库语法查询测试！！！@TEST8-8' RETURNING e8 INTO a;SELECT a;END;
CALL test_procedure_dxy.pro4('1');
DROP PROCEDURE test_procedure_dxy.pro4;

CREATE PROCEDURE test_procedure_dxy.pro5(a INT2) BEGIN UPDATE test_procedure_dxy.t1 SET e3=9223372036854775806 WHERE id=9 RETURNING e3 INTO a;SELECT a;END;
CALL test_procedure_dxy.pro5(1);
DROP PROCEDURE test_procedure_dxy.pro5;

CREATE PROCEDURE test_procedure_dxy.pro6(a INT2)BEGIN delete from test_procedure_dxy.t1 where e3=9223372036854775806 RETURNING e3 INTO a;SELECT a;END;
CALL test_procedure_dxy.pro6(1);
DROP PROCEDURE test_procedure_dxy.pro6;


CREATE TABLE test_procedure_dxy.test1(
                                               k_ts TIMESTAMPTZ NOT NULL,
                                               id INT NOT NULL,
                                               e1 timestamp,
                                               e2 timestamptz);
INSERT INTO test_procedure_dxy.test1 VALUES('2024-3-1 23:59:59.999',3,'2024-3-1 23:59:59.999','2024-3-1 23:59:59.999');

CREATE PROCEDURE test_procedure_dxy.pro7() BEGIN DECLARE test1 TIMESTAMP(1); DECLARE test2 TIMESTAMPtz(1); SELECT e1, e2 INTO test1, test2 FROM test_procedure_dxy.test1; SELECT test1, test2;END;
CALL test_procedure_dxy.pro7();
DROP PROCEDURE test_procedure_dxy.pro7;

CREATE PROCEDURE test_procedure_dxy.pro8() BEGIN DECLARE test1 TIMESTAMP(1); DECLARE test2 TIMESTAMPtz(1); UPDATE test_procedure_dxy.test1 set k_ts = '2024-6-1 23:59:59.999' where id = 3 returning e1, e2 INTO test1, test2; SELECT test1, test2;END;
CALL test_procedure_dxy.pro8();
DROP PROCEDURE test_procedure_dxy.pro8;

CREATE PROCEDURE test_procedure_dxy.pro9() BEGIN DECLARE test1 TIMESTAMP(1); DECLARE test2 TIMESTAMPtz(1); DELETE FROM test_procedure_dxy.test1 where id = 3 returning e1, e2 INTO test1, test2; SELECT test1, test2;END;
CALL test_procedure_dxy.pro9();
DROP PROCEDURE test_procedure_dxy.pro9;

DROP DATABASE test_procedure_dxy CASCADE;

DROP DATABASE IF EXISTS test_procedure_my_rel_tz CASCADE;
DROP DATABASE IF EXISTS test_procedure_my_ts_tz CASCADE;

CREATE DATABASE test_procedure_my_rel_tz ;
CREATE TS DATABASE test_procedure_my_ts_tz ;

CREATE TABLE test_procedure_my_ts_tz.test1(
                                              k_ts TIMESTAMPTZ NOT NULL,
                                              id INT NOT NULL,
                                              e1 timestamp,
                                              e2 timestamptz,
                                              e3 timestamptz(3),
                                              e4 timestamp(6))
    TAGS (
code1 INT2 NOT NULL)
PRIMARY TAGS(code1);

INSERT INTO test_procedure_my_ts_tz.test1 VALUES('2024-1-1 08:00:00.1' ,1,'2024-1-1 08:00:00.1' ,'2024-1-1 08:00:00.1' ,'2024-1-1 08:00:00.1' ,'2024-1-1 08:00:00.1' ,1);
INSERT INTO test_procedure_my_ts_tz.test1 VALUES('2024-2-1 16:00:00.01' ,2,'2024-2-1 16:00:00.01' ,'2024-2-1 16:00:00.01' ,'2024-2-1 16:00:00.01' ,'2024-2-1 16:00:00.01' ,2);
INSERT INTO test_procedure_my_ts_tz.test1 VALUES('2024-3-1 23:59:59.999',3,'2024-3-1 23:59:59.999','2024-3-1 23:59:59.999','2024-3-1 23:59:59.999','2024-3-1 23:59:59.999',3);

CREATE TABLE test_procedure_my_rel_tz.test1(
                                               k_ts TIMESTAMPTZ NOT NULL,
                                               id INT NOT NULL,
                                               e1 timestamp,
                                               e2 timestamptz,
                                               e3 timestamptz(3),
                                               e4 timestamp(6),
                                               code1 INT2 NOT NULL);

INSERT INTO test_procedure_my_rel_tz.test1 VALUES('2024-1-1 08:00:00.1' ,1,'2024-1-1 08:00:00.1' ,'2024-1-1 08:00:00.1' ,'2024-1-1 08:00:00.1' ,'2024-1-1 08:00:00.1' ,1);
INSERT INTO test_procedure_my_rel_tz.test1 VALUES('2024-2-1 16:00:00.01' ,2,'2024-2-1 16:00:00.01' ,'2024-2-1 16:00:00.01' ,'2024-2-1 16:00:00.01' ,'2024-2-1 16:00:00.01' ,2);
INSERT INTO test_procedure_my_rel_tz.test1 VALUES('2024-3-1 23:59:59.999',3,'2024-3-1 23:59:59.999','2024-3-1 23:59:59.999','2024-3-1 23:59:59.999','2024-3-1 23:59:59.999',3);


DROP PROCEDURE IF EXISTS test_procedure_my_rel_tz.test_declare_test;
CREATE PROCEDURE test_procedure_my_rel_tz.test_declare_test(i1 INT)BEGIN DECLARE test TIMESTAMP ;SELECT  k_ts INTO test FROM test_procedure_my_ts_tz.test1 WHERE id = i1;SELECT test;END;
CALL test_procedure_my_rel_tz.test_declare_test(1);
drop procedure test_procedure_my_rel_tz.test_declare_test;

DROP DATABASE test_procedure_my_rel_tz CASCADE;
DROP DATABASE test_procedure_my_ts_tz CASCADE;
