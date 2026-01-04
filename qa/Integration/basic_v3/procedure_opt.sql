drop database if exists d1;
create database d1;
use d1;
create table t1(a int, b int);
create table t2(a int, b int, c int);
insert into t1 values (1, 2), (3, 4);
insert into t2 values (1, 2, 3), (4, 5, 6);
create table t3(a int, b int, c int);
create table t4(a int);

-- 1.
delimiter \\
create procedure pc1()
begin
declare a1 int;
set a1=123;

set @b1:=456;
select @b1;
set @b1=((a1)*3);
select @b1;

declare c1 string;
set c1='abc';

set @d1:='def';
set @d1=c1;
select @d1;

end \\
delimiter ;

call pc1();
drop procedure pc1;


-- 2.
select @v;

delimiter \\
create procedure pc2()
begin
set @v=10;
end \\
delimiter ;

select @v;

call pc2();
select @v;
drop procedure pc2;

-- 3.
create procedure pc3() begin start transaction;set @vv = 4;rollback;end;

call pc3();

select @vv;
drop procedure pc3;

-- 4.
set @v4 = 1;
create procedure pc4() begin set @v4 = 'abc'; select @v4; end;

call pc4();
drop procedure pc4;
select @v4;

-- 5.
create procedure pc5() begin set @v5 = 1; select @v5; end;
select @v5;
call pc5();
select @v5;
drop procedure pc5;

-- 6.
create procedure pc6() begin set @v6 = 1; select @v6; set @v6 = 6; select @v6+3 where @v6>0 group by @v6+1 having @v6 <10; end;

call pc6();
drop procedure pc6;

-- 7.
create procedure pc7() begin set @v7=7; select @v7; set @v7=8;select @v7; end;

call pc7();
drop procedure pc7;

-- 8.
create procedure pc8() begin set @v8=8; select @v8; set @v8='abc'; end;

call pc8();
drop procedure pc8;

-- 11.
delimiter \\
create procedure pc11()
begin
PREPARE p1 AS OPT PLAN '
(Root
  (Scan [ (Table "t2") (Cols "a,b") ])
  (Presentation "a,b")
  (OrderingChoice "+a")
)';
end \\
delimiter ;

call pc11();
drop procedure pc11;


-- 12.
delimiter \\
create procedure pc12()
begin
PREPARE p12 AS select 1;
end \\
delimiter ;

call pc12();
execute p12;
drop procedure pc12;

-- 13.
PREPARE p13 AS select 1;
delimiter \\
create procedure pc13()
begin
PREPARE p13 AS select 2;
end \\
delimiter ;

call pc13();
drop procedure pc13;

-- 14.
delimiter \\
create procedure pc14()
begin
PREPARE p14 AS select 'PREPARE pp14 as "SELECT 1"';
end \\
delimiter ;

call pc14();
drop procedure pc14;

-- 16.
delimiter \\
create procedure pc16()
begin
PREPARE p16 AS insert into t4 values (16);
execute p16;
end \\
delimiter ;

call pc16();
select a from t4 order by a;
drop procedure pc16;

-- 17.
delimiter \\
create procedure pc17()
begin
PREPARE p17 AS update t4 set a=17 where a=16;
execute p17;
end \\
delimiter ;

call pc17();
select a from t4 order by a;
drop procedure pc17;

-- 18.
delimiter \\
create procedure pc18()
begin
PREPARE p18 AS upsert into t4 values (18);
execute p18;
end \\
delimiter ;

call pc18();
select a from t4 order by a;
drop procedure pc18;

-- 19.
delimiter \\
create procedure pc19()
begin
PREPARE p19 AS select a from t4 order by a;
execute p19;
end \\
delimiter ;

call pc19();
drop procedure pc19;

-- 20.
delimiter \\
create procedure pc20()
begin
PREPARE p20 AS delete from t4 where 1=1;
execute p20;
end \\
delimiter ;

call pc20();
select a from t4 order by a;
drop procedure pc20;


create procedure pc102() begin declare a int8; set a=102; set @v102=a;prepare p102 as select 102 where 102 = $1; execute p102(@v102); deallocate p102; end;
call pc102();
drop procedure pc102;



-- 21.
delimiter \\
create procedure pc21(min_a int)
begin
set @param = min_a;
PREPARE p21 AS select a, b from t1 where a >= $1;
execute p21(@param);
DEALLOCATE PREPARE p21;
end \\
delimiter ;
call pc21(2);
drop procedure pc21;

-- 22.
delimiter \\
create procedure pc22(batch_num int)
begin
set @v22 = 1;
while @v22 <= batch_num do
set @a_val = @v22;
set @b_val = @v22 * 5;
set @c_val = @b_val + 3;
PREPARE p22 AS insert into t3(a, b, c) values($1, $2, $3);
execute p22(@a_val, @b_val, @c_val);
set @v22 = @v22 + 1;
endwhile;
DEALLOCATE PREPARE p22;
select * from t3 order by a;
end \\
delimiter ;
call pc22(3);
drop procedure pc22;

-- 23.
delimiter \\
CREATE PROCEDURE pc23()
BEGIN
SET @v23 = 1;
WHILE @v23 < 5 DO
SELECT @v23;
SET @v23=@v23+1;
ENDWHILE;
END\\
delimiter ;
CALL pc23();
drop procedure pc23;

-- 24.
create table t24 (e1 int, e2 timestamp);
insert into t24 values (1, '2000-1-1');

delimiter \\
CREATE PROCEDURE pc24()
BEGIN
SET @v24 = 0;
update t24 set e1 = 2 where e1 = 1 returning e1 into @v24;
select @v24;

SET @vv24 = timestamp'1999-1-1';
delete from t24 where e1 = 2 returning e2 into @vv24;
select @vv24;

END\\
delimiter ;
CALL pc24();
drop procedure pc24;
drop table t24 cascade;

delimiter \\
CREATE PROCEDURE pc25()
BEGIN
prepare p25 as select 1;
execute p25;
deallocate p25;
END\\
delimiter ;
CALL pc25();
CALL pc25();
drop procedure pc25;

delimiter \\
CREATE PROCEDURE pc27()
BEGIN
SET @v27=27;
SET @vv27=270;
prepare p27 as select @v27+@vv27 from t1 order by a;
execute p27;
deallocate prepare p27;
END \\
delimiter ;

delimiter \\
CREATE PROCEDURE pc28()
BEGIN
SET @v28=28;
prepare p28 as insert into t1 values(10, @v28);
execute p28;
deallocate prepare p28;
END\\
delimiter ;

delimiter \\
CREATE PROCEDURE pc26()
BEGIN
SET @v26=26;
prepare p26 as update t1 set a=@v26 where a=100;
execute p26;
deallocate prepare p26;
END \\
delimiter ;

delimiter \\
CREATE PROCEDURE pc29()
BEGIN
SET @v29=29;
prepare p29 as delete from t1 where a=@v29;
execute p29;
deallocate prepare p29;
END \\
delimiter ;

delimiter \\
CREATE PROCEDURE pc30()
BEGIN
SET @v30=1;
prepare p30 as select a from t1 order by @v30;
execute p30;
deallocate prepare p30;
END \\
delimiter ;

delimiter \\
CREATE PROCEDURE pc31()
BEGIN
SET @v31=31;
prepare p31 as update t1 set a = $1 where a = 1;
execute p31(@v31);
deallocate prepare p31;
END \\
delimiter ;
CALL pc31();
drop procedure pc31;

delimiter \\
CREATE PROCEDURE pc32()
BEGIN
SET @v32=1;
prepare p32 as update t1 set a = $1 where a = 31;
execute p32(@v32);
deallocate prepare p32;
END \\
delimiter ;
CALL pc32();
drop procedure pc32;

select * from t1 order by a;



drop database if exists d1 cascade;

create ts database d1;
use d1;
create table t1(ts timestamp not null, e1 int) tags (tag1 int not null, tag2 int) primary tags (tag1);

create procedure pc33() begin set @v33=33; insert into t1 values ('2000-1-1', @v33, 10, 20);end;
call pc33();
drop procedure pc33;
select * from t1 order by e1;

create procedure pc38() begin set @v38=38; update t1 set tag2=@v38 where tag1=10;end;
call pc38();
drop procedure pc38;
select * from t1 order by e1;

create procedure pc34() begin set @v34=10; delete from t1 where tag1=@v34;end;
call pc34();
drop procedure pc34;
select * from t1 order by e1;

drop database if exists d1 cascade;

create database d1;
use d1;
create table t1 (e1 int primary key);
insert into t1 values (1);
insert into t1 values (2);
insert into t1 values (3);

SET @v35=35;

delimiter \\
create procedure pc35() begin
DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
BEGIN
    SET @v35=350;
ENDHANDLER;
SELECT @v35;
insert into t1 values(1);
SELECT @v35;
end\\
delimiter ;
call pc35();
drop procedure pc35;


delimiter \\
create procedure pc36() begin
SET @v36=36;
DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
BEGIN
    SET @v36=360;
ENDHANDLER;
SELECT @v36;
insert into t1 values(1);
SELECT @v36;
end\\

delimiter ;
call pc36();
drop procedure pc36;


delimiter \\
create procedure pc37() begin
DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
BEGIN
    SET @v37=37;
ENDHANDLER;
SELECT @v37;
insert into t1 values(1);
SELECT @v37;
end\\

delimiter ;
call pc37();
drop procedure pc37;

drop database if exists d1 cascade;




drop database if exists test_procedure_opt_dxy_ts cascade;
CREATE ts DATABASE test_procedure_opt_dxy_ts;
CREATE TABLE test_procedure_opt_dxy_ts.t1(
                                             k_timestamp TIMESTAMPTZ NOT NULL,
                                             id INT NOT NULL,
                                             e1 INT2,
                                             e2 INT,
                                             e3 INT8,
                                             e4 FLOAT4,
                                             e5 FLOAT8,
                                             e6 BOOL,
                                             e7 TIMESTAMPTZ,
                                             e8 CHAR(1023),
                                             e9 NCHAR(255),
                                             e10 VARCHAR(4096),
                                             e11 CHAR,
                                             e12 CHAR(255),
                                             e13 NCHAR,
                                             e14 NVARCHAR(4096),
                                             e15 VARCHAR(1023),
                                             e16 NVARCHAR(200),
                                             e17 NCHAR(255),
                                             e18 CHAR(200),
                                             e19 VARBYTES,
                                             e20 VARBYTES(60),
                                             e21 VARCHAR,
                                             e22 NVARCHAR)
    ATTRIBUTES (
            code1 INT2,code2 INT,code3 INT8,
            code4 FLOAT4 ,code5 FLOAT8,
                        code6 BOOL,
                        code7 VARCHAR,code8 VARCHAR(128) NOT NULL,
                        code9 VARBYTES,code10 VARBYTES(60),
                        code11 VARCHAR,code12 VARCHAR(60),
                        code13 CHAR(2),code14 CHAR(1023) NOT NULL,
                        code15 NCHAR,code16 NCHAR(254) NOT NULL)
PRIMARY TAGS(code14,code8,code16);
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES(0,1,0,0,0,0,0,true,0,'','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','','');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES(1,2,0,0,0,0,0,true,999999,'          ','          ','          ',' ','          ',' ',' ','          ','          ','          ',' ',' ','          ','          ','          ',0,0,0,0,0,TRUE,'          ',' ',' ','          ','          ','          ','  ','          ',' ','          ');

delimiter \\
CREATE PROCEDURE test_procedure_opt_dxy_ts.pro1()
BEGIN
SET @a=24;
SET @b=123.123::float;
SET @c=TRUE;
SET @d='dxydxydxydxydxy';
SET @e='\x647879647879647879647879647879';
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES('1989-2-28 00:00:00',@a,20002,20000002,200000000002,-20873209.0220322201,@b,@c,123,@d,'test数据库语法查询测试！！！@TEST4-9','test数据库语法查询测试！！！@TEST4-10','t','test数据库语法查询测试！！！@TEST4-12','中','test数据库语法查询测试！！！@TEST4-14','test数据库语法查询测试！！！@TEST4-15','test数据库语法查询测试！TEST4-16xaa','test数据库语法查询测试！！！@TEST4-17','test数据库语法查询测试！！！@TEST4-18',b'\xca',@e,'test数据库语法查询测试！！！@TEST4-21','test数据库语法查询测试！！！@TEST4-22',20002,-20000002,200000000002,-20873209.0220322201,22012110.113011921,true,'test数据库语法查询测试！！！@TEST4-7','test数据库语法查询测试！！！@TEST4-8',b'\xbb','test数据库语法查询测试！！！@TEST4-10','test数据库语法查询测试！！！@TEST4-11','test数据库语法查询测试！！！@TEST4-12','t3','test数据库语法查询测试！！！@TEST4-14','中','test数据库语法查询测试！！！@TEST4-16');
END\\

delimiter ;
call test_procedure_opt_dxy_ts.pro1();
drop procedure test_procedure_opt_dxy_ts.pro1;
select id from test_procedure_opt_dxy_ts.t1 where id=24 order by id;

delimiter \\
CREATE PROCEDURE test_procedure_opt_dxy_ts.pro1()
BEGIN
SET @vpro1=100;
UPDATE test_procedure_opt_dxy_ts.t1 SET code2=@vpro1 WHERE code8='\0\0中文te@@~eng TE./。\0\0\0' AND code14='\0\0中文te@@~eng TE./。\0\0\0' AND code16='\0\0中文te@@~eng TE./。\0\0\0';SELECT code2,code8 FROM test_procedure_opt_dxy_ts.t1 WHERE code8='\0\0中文te@@~eng TE./。\0\0\0';
END \\

delimiter ;
call test_procedure_opt_dxy_ts.pro1();
drop procedure test_procedure_opt_dxy_ts.pro1;
select code2 from test_procedure_opt_dxy_ts.t1 where code2=100 order by code2;


delimiter \\
CREATE PROCEDURE test_procedure_opt_dxy_ts.pro1()
BEGIN
SET @vpro2='\0\0中文te@@~eng TE./。\0\0\0';
DELETE FROM test_procedure_opt_dxy_ts.t1 WHERE code8=@vpro2 AND code14=@vpro2 AND code16='\0\0中文te@@~eng TE./。\0\0\0';SELECT code2,code8 FROM test_procedure_opt_dxy_ts.t1 WHERE code8='\0\0中文te@@~eng TE./。\0\0\0';
END \\

delimiter ;
call test_procedure_opt_dxy_ts.pro1();
drop procedure test_procedure_opt_dxy_ts.pro1;
select id from test_procedure_opt_dxy_ts.t1 WHERE code8=@vpro2 AND code14=@vpro2 AND code16='\0\0中文te@@~eng TE./。\0\0\0';SELECT code2,code8 FROM test_procedure_opt_dxy_ts.t1 WHERE code8='\0\0中文te@@~eng TE./
。\0\0\0' order by id;

drop database if exists test_procedure_opt_dxy_ts cascade;

drop database if exists d2 cascade;
create database d2;
use d2;
create table t2(e1 int);


delimiter \\
CREATE PROCEDURE pc39()
BEGIN
SET @v39=39;
insert into t2 values(@v39);
END \\

delimiter ;
call pc39();
drop procedure pc39;
select * from t2 order by e1;


delimiter \\
CREATE PROCEDURE pc40()
BEGIN
SET @v40=40;
update t2 set e1=@v40 where e1 = 39;
END \\

delimiter ;
call pc40();
drop procedure pc40;
select * from t2 order by e1;


delimiter \\
CREATE PROCEDURE pc41()
BEGIN
SET @v41=40;
delete from t2 where e1=@v41;
END \\

delimiter ;
call pc41();
drop procedure pc41;
select * from t2 order by e1;

drop database if exists d2 cascade;

create database d2;
use d2;
create table t1(e1 int, e2 int);
insert into t1 values (1, 2);

delimiter \\
CREATE PROCEDURE pc42()
BEGIN
prepare p42 as select e1 from t1 where e1=$1 and e2=$2;
execute p42(1, 2);

prepare p420 as select e1 from t1 where e1=$1;
execute p420(1);
END \\

delimiter ;
call pc42();
drop procedure pc42;

drop database if exists d2 cascade;

DROP DATABASE IF EXISTS test_procedure_opt1_my1_rel CASCADE;
DROP DATABASE IF EXISTS test_procedure_opt1_my1_ts CASCADE;
CREATE DATABASE test_procedure_opt1_my1_rel;
CREATE TS DATABASE test_procedure_opt1_my1_ts;
CREATE TABLE test_procedure_opt1_my1_rel.t1(
                                               k_ts TIMESTAMPTZ NOT NULL,
                                               id INT NOT NULL,
                                               e1 INT2,
                                               e2 INT,
                                               e3 INT8,
                                               e4 FLOAT4,
                                               e5 FLOAT8,
                                               e6 BOOL,
                                               e7 TIMESTAMP,
                                               e8 CHAR(1023),
                                               e9 NCHAR(255),
                                               e10 VARCHAR(4096),
                                               e11 CHAR,
                                               e12 CHAR(255),
                                               e13 NCHAR,
                                               e14 NVARCHAR(4096),
                                               e15 VARCHAR(1023),
                                               e16 NVARCHAR(200),
                                               e17 NCHAR(255),
                                               e18 CHAR(200),
                                               e19 VARBYTES,
                                               e20 VARBYTES,
                                               e21 STRING,
                                               e22 NVARCHAR,
                                               code1 INT2 NOT NULL,
                                               code2 INT,code3 INT8,
                                               code4 DECIMAL(1000,10) ,
                                               code5 DECIMAL,
                                               code6 BOOL,
                                               code7 VARCHAR,
                                               code8 VARCHAR(128) NOT NULL,
                                               code9 VARBYTES,
                                               code10 VARBYTES,
                                               code11 VARCHAR,
                                               code12 VARCHAR(60),
                                               code13 CHAR(2),
                                               code14 CHAR(1023) NOT NULL,
                                               code15 NCHAR,
                                               code16 NCHAR(254) NOT NULL );
INSERT INTO test_procedure_opt1_my1_rel.t1 VALUES('1970-01-01 00:00:00+00:00',1,0,0,0,0,0,true,'1970-01-01 00:16:39.999+00:00','','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','','');

CREATE TABLE test_procedure_opt1_my1_ts.t1(
                k_ts   TIMESTAMPTZ NOT NULL,
                id     INT NOT NULL,
                e1     INT2,
                e2     INT,
                e3     INT8,
                e4     FLOAT4,
                e5     FLOAT8,
                e6     BOOL,
                e7     TIMESTAMP,
                e8     CHAR(1023),
                e9     NCHAR(255),
                e10    VARCHAR(4096),
                e11    CHAR,
                e12    CHAR(255),
                e13    NCHAR,
                e14    NVARCHAR(4096),
                e15    VARCHAR(1023),
                e16    NVARCHAR(200),
                e17    NCHAR(255),
                e18    CHAR(200),
                e19    VARBYTES,
                e20    VARBYTES(60),
                e21    VARCHAR,
                e22    NVARCHAR)
TAGS(
                code1  INT2 NOT NULL,
                code2  INT,
                code3  INT8,
                code4  FLOAT4 ,
                code5  FLOAT8,
                code6  BOOL,
                code7  VARCHAR,
                code8  VARCHAR(128) NOT NULL,
                code9  VARBYTES,
                code10 VARBYTES(60),
                code11 VARCHAR,
                code12 VARCHAR(60),
                code13 CHAR(2),
                code14 CHAR(1023) NOT NULL,
                code15 NCHAR,
                code16 NCHAR(254) NOT NULL)
PRIMARY TAGS(code1,code14,code8,code16);
INSERT INTO test_procedure_opt1_my1_ts.t1 VALUES(0,1,0,0,0,0,0,true,0,'','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','','');

delimiter \\
CREATE PROCEDURE test_procedure_opt1_my1_ts.test_prepare(id1 INT2,id2 INT4,id3 INT8,id4 FLOAT4,id5 FLOAT8,id6 CHAR(100),id7 NCHAR(100),id8 VARCHAR(100),id9 NVARCHAR(100),id10 DECIMAL(10,5))
BEGIN
SET @id1 = id1;
SET @id2 = id2;
SET @id3 = id3;
SET @id4 = id4;
SET @id5 = id5;
SET @id6 = id6;
SET @ID7 = id7;
SET @id8 = id8;
SET @id9 = id9;
SET @id10 = id10;
PREPARE test1 as
SELECT tst1.id,tst1.code3,tst1.code15,relt1.e20
FROM test_procedure_opt1_my1_rel.t1 relt1 , test_procedure_opt1_my1_ts.t1 tst1
WHERE
    relt1.id = tst1.id AND
    relt1.code3 = tst1.code3 AND
    tst1.code3 >= $1 AND
    tst1.e4 NOT IN ($2) AND
    tst1.code16 NOT IN($3) AND
    tst1.e18 NOT LIKE $3 AND
    relt1.code4 !=$4 ;
EXECUTE test1(@id3,@id4,@id9,@id10);

PREPARE test2 as
SELECT * FROM test_procedure_opt1_my1_ts.t1 WHERE id IN
(SELECT id FROM test_procedure_opt1_my1_rel.t1 WHERE id = $1);
EXECUTE test2(@id1);
END \\

delimiter ;
call test_procedure_opt1_my1_ts.test_prepare(1,2,3,4,5,'6','7','8','9',10);
drop procedure test_procedure_opt1_my1_ts.test_prepare;

DROP DATABASE IF EXISTS test_procedure_opt1_my1_rel CASCADE;
DROP DATABASE IF EXISTS test_procedure_opt1_my1_ts CASCADE;

drop database if exists d1 cascade;

create ts database d1;
use d1;
create table t1(ts timestamp not null, e1 int) tags (tag1 int not null, tag2 int) primary tags (tag1);
insert into t1 values('2000-1-1', 1, 10, 20);

delimiter \\
CREATE PROCEDURE pc43()
BEGIN
SET @v43 = 10;
UPDATE t1 SET tag2=200 WHERE tag1=@v43;
select * from t1 order by ts;
END\\
delimiter ;

call pc43();
drop procedure pc43;
drop database if exists d1 cascade;
