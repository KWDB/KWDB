create ts database ts_db;

create table ts_db.t1 (kt timestamptz not null, s1 varchar(8) not null,s2 char(8),s3 varbytes(8)) tags (t1 int2 not null) primary tags(t1);
insert into ts_db.t1 values (now(), 'var', ' 你好2' , 'E''\\x01' , 1);
insert into ts_db.t1 values (now(), 'var', '\U' , 'E''\\x02' ,1);

select ltrim(s2,'你 ') from ts_db.t1;
select ltrim(s2,'\U你asdasdadasd\u ') from ts_db.t1;
select ltrim(s2,'\u你\U ') from ts_db.t1;
select ltrim(s2,'\n你\t ') from ts_db.t1;
select ltrim(s2,'\x你\0 ') from ts_db.t1;
select ltrim(s2,'\\你123\\ ') from ts_db.t1;
select ltrim(s2,'\\你123\\ ') from ts_db.t1;

select rtrim(s2,'好2x') from ts_db.t1;
select rtrim(s2,'\U你\u ') from ts_db.t1;
select rtrim(s2,'\u你\U ') from ts_db.t1;
select rtrim(s2,'\n你\t ') from ts_db.t1;
select rtrim(s2,'\x你\0 ') from ts_db.t1;
select rtrim(s2,'\\你123\\ ') from ts_db.t1;
select rtrim(s2,'\\你123\\ ') from ts_db.t1;

select substr(s2,-1) from ts_db.t1;
select substr(s2,-1.1) from ts_db.t1;
select substr(s2,1.1) from ts_db.t1;
select substr(s2,0) from ts_db.t1;
select substr(s2,1) from ts_db.t1;
select substr(s2,2) from ts_db.t1;
select substr(s2,0,1) from ts_db.t1;
select substr(s2,1,10) from ts_db.t1;
select substr(s2,2,999) from ts_db.t1;
select substr(s2,0,-1) from ts_db.t1;
select substr(s2,1,-1.3) from ts_db.t1;
select substr(s2,1,-1.1) from ts_db.t1;
select substr(s2,2,0) from ts_db.t1;

select length(s2) from ts_db.t1;
select char_length(s2) from ts_db.t1;
select character_length(s2) from ts_db.t1;
select bit_length(s2) from ts_db.t1;
select octet_length(s2) from ts_db.t1;
select get_bit(s3,1) from ts_db.t1;
select get_bit(s3,2) from ts_db.t1;
select initcap(s1) from ts_db.t1;

select concat(s2,'哈哈',s2) from ts_db.t1;
select concat(s2,'哈哈\Usadaidapouisad',s2) from ts_db.t1;

select lpad(s2,0) from ts_db.t1;
select lpad(s2,20) from ts_db.t1;
select lpad(s2,-12) from ts_db.t1;
select lpad(s2,1.2) from ts_db.t1;
select lpad(s2,11,'再见3') from ts_db.t1;
select lpad(s2,0,'再见3') from ts_db.t1;
select lpad(s2,-123,'再见3') from ts_db.t1;
select lpad(s2,20,'\U再见3sdasdosaoiduaoisud') from ts_db.t1;
select lpad(s2,20,'\U再见3sd\u') from ts_db.t1;
select lpad(s2,20,'\U再见3\t') from ts_db.t1;
select lpad(s2,20,'\U再见3\0') from ts_db.t1;

select rpad(s2,11,'再见3') from ts_db.t1;
select rpad(s2,0) from ts_db.t1;
select rpad(s2,20) from ts_db.t1;
select rpad(s2,-12) from ts_db.t1;
select rpad(s2,1.2) from ts_db.t1;
select rpad(s2,11,'再见3') from ts_db.t1;
select rpad(s2,0,'再见3') from ts_db.t1;
select rpad(s2,-123,'再见3') from ts_db.t1;
select rpad(s2,20,'\U再见3sdasdosaoiduaoisud') from ts_db.t1;
select rpad(s2,20,'\U再见3sd\u') from ts_db.t1;
select rpad(s2,20,'\U再见3\t') from ts_db.t1;
select rpad(s2,20,'\U再见3\0') from ts_db.t1;

select lower(s1),lower(s2) from ts_db.t1;

drop database ts_db cascade;

create ts database ts_db;
create table ts_db.t1 (kt timestamptz not null,a int, s1 varbytes(20) not null,s2 char(20),s3 varchar(20)) tags (t1 int2 not null) primary tags(t1);
insert into ts_db.t1 values (now(), 12323, 'hello,世界', 'var', ' 你好2' ,1);
select chr(12323);
select chr(a) from ts_db.t1;
explain select chr(a) from ts_db.t1;
select encode('hello,世界','escape');
select encode(s1,'escape') from ts_db.t1;
explain select encode(s1,'escape') from ts_db.t1;
select decode(' 你好2','escape');
select decode(s3,'escape') from ts_db.t1;
explain select decode(s3,'escape') from ts_db.t1;
select decode('var','escape');
select decode(s2,'escape') from ts_db.t1;
explain select decode(s2,'escape') from ts_db.t1;
insert into ts_db.t1 values (now(), 1114112, 'hello,世界', 'var', ' 你好2\n' ,1);
select chr(1114112);
select chr(a) from ts_db.t1;
select encode('hello,世界','test');
select encode(s1,'error') from ts_db.t1;
select decode(s3,'escape') from ts_db.t1;
drop database ts_db cascade;

create ts database test_function_2;
create table test_function_2.t1(k_timestamp timestamp not null,e1 timestamp,e2 int2,e3 int4,e4 int8,e5 float4,e6 float8,e7 varbytes(20)) ATTRIBUTES (code1 INT2 NOT NULL,code2 INT4,code3 INT8,code4 FLOAT4 ,code5 FLOAT8,code6 BOOL,code7 VARCHAR,code8 VARCHAR(128) NOT NULL,code9 VARBYTES,code10 varbytes(60),code11 VARCHAR,code12 VARCHAR(60),code13 CHAR(2),code14 CHAR(1023) NOT NULL,code15 NCHAR,code16 NCHAR(254) NOT NULL) PRIMARY TAGS(code1,code14,code8,code16);
insert into test_function_2.t1 values ('2021-04-01 15:00:00',111111110000,1000,1000000,100000000,100000.101,1000000.10101111,b'0',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
insert into test_function_2.t1 values ('2020-05-05 17:00:10.123',222222220000,2000,2000000,200000000,200000.202,2000000.2020222,b'1',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
insert into test_function_2.t1 values ('2022-05-10 17:15:00',333333330000,3000,2000000,300000000,300000.303,3000000.3030333,b'-1',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
insert into test_function_2.t1 values ('2023-01-01 18:00:00',333333330000,3000,3000000,400000000,400000.404,4000000.4040444,b'abc',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
insert into test_function_2.t1 values ('2022-06-01 10:00:00',555555550000,5000,5000000,500000000,500000.505,5000000.5050555,b'1000000000',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
insert into test_function_2.t1 values ('2022-10-01 17:00:00',666666660000,6000,6000000,600000000,600000.606,6000000.6060666,b'-1000000000',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
insert into test_function_2.t1 values ('2020-11-06 17:10:23',444444440000,7000,7000000,700000000,700000.707,7000000.1010111,b'20.20',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
insert into test_function_2.t1 values ('2022-08-16 10:23:05.123',888888880000,8000,8000000,800000000,800000.808,8000000.2020222,b'-8000000.2020222',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
insert into test_function_2.t1 values ('2023-05-10 09:04:18.223',null,null,null,null,null,null,null,-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
insert into test_function_2.t1 values ('2023-05-10 11:04:18.223',null,null,null,null,null,null,null,-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
insert into test_function_2.t1 values ('2023-04-10 08:04:15.783',666666660000,5000,5000000,600000000,500000.505,5000000.5050555,b'500000.505',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
insert into test_function_2.t1 values ('2023-04-10 08:04:16.783',666666660000,5000,5000000,600000000,500000.505,5000000.5050555,b',',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
insert into test_function_2.t1 values ('2023-04-10 08:04:17.783',666666660000,5000,5000000,600000000,500000.505,5000000.5050555,b'中',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
insert into test_function_2.t1 values ('2023-04-10 08:04:18.783',666666660000,5000,5000000,600000000,500000.505,5000000.5050555,b'a/0',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
insert into test_function_2.t1 values ('2023-04-10 08:04:19.783',666666660000,5000,5000000,600000000,500000.505,5000000.5050555,b'\xaa',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');


select e7,encode(e7,'hex') from test_function_2.t1 order by k_timestamp;
drop database test_function_2;