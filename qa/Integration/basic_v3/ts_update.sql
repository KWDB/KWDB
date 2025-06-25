--updata tag
use defaultdb;
drop database update_tag cascade;
create ts database update_tag;

--primary tag is int2
create table update_tag.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool,t5 float4,t6 float8,t7 char,t8 char(100),t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128)
    ,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1);

insert into update_tag.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
                                  1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into update_tag.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
                                  32767,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','test查询  @TEST1','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into update_tag.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
                                  32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'','     ',' ','test查询','test查询！！！@TEST1','64_3','t','es1023_2','f','tes4096_2');

select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10 from update_tag.tb order by k_timestamp;

--int
select t2 from update_tag.tb order by k_timestamp;
update update_tag.tb set t2 = 1000 where t1 = 32767;
select t2 from update_tag.tb order by k_timestamp;
update update_tag.tb set t2 = null where t1 = 1;
select t2 from update_tag.tb order by k_timestamp;
update update_tag.tb set t2 = -2147483648 where t1 = 1;
select t2 from update_tag.tb order by k_timestamp;
update update_tag.tb set t2 = 2147483647 where t1 = 1;
select t2 from update_tag.tb order by k_timestamp;
prepare p1 as update update_tag.tb set t2 = $1 where t1 = $2;
execute p1(null, 1);
select t2 from update_tag.tb order by k_timestamp;
execute p1(2147483647, 1);
select t2 from update_tag.tb order by k_timestamp;
execute p1(2147483648, 1);
select t2 from update_tag.tb order by k_timestamp;

--int8
select t3 from update_tag.tb order by k_timestamp;
update update_tag.tb set t3 = 10000 where t1 = 32767;
select t3 from update_tag.tb order by k_timestamp;
update update_tag.tb set t3 = null where t1 = 1;
select t3 from update_tag.tb order by k_timestamp;
update update_tag.tb set t3 = -9223372036854775808 where t1 = 1;
select t3 from update_tag.tb order by k_timestamp;
update update_tag.tb set t3 = 9223372036854775807 where t1 = 1;
select t3 from update_tag.tb order by k_timestamp;
prepare p2 as update update_tag.tb set t3 = $1 where t1 = $2;
execute p2(null, 1);
select t3 from update_tag.tb order by k_timestamp;
execute p2(9223372036854775807, 1);
select t3 from update_tag.tb order by k_timestamp;
execute p2(9223372036854775808, 1);
select t3 from update_tag.tb order by k_timestamp;

--bool
select t4 from update_tag.tb order by k_timestamp;
update update_tag.tb set t4 = true where t1 = 32767;
select t4 from update_tag.tb order by k_timestamp;
update update_tag.tb set t4 = false where t1 = 32767;
select t4 from update_tag.tb order by k_timestamp;
update update_tag.tb set t4 = null where t1 = 32767;
select t4 from update_tag.tb order by k_timestamp;
prepare p3 as update update_tag.tb set t4 = $1 where t1 = $2;
execute p3(null, 32767);
select t4 from update_tag.tb order by k_timestamp;
execute p3(true, 32767);
select t4 from update_tag.tb order by k_timestamp;

--primary tag is int
create table update_tag.tb1(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 ,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char,t8 char(100),t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128)
    ,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t2);

insert into update_tag.tb1 values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
                                   1,2,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into update_tag.tb1 values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
                                   -32768,2147483647,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','test查询  @TEST1','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into update_tag.tb1 values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
                                   32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'','     ',' ','test查询','test查询！！！@TEST1','64_3','t','es1023_2','f','tes4096_2');

--int2
select t1 from  update_tag.tb1 order by k_timestamp;
update update_tag.tb1 set t1 = 1000 where t2 = 2147483647;
select t1 from  update_tag.tb1 order by k_timestamp;
update update_tag.tb1 set t1 = null where t2 = 2;
select t1 from  update_tag.tb1 order by k_timestamp;
update update_tag.tb1 set t1 = -32768 where t2 = 2;
select t1 from  update_tag.tb1 order by k_timestamp;
update update_tag.tb1 set t1 = 32767 where t2 = 2;
select t1 from  update_tag.tb1 order by k_timestamp;
prepare p4 as update update_tag.tb1 set t1 = $1 where t2 = $2;
execute p4(null, 2);
select t1 from update_tag.tb1 order by k_timestamp;
execute p4(32767, 2);
select t1 from update_tag.tb1 order by k_timestamp;
execute p4(32768, 2);
select t1 from update_tag.tb1 order by k_timestamp;

--float
select t5 from  update_tag.tb1 order by k_timestamp;
update update_tag.tb1 set t5 = null where t2 = 2147483647;
select t5 from  update_tag.tb1 order by k_timestamp;
update update_tag.tb1 set t5 = -9223372036854775807.12345 where t2 = 2;
select t5 from  update_tag.tb1 order by k_timestamp;
update update_tag.tb1 set t5 = 9223372036854775806.12345 where t2 = 2;
select t5 from  update_tag.tb1 order by k_timestamp;
prepare p34 as update update_tag.tb1 set t5 = $1 where t2 = $2;
execute p34(null, 2);
select t5 from update_tag.tb1 order by k_timestamp;
execute p34(9223372036854775806.12345, 2);
select t5 from update_tag.tb1 order by k_timestamp;
execute p34(-9223372036854775807.12345, 2);
select t5 from update_tag.tb1 order by k_timestamp;

--primary tag is int8
create table update_tag.tb2(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 ,t2 int ,t3 int8 not null,t4 bool,t5 float4,t6 float8,t7 char,t8 char(100),t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128)
    ,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t3);

insert into update_tag.tb2 values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
                                   1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into update_tag.tb2 values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
                                   -32768,-2147483648,9223372036854775807,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','test查询  @TEST1','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into update_tag.tb2 values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
                                   32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'','     ',' ','test查询','test查询！！！@TEST1','64_3','t','es1023_2','f','tes4096_2');

--float8
select t6 from  update_tag.tb2 order by k_timestamp;
update update_tag.tb2 set t6 = null where t3 = 9223372036854775807;
select t6 from  update_tag.tb2 order by k_timestamp;
update update_tag.tb2 set t6 = 111.922337203 where t3 = 7000;
select t6 from  update_tag.tb2 order by k_timestamp;
update update_tag.tb2 set t6 = -111.922337203 where t3 = 7000;
select t6 from  update_tag.tb2 order by k_timestamp;
prepare p5 as update update_tag.tb2 set t6 = $1 where t3 = $2;
execute p5(null, 7000);
select t6 from update_tag.tb2 order by k_timestamp;
execute p5(111.922337203, 7000);
select t6 from update_tag.tb2 order by k_timestamp;
execute p5(-111.922337203, 7000);
select t6 from update_tag.tb2 order by k_timestamp;

--primary tag is bool
create table update_tag.tb3(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 ,t2 int ,t3 int8 ,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100),t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128)
    ,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t4);

insert into update_tag.tb3 values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
                                   1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into update_tag.tb3 values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
                                   -32768,-2147483648,9223372036854775807,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','test查询  @TEST1','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into update_tag.tb3 values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,true,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
                                   32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'','     ',' ','test查询','test查询！！！@TEST1','64_3','t','es1023_2','f','tes4096_2');

--char
select t7 from  update_tag.tb3 order by k_timestamp;
update update_tag.tb3 set t7 = null where t4 = true;
select t7 from  update_tag.tb3 order by k_timestamp;
update update_tag.tb3 set t7 = true where t4 = false;
select t7 from  update_tag.tb3 order by k_timestamp;
update update_tag.tb3 set t7 = false where t4 = true;
select t7 from  update_tag.tb3 order by k_timestamp;
prepare p6 as update update_tag.tb3 set t7 = $1 where t4 = $2;
execute p6(null, true);
select t7 from update_tag.tb3 order by k_timestamp;
execute p6('b', true);
select t7 from update_tag.tb3 order by k_timestamp;
execute p6('bc', true);
select t7 from update_tag.tb3 order by k_timestamp;

--primary tag is char
create table update_tag.tb4(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 ,t2 int ,t3 int8 ,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128)
    ,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t7);

insert into update_tag.tb4 values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
                                   1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into update_tag.tb4 values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
                                   -32768,-2147483648,9223372036854775807,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','test查询  @TEST1','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into update_tag.tb4 values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,true,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
                                   32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'b','     ',' ','test查询','test查询！！！@TEST1','64_3','t','es1023_2','f','tes4096_2');

--char(n)
select t8 from  update_tag.tb4 order by k_timestamp;
update update_tag.tb4 set t8 = null where t7 = 'a';
select t8 from  update_tag.tb4 order by k_timestamp;
update update_tag.tb4 set t8 = 'test测试！！！@TEST1' where t7 = 'b';
select t8 from  update_tag.tb4 order by k_timestamp;
prepare p36 as update update_tag.tb4 set t8 = $1 where t7 = $2;
execute p36(null, 'b');
select t8 from update_tag.tb4 order by k_timestamp;
execute p36('b', 'b');
select t8 from update_tag.tb4 order by k_timestamp;
execute p36('bc', 'b');
select t8 from update_tag.tb4 order by k_timestamp;


--primary tag is char(n)
create table update_tag.tb5(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 ,t2 int ,t3 int8 ,t4 bool,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128)
    ,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t8);

insert into update_tag.tb5 values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
                                   1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into update_tag.tb5 values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
                                   -32768,-2147483648,9223372036854775807,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','test查询  @TEST1','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into update_tag.tb5 values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,true,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
                                   32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'b','     ',' ','test查询','test查询！！！@TEST1','64_3','t','es1023_2','f','tes4096_2');

--nchar
select t9 from  update_tag.tb5 order by k_timestamp;
update update_tag.tb5 set t9 = null where t8 = '     ';
select t9 from  update_tag.tb5 order by k_timestamp;
update update_tag.tb5 set t9 = 'p' where t8 = 'test测试！！！@TEST1';
select t9 from  update_tag.tb5 order by k_timestamp;
update update_tag.tb5 set t9 = '*' where t8 = 'test测试！！！@TEST1';
select t9 from  update_tag.tb5 order by k_timestamp;
prepare p7 as update update_tag.tb5 set t9 = $1 where t8 = $2;
execute p7(null, 'test测试！！！@TEST1');
select t9 from update_tag.tb5 order by k_timestamp;
execute p7('b', 'test测试！！！@TEST1');
select t9 from update_tag.tb5 order by k_timestamp;
execute p7('bcdef', 'test测试！！！@TEST1');
select t9 from update_tag.tb5 order by k_timestamp;

--primary tag is nchar
create table update_tag.tb6(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 ,t2 int ,t3 int8 ,t4 bool,t5 float4,t6 float8,t7 char ,t8 char(100) ,t9 nchar not null,t10 nchar(254),t11 varchar,t12 varchar(128)
    ,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t9);

insert into update_tag.tb6 values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
                                   1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into update_tag.tb6 values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
                                   -32768,-2147483648,9223372036854775807,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','test查询  @TEST1','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into update_tag.tb6 values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,true,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
                                   32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'b','     ','','test查询','test查询！！！@TEST1','64_3','t','es1023_2','f','tes4096_2');

--nchar(n)
select t10 from  update_tag.tb6 order by k_timestamp;
update update_tag.tb6 set t10 = null where t9 = 'e';
select t10 from  update_tag.tb6 order by k_timestamp;
update update_tag.tb6 set t10 = 'test测试！TEST1xaa' where t9 = '';
select t10 from  update_tag.tb6 order by k_timestamp;
update update_tag.tb6 set t10 = '\a' where t9 = '';
select t10 from  update_tag.tb6 order by k_timestamp;
prepare p8 as update update_tag.tb6 set t10 = $1 where t9 = $2;
execute p8(null, 'e');
select t10 from update_tag.tb6 order by k_timestamp;
execute p8('b', 'e');
select t10 from update_tag.tb6 order by k_timestamp;

--primary tag is nchar(n)
create table update_tag.tb7(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 ,t2 int ,t3 int8 ,t4 bool,t5 float4,t6 float8,t7 char ,t8 char(100) ,t9 nchar,t10 nchar(254) not null,t11 varchar,t12 varchar(128)
    ,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t10);

insert into update_tag.tb7 values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
                                   1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into update_tag.tb7 values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
                                   -32768,-2147483648,9223372036854775807,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','test查询  @TEST1','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into update_tag.tb7 values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,true,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
                                   32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'b','     ','','\a','test查询！！！@TEST1','64_3','t','es1023_2','f','tes4096_2');

--varchar
select t11 from  update_tag.tb7 order by k_timestamp;
update update_tag.tb7 set t11 = null where t10 = '\a';
select t11 from  update_tag.tb7 order by k_timestamp;
update update_tag.tb7 set t11 = '' where t10 = '\a';
select t11 from  update_tag.tb7 order by k_timestamp;
update update_tag.tb7 set t11 = 'test查询！！！@TEST1test查询！！！@TEST1test查询！！！@TEST1' where t10 = 'test测试！TEST1xaa';
select t11 from  update_tag.tb7 order by k_timestamp;
prepare p9 as update update_tag.tb7 set t11 = $1 where t10 = $2;
execute p9(null, '\a');
select t11 from update_tag.tb7 order by k_timestamp;
execute p9('b', '\a');
select t11 from update_tag.tb7 order by k_timestamp;

--primary tag is varchar
create table update_tag.tb8(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 ,t2 int ,t3 int8 ,t4 bool,t5 float4,t6 float8,t7 char ,t8 char(100) ,t9 nchar,t10 nchar(254) ,t11 varchar not null,t12 varchar(128)
    ,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t11);

insert into update_tag.tb8 values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
                                   1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a','p','vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into update_tag.tb8 values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
                                   -32768,-2147483648,9223372036854775807,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','test查询  @TEST1','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into update_tag.tb8 values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,true,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
                                   32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'b','     ','','\a','test查询！！！@TEST1','64_3','t','es1023_2','f','tes4096_2');

--varchar(n)
select t12 from  update_tag.tb8 order by k_timestamp;
update update_tag.tb8 set t12 = null where t11 = 'p';
select t12 from  update_tag.tb8 order by k_timestamp;
update update_tag.tb8 set t12 = '' where t11 = 'test查询  @TEST1';
select t12 from  update_tag.tb8 order by k_timestamp;
update update_tag.tb8 set t12 = 'test查询！！！@TEST1\' where t11 = 'test查询  @TEST1';
select t12 from  update_tag.tb8 order by k_timestamp;
prepare p10 as update update_tag.tb8 set t12 = $1 where t11 = $2;
execute p10(null, 'p');
select t12 from update_tag.tb8 order by k_timestamp;
execute p10('b', 'p');
select t12 from update_tag.tb8 order by k_timestamp;

--primary tag is varchar(n)
create table update_tag.tb9(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 ,t2 int ,t3 int8 ,t4 bool,t5 float4,t6 float8,t7 char ,t8 char(100) ,t9 nchar,t10 nchar(254) ,t11 varchar,t12 varchar(128)  not null
    ,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t12);

insert into update_tag.tb9 values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
                                   1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a','p','vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into update_tag.tb9 values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
                                   -32768,-2147483648,9223372036854775807,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','test查询  @TEST1','vvvaa64_1','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into update_tag.tb9 values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,true,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
                                   32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'b','     ','','\a','test查询！！！@TEST1','64_3','t','es1023_2','f','tes4096_2');

--bytes
select t13 from  update_tag.tb9 order by k_timestamp;
update update_tag.tb9 set t13 = null where t12 = 'vvvaa64_1';
select t13 from  update_tag.tb9 order by k_timestamp;
update update_tag.tb9 set t13 = 'b' where t12 = '64_3';
select t13 from  update_tag.tb9 order by k_timestamp;
update update_tag.tb9 set t13 = b'\x62\x63\x64' where t12 = '64_3';;
select t13 from  update_tag.tb9 order by k_timestamp;
prepare p11 as update update_tag.tb9 set t13 = $1 where t12 = $2;
execute p11(null, '64_3');
select t13 from update_tag.tb9 order by k_timestamp;
execute p11('b', '64_3');
select t13 from update_tag.tb9 order by k_timestamp;
execute p11('bc', '64_3');
select t13 from update_tag.tb9 order by k_timestamp;

--multi primary tag
use defaultdb;drop database update_tag cascade;
create ts database update_tag;
create table update_tag.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1,t4,t8,t12);
create table update_tag.tb2(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar not null,t10 nchar(254) not null,t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t2,t7,t9,t10);
insert into update_tag.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
                                  1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into update_tag.tb values ('2020-11-06 17:10:23.231','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
                                  1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into update_tag.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
                                  -32768,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','test查询  @TEST1','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into update_tag.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
                                  32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'','     ',' ','test查询','test查询！！！@TEST1','64_3','t','es1023_2','f','tes4096_2');
insert into update_tag.tb2 values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','中文  中文', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
                                   5,600,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into update_tag.tb2 values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
                                   6,100,1000,true,-10.123,100.111111,'b','TEST1 ','f','测试！TEST1xaa','test查询  @TEST1','bd64_1','y','test@测试！10_1','vwwws_1','cddde');

update update_tag.tb set t2 =null  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t3 =9223372036854775807  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t5 =922337203685.4775807  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t6 =92233.720368  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t7 ='1'  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t9 =null  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t10 ='test测试！！！@TEST1'  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t11 ='p'  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
prepare p14 as update update_tag.tb set t2 =$1  where t1 = $2 and t4 = $3 and t8 = $4 and t12 = $5;
execute p14(null, 1, false, 'test测试！！！@TEST1', 'vvvaa64_1');

update update_tag.tb2 set t1 =32767  where t2 = 600 and t7 = 'a' and t9 = 'e' and t10 = '\a';
update update_tag.tb2 set t4 =true  where t2 = 600 and t7 = 'a' and t9 = 'e' and t10 = '\a';
update update_tag.tb2 set t8 ='abc255测试1()*&^%'  where t2 = 600 and t7 = 'a' and t9 = 'e' and t10 = '\a';
update update_tag.tb2 set t12 ='abc255测试1()*&^%'  where t2 = 600 and t7 = 'a' and t9 = 'e' and t10 = '\a';
update update_tag.tb2 set t13 ='b' where t2 = 600 and t7 = 'a' and t9 = 'e' and t10 = '\a';
update update_tag.tb2 set t14 ='abc255测试1()*&^%' where t2 = 600 and t7 = 'a' and t9 = 'e' and t10 = '\a';
prepare p15 as update update_tag.tb2 set t1 =$1 where t2 = $2 and t7 = $3 and t9 = $4 and t10 = $5;
execute p15(null, 600, 'a', 'e', '\a');
execute p15(32767, 600, 'a', 'e', '\a');

update update_tag.tb2 set t15 = 'b' where t3 = 2000 and t11 = 'test查询！！！@TEST1' and t13 = '64_3' and t14 = 't';
update update_tag.tb2 set t16 = b'\x0001001001' where t3 = 2000 and t11 = 'test查询！！！@TEST1' and t13 = '64_3' and t14 = 't';
prepare p16 as update update_tag.tb2 set t15 =$1 where t3 = $2 and t11 = $3 and t13 = $4 and t14 = $5;

--set invalid value
--type mismatch
update update_tag.tb set t2 ='a'  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t3 =true  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t5 ='a_1'  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t7 = 987678  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
prepare p17 as update update_tag.tb set t2 =$1 where t1 = $2 and t4 = $3 and t8 = $4 and t12 = $5;
prepare p18 as update update_tag.tb set t3 =$1 where t1 = $2 and t4 = $3 and t8 = $4 and t12 = $5;
prepare p19 as update update_tag.tb set t5 =$1 where t1 = $2 and t4 = $3 and t8 = $4 and t12 = $5;
prepare p20 as update update_tag.tb set t7 =$1 where t1 = $2 and t4 = $3 and t8 = $4 and t12 = $5;
execute p17('a', 1, false, 'test测试！！！@TEST1', 'vvvaa64_1');
execute p18(true, 1, false, 'test测试！！！@TEST1', 'vvvaa64_1');
execute p19('a_1', 1, false, 'test测试！！！@TEST1', 'vvvaa64_1');
execute p20(987678, 1, false, 'test测试！！！@TEST1', 'vvvaa64_1');
--value out of range
update update_tag.tb set t2 =214748364823455   where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t3 =92233720368547758082345  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
prepare p21 as update update_tag.tb set t2 =$1 where t1 = $2 and t4 = $3 and t8 = $4 and t12 = $5;
prepare p22 as update update_tag.tb set t3 =$1 where t1 = $2 and t4 = $3 and t8 = $4 and t12 = $5;
execute p21(214748364823455, 1, false, 'test测试！！！@TEST1', 'vvvaa64_1');
execute p22(92233720368547758082345, 1, false, 'test测试！！！@TEST1', 'vvvaa64_1');
--width out of range
update update_tag.tb set t7 ='看看2112sdd'  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t9 ='x7839h7jj_1'  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t11 ='test测试！！！@TEST1'  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t13 ='test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1'  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
prepare p23 as update update_tag.tb set t7 =$1 where t1 = $2 and t4 = $3 and t8 = $4 and t12 = $5;
prepare p24 as update update_tag.tb set t9 =$1 where t1 = $2 and t4 = $3 and t8 = $4 and t12 = $5;
prepare p25 as update update_tag.tb set t11 =$1 where t1 = $2 and t4 = $3 and t8 = $4 and t12 = $5;
prepare p26 as update update_tag.tb set t13 =$1 where t1 = $2 and t4 = $3 and t8 = $4 and t12 = $5;
execute p23('看看2112sdd', 1, false, 'test测试！！！@TEST1', 'vvvaa64_1');
execute p24('x7839h7jj_1', 1, false, 'test测试！！！@TEST1', 'vvvaa64_1');
execute p25('test测试！！！@TEST1', 1, false, 'test测试！！！@TEST1', 'vvvaa64_1');
execute p26('test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1test测试！！！@TEST1', 1, false, 'test测试！！！@TEST1', 'vvvaa64_1');
-- column match
update update_tag.tb set t2 =1  where e1 = 1 and e4 = false;
prepare p27 as update update_tag.tb set t2 =$1  where e1 = $2 and e4 = $3;

-- other tag match
update update_tag.tb set t2 =1  where t3 = 7000;
update update_tag.tb set t2 =1  where t3 = 2000 and t11 = 'test查询！！！@TEST1' and t13 = '64_3' and t14 = 't';
prepare p28 as update update_tag.tb set t2 =$1  where t3 = $2;
prepare p29 as update update_tag.tb set t2 =$1  where t3 = $2 and t11 = $3 and t13 = $4 and t14 = $5;

--operator except =，e.g.>= 、<= 、between and etc.
update update_tag.tb set t2 =null  where t1 >= 1 and t4 != false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t3 =9223372036854775807  where t2 is null and t4 = false;
update update_tag.tb set t5 =922337203685.4775807 where t12 like '%\\';
update update_tag.tb set t6 =92233.720368  where t1 in(1,2) or t12 in('vvvaa64_1');
update update_tag.tb set t7 ='1'  where t1 < 500 group by t1,t2,t3 order by t1;
update update_tag.tb set t9 =null  where t4 in(true,false) group by t4;
update update_tag.tb set t10 ='test测试！！！@TEST1'  where t1 between 0 and 10;
update update_tag.tb set t11 ='p'  where t1 between 3 and 6 group by t1,t5,t12;
update update_tag.tb set t2 =null  where t1 = 1 or t4 = false or t8 = 'test测试！！！@TEST1' or t12 = 'vvvaa64_1';
prepare p30 as update update_tag.tb set t3 =9223372036854775807  where t1 > $1;
prepare p31 as update update_tag.tb set t3 =9223372036854775807  where t12 like $1;
prepare p32 as update update_tag.tb set t3 =9223372036854775807  where t1 in ($1, $2);
prepare p33 as update update_tag.tb set t3 =9223372036854775807  where t1 between $1 and $2;

update update_tag.tb set t2 =1  where t1 = t1+1 and t4 = t4 and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';;

--type mismatch
update update_tag.tb set t2 =null  where t1 = 'op9' and t4 = '11' and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';

--tag does not exist
update update_tag.tb set t111 =null  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';

--use part primary tag
update update_tag.tb set t2 =null  where t1 = 1 and t4 = false ;
update update_tag.tb set t7 ='1'  where t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';

--update primary tag
update update_tag.tb set t1 =1000  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t4 =true  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t8 = 't'  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set t12 = 'v1'  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';

--update column
update update_tag.tb set e2 =1000  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set e5 =true  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';
update update_tag.tb set e14 = 't'  where t1 = 1 and t4 = false and t8 = 'test测试！！！@TEST1' and t12 = 'vvvaa64_1';

--fix bug 36130
create table update_tag.t_36130(ts timestamp not null, a int) tags(b int not null, c int2 , d int4, e int8, "F" int2) primary tags(b);
insert into update_tag.t_36130 values(now(), 1, 1, 1, 1, 1, 1);
update update_tag.t_36130 set c = 32768 where b = 1;
update update_tag.t_36130 set d = 2147483648 where b = 1;
update update_tag.t_36130 set e = 9223372036854775808 where b = 1;
update update_tag.t_36130 set f = 32768 where b = 1;
update update_tag.t_36130 set "F" = 32768 where b = 1;

--fix bug 39137
create table update_tag.t11(ts timestamp not null ,a int) tags( b int2 not null, c int) primary tags(b);
insert into update_tag.t11 values('2024-07-03 00:00:00', 1, 1, 1);
alter table update_tag.t11 add column d int;
update update_tag.t11 set c = 2 where b = 1;
alter table update_tag.t11 drop column a;
update update_tag.t11 set c = 3 where b = 1;

---fix bug 45329
create table update_tag.t12(k_timestamp timestamptz not null,e1 int2) tags (t1 int2 not null,t2 int not null, t3 int) primary tags(t1,t2);
insert into update_tag.t12 values('2020-11-06 17:10:23', 1, 1, 1, 1);
insert into update_tag.t12 values('2020-11-06 17:10:24', 2, 2, 1, 1);
insert into update_tag.t12 values('2020-11-06 17:10:25', 2, 2, 1, 1);
update update_tag.t12 set t3 = 3 where e1 = 1;
update update_tag.t12 set t3 = 3 where t1 = 1 and e1 = 1;
update update_tag.t12 set t3 = 3 where t2 = 1 and e1 = 1;
update update_tag.t12 set t3 = 3 where t1 = 1 and t2 = 1 and e1 = 1;
update update_tag.t12 set t3 = 3 where t1 = 1 and t2 = 1 and k_timestamp = '2020-11-06 17:10:23';

drop database update_tag cascade;