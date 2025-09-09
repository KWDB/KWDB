drop database if exists test_timebucket_gapfill cascade;
create ts database test_timebucket_gapfill;
create table test_timebucket_gapfill.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t4,t8,t12);
insert into test_timebucket_gapfill.tb values('0001-11-06 17:10:55.123','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');

select time_bucket_gapfill(k_timestamp,'400000hour') as a,interpolate(count(*),null) from test_timebucket_gapfill.tb group by a order by a;

select time_bucket_gapfill(k_timestamp,'400000hour') as a,interpolate(count(1),null) from test_timebucket_gapfill.tb group by a order by a;

drop database test_timebucket_gapfill cascade;