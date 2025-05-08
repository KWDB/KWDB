drop database if exists bug34896 cascade;
CREATE ts DATABASE bug34896;
use bug34896;

CREATE TABLE t_electmeter (
                              k_timestamp TIMESTAMPTZ NOT NULL,
                              elect_name VARCHAR(63) NOT NULL,
                              vol_a FLOAT8 NOT NULL,
                              cur_a FLOAT8 NOT NULL,
                              powerf_a FLOAT8 NULL,
                              allenergy_a INT4 NOT NULL,
                              pallenergy_a INT4 NOT NULL,
                              rallenergy_a INT4 NOT NULL,
                              allrenergy1_a INT4 NOT NULL,
                              allrenergy2_a INT4 NOT NULL,
                              powera_a FLOAT8 NOT NULL,
                              powerr_a FLOAT8 NOT NULL,
                              powerl_a FLOAT8 NOT NULL,
                              vol_b FLOAT8 NOT NULL,
                              cur_b FLOAT8 NOT NULL,
                              powerf_b FLOAT8 NOT NULL,
                              allenergy_b INT4 NOT NULL,
                              pallenergy_b INT4 NOT NULL,
                              rallenergy_b INT4 NOT NULL,
                              allrenergy1_b INT4 NOT NULL,
                              allrenergy2_b INT4 NOT NULL,
                              powera_b FLOAT8 NOT NULL,
                              powerr_b FLOAT8 NOT NULL,
                              powerl_b FLOAT8 NOT NULL,
                              vol_c FLOAT8 NOT NULL,
                              cur_c FLOAT8 NOT NULL,
                              powerf_c FLOAT8 NOT NULL,
                              allenergy_c INT4 NOT NULL,
                              pallenergy_c INT4 NOT NULL,
                              rallenergy_c INT4 NOT NULL,
                              allrenergy1_c INT4 NOT NULL,
                              allrenergy2_c INT4 NOT NULL,
                              powera_c FLOAT8 NOT NULL,
                              powerr_c FLOAT8 NOT NULL,
                              powerl_c FLOAT8 NOT NULL,
                              vol_ab FLOAT8 NULL,
                              vol_bc FLOAT8 NULL,
                              vol_ca FLOAT8 NULL,
                              infre FLOAT8 NOT NULL,
                              powerf FLOAT8 NOT NULL,
                              allpower FLOAT8 NOT NULL,
                              pallpower FLOAT8 NOT NULL,
                              rallpower FLOAT8 NOT NULL,
                              powerr FLOAT8 NOT NULL,
                              powerl FLOAT8 NOT NULL,
                              allrenergy1 FLOAT8 NOT NULL,
                              allrenergy2 FLOAT8 NOT NULL
) TAGS (
	machine_code VARCHAR(64) NOT NULL,
	op_group VARCHAR(64) NOT NULL,
	location VARCHAR(64) NOT NULL,
	cnc_number INT4 ) PRIMARY TAGS(machine_code);

insert into t_electmeter values ('2024-02-02 12:00:00','e',1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,'a','b','c',1);


CREATE TABLE t_cnc (
                       k_timestamp TIMESTAMPTZ NOT NULL,
                       cnc_sn VARCHAR(200) NULL,
                       cnc_sw_mver VARCHAR(30) NULL,
                       cnc_sw_sver VARCHAR(30) NULL,
                       cnc_tol_mem VARCHAR(10) NULL,
                       cnc_use_mem VARCHAR(10) NULL,
                       cnc_unuse_mem VARCHAR(10) NULL,
                       cnc_status VARCHAR(2) NULL,
                       path_quantity VARCHAR(30) NULL,
                       axis_quantity VARCHAR(30) NULL,
                       axis_path VARCHAR(100) NULL,
                       axis_type VARCHAR(100) NULL,
                       axis_unit VARCHAR(100) NULL,
                       axis_num VARCHAR(100) NULL,
                       axis_name VARCHAR(100) NULL,
                       sp_name VARCHAR(100) NULL,
                       abs_pos VARCHAR(200) NULL,
                       rel_pos VARCHAR(200) NULL,
                       mach_pos VARCHAR(200) NULL,
                       dist_pos VARCHAR(200) NULL,
                       sp_override FLOAT8 NULL,
                       sp_set_speed VARCHAR(30) NULL,
                       sp_act_speed VARCHAR(30) NULL,
                       sp_load VARCHAR(300) NULL,
                       feed_set_speed VARCHAR(30) NULL,
                       feed_act_speed VARCHAR(30) NULL,
                       feed_override VARCHAR(30) NULL,
                       servo_load VARCHAR(300) NULL,
                       parts_count VARCHAR(30) NULL,
                       cnc_cycletime VARCHAR(30) NULL,
                       cnc_alivetime VARCHAR(30) NULL,
                       cnc_cuttime VARCHAR(30) NULL,
                       cnc_runtime VARCHAR(30) NULL,
                       mprog_name VARCHAR(500) NULL,
                       mprog_num VARCHAR(30) NULL,
                       sprog_name VARCHAR(500) NULL,
                       sprog_num VARCHAR(30) NULL,
                       prog_seq_num VARCHAR(30) NULL,
                       prog_seq_content VARCHAR(1000) NULL,
                       alarm_count VARCHAR(10) NULL,
                       alarm_type VARCHAR(100) NULL,
                       alarm_code VARCHAR(100) NULL,
                       alarm_content VARCHAR(2000) NULL,
                       alarm_time VARCHAR(200) NULL,
                       cur_tool_num VARCHAR(20) NULL,
                       cur_tool_len_num VARCHAR(20) NULL,
                       cur_tool_len VARCHAR(20) NULL,
                       cur_tool_len_val VARCHAR(20) NULL,
                       cur_tool_x_len VARCHAR(20) NULL,
                       cur_tool_x_len_val VARCHAR(20) NULL,
                       cur_tool_y_len VARCHAR(20) NULL,
                       cur_tool_y_len_val VARCHAR(20) NULL,
                       cur_tool_z_len VARCHAR(20) NULL,
                       cur_tool_z_len_val VARCHAR(20) NULL,
                       cur_tool_rad_num VARCHAR(20) NULL,
                       cur_tool_rad VARCHAR(20) NULL,
                       cur_tool_rad_val VARCHAR(20) NULL,
                       device_state INT4 NULL,
                       value1 VARCHAR(10) NULL,
                       value2 VARCHAR(10) NULL,
                       value3 VARCHAR(10) NULL,
                       value4 VARCHAR(10) NULL,
                       value5 VARCHAR(10) NULL
) TAGS (
	machine_code VARCHAR(64) NOT NULL,
	op_group VARCHAR(64) NOT NULL,
	brand VARCHAR(64) NOT NULL,
	number_of_molds INT4 ) PRIMARY TAGS(machine_code, op_group);

insert into t_cnc (k_timestamp, machine_code, op_group, brand) values ('2024-02-02 13:00:00','a1','b2','c3');

insert into t_cnc values ('2024-02-02 14:00:00','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e', 1 , 'e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e','e', 1, 'e', 'e','e','e','e','e','e','e',1);


select
    subq_2.c0 as c0,
    subq_2.c0 as c1,
    subq_2.c0 as c2,
    subq_2.c0 as c3,
    cast(coalesce(subq_2.c0,

                  pg_catalog.last_row_ts(
                          cast(pg_catalog.current_time() as "timetz"),
                          cast((select k_timestamp from public.t_cnc limit 1 offset 2)
           as "timestamptz")) over (partition by subq_2.c0,subq_2.c0,subq_2.c0,subq_2.c0 order by subq_2.c0)) as "timestamptz") as c4,
    subq_2.c0 as c5,
    subq_2.c0 as c6,
    subq_2.c0 as c7,
    subq_2.c0 as c8,
    subq_2.c0 as c9
from
    (select
         ref_0.k_timestamp as c0
     from
         public.t_electmeter as ref_0,
         lateral (select
                      ref_1.feed_override as c0
                  from
                      public.t_cnc as ref_1
                  where cast(null as _inet) IS NOT DISTINCT FROM cast(null as _inet)
         limit 90) as subq_0
where case when EXISTS (
    select
        subq_0.c0 as c0,
        ref_2.vol_bc as c1,
        subq_1.c0 as c2
    from
        public.t_electmeter as ref_2,
        lateral (select
                     (select allrenergy1_a from public.t_electmeter limit 1 offset 6)
            as c0,
        ref_3.pallenergy_b as c1
    from
    public.t_electmeter as ref_3
    where (true <= false)
        or (cast(null as inet) < cast(null as inet))
    limit 94) as subq_1
              where cast(null as _jsonb) >= cast(null as _jsonb)) then ref_0.pallenergy_b else ref_0.pallenergy_b end
          is not NULL
    limit 68) as subq_2
where 58 is not NULL
    limit 151;


select
    cast(coalesce(subq_2.c0,
                  pg_catalog.last_row_ts(
                          cast(pg_catalog.current_time() as "timetz"),
                          cast((select k_timestamp from public.t_cnc limit 1 offset 2)
           as "timestamptz")) over (partition by subq_2.c0,subq_2.c0,subq_2.c0,subq_2.c0 order by subq_2.c0)) as "timestamptz") as c4
from
    (select t_electmeter.k_timestamp as c0 from t_electmeter )as subq_2;

select
    cast(coalesce(subq_2.c0,
                  pg_catalog.lastts(
                          cast(pg_catalog.current_time() as "timetz"),
                          cast((select k_timestamp from public.t_cnc limit 1 offset 2)
           as "timestamptz")) over (partition by subq_2.c0,subq_2.c0,subq_2.c0,subq_2.c0 order by subq_2.c0)) as "timestamptz") as c4
from
    (select t_electmeter.k_timestamp as c0 from t_electmeter )as subq_2;


select
    cast(coalesce(subq_2.c0,
                  pg_catalog.first_row_ts(
                          cast(pg_catalog.current_time() as "timetz"),
                          cast((select k_timestamp from public.t_cnc limit 1 offset 2)
           as "timestamptz")) over (partition by subq_2.c0,subq_2.c0,subq_2.c0,subq_2.c0 order by subq_2.c0)) as "timestamptz") as c4
from
    (select t_electmeter.k_timestamp as c0 from t_electmeter )as subq_2;

select
    cast(coalesce(subq_2.c0,
                  pg_catalog.firstts(
                          cast(pg_catalog.current_time() as "timetz"),
                          cast((select k_timestamp from public.t_cnc limit 1 offset 2)
           as "timestamptz")) over (partition by subq_2.c0,subq_2.c0,subq_2.c0,subq_2.c0 order by subq_2.c0)) as "timestamptz") as c4
from
    (select t_electmeter.k_timestamp as c0 from t_electmeter )as subq_2;

select
    coalesce(
            t_electmeter.k_timestamp,
            pg_catalog.last_row_ts(t_electmeter.k_timestamp, t_electmeter.k_timestamp) over (partition by t_electmeter.k_timestamp)
        )
from t_electmeter;

select
    coalesce(
            t_electmeter.k_timestamp,
            pg_catalog.last_row_ts(t_electmeter.k_timestamp) over (partition by t_electmeter.k_timestamp)
        )
from t_electmeter;

drop database bug34896 cascade;

drop database if exists spanBoundary cascade;
CREATE ts DATABASE spanBoundary;
use spanBoundary;
create table spanBoundary.t1 (ts timestamp not null, e1 int) tags (tag1 int not null) primary tags (tag1);
insert into spanBoundary.t1 values ('2021-01-01 12:12:12.123456',1,10);

select min(e1) from spanBoundary.t1 where tag1 = 10 and ts >= '2021-01-01 12:12:12.123+00:00';
select min(e1) from spanBoundary.t1 where tag1 = 10 and ts > '2021-01-01 12:12:12.122+00:00';
select min(e1) from spanBoundary.t1 where tag1 = 10 and ts > '2021-01-01 12:12:12.1221+00:00';
select min(e1) from spanBoundary.t1 where tag1 = 10 and ts > '2021-01-01 12:12:12.122999+00:00';
select min(e1) from spanBoundary.t1 where tag1 = 10 and ts > '2021-01-01 12:12:12.122999999+00:00';

select min(e1) from spanBoundary.t1 where tag1 = 10 and ts <= '2021-01-01 12:12:12.123+00:00';
select min(e1) from spanBoundary.t1 where tag1 = 10 and ts < '2021-01-01 12:12:12.123+00:00';
select min(e1) from spanBoundary.t1 where tag1 = 10 and ts < '2021-01-01 12:12:12.1231+00:00';
select min(e1) from spanBoundary.t1 where tag1 = 10 and ts < '2021-01-01 12:12:12.123001+00:00';
select min(e1) from spanBoundary.t1 where tag1 = 10 and ts < '2021-01-01 12:12:12.123000001+00:00';
drop database spanBoundary cascade;

--- fix bug 36173
create database rel_db;
use rel_db;
create table ref_0(c0 string, c1 int);
insert into ref_0 values('plc|plc|plc|plc', 50);
select pg_catalog.matching(299.4,1,2::numeric, 3::text,null::int8) over (PARTITION BY ref_0.c0 ORDER BY ref_0.c1, ref_0.c1) as bool from ref_0;

--- fix big 36235
select pg_catalog.interpolate(cast(1 as float8), cast(pg_catalog.pg_client_encoding() as text)) over (partition by 1);
drop database rel_db cascade;

--- fix big 37018
drop database if exists test_select_function cascade;
create ts database test_select_function;
create table test_select_function.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t4,t8,t12);


insert into test_select_function.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select_function.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,-32768,-2147483648,-9223372036854775808,false,-922.123,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','\0test查询  @TEST1\0','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select_function.tb values ('2022-05-01 12:10:25','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,400,4000,false,50.555,500.578578,'d','\\test测试！！！@TEST1','e','test测试！T  EST1xaa','查询查询 ','\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_select_function.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',32767,2147483647,9223372036854775807,true,922.123,500.578578,'','     ',' ','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_select_function.tb values ('2022-05-01 12:10:31.22','2020-05-01 22:30:11',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\', 'v2551cdf~#   ', '  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,300,300,false,60.666,600.678,'','\\test测试！！！@TEST1',' ','test测试！T  EST1xaa','查询查询 ','\\','','    ','','  ');
insert into test_select_function.tb values ('2023-05-10 09:08:19.22','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e','a',null,'测试测试 ','b','test测试10_1','vwwws中文_1',null);
insert into test_select_function.tb values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'测试测试 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_select_function.tb values ('2023-05-10 09:08:18.223','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,6,100,1000,false,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','5555 5','  bdbd','y','test@测试！10_1','vwwws_1','cddde');


select max(e10),max(e11),max(t2),max(t8),max(t12) from test_select_function.tb where t8 like '% ' or t10 like '% ' or t11 is null group by e10,t8,t10,t11 order by e10,t8;
select min(e10),min(e11),min(t2),min(t8),min(t12) from test_select_function.tb where t8 like '% ' or t10 like '% ' or t11 is null group by e10,t8,t10,t11 order by e10,t8;
select max(t11),last_row(t12) from test_select_function.tb where t10 like ' ' or t11 is null or t12 like '%\\' group by t10,t11,t12 order by t10,t11 asc,t12 desc;
select sum(e3) as sumVal1,sum(e4) as sumVal2 from test_select_function.tb group by e2,e3 order by sumVal1,sumVal2 limit 2 offset 1;

drop database test_select_function cascade;
