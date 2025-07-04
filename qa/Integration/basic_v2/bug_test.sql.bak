create ts database test;
USE test;
CREATE TABLE t_cnc (k_timestamp TIMESTAMPTZ NOT NULL,cnc_sn VARCHAR(200) NULL,cnc_sw_mver VARCHAR(30) NULL,cnc_sw_sver VARCHAR(30) NULL,cnc_tol_mem VARCHAR(10) NULL,cnc_use_mem VARCHAR(10) NULL,cnc_unuse_mem VARCHAR(10) NULL,cnc_status VARCHAR(2) NULL,path_quantity VARCHAR(30) NULL,axis_quantity VARCHAR(30) NULL,axis_path VARCHAR(100) NULL,axis_type VARCHAR(100) NULL,axis_unit VARCHAR(100) NULL,axis_num VARCHAR(100) NULL,axis_name VARCHAR(100) NULL,sp_name VARCHAR(100) NULL,abs_pos VARCHAR(200) NULL,rel_pos VARCHAR(200) NULL,mach_pos VARCHAR(200) NULL,dist_pos VARCHAR(200) NULL,sp_override FLOAT8 NULL,sp_set_speed VARCHAR(30) NULL,sp_act_speed VARCHAR(30) NULL,sp_load VARCHAR(300) NULL,feed_set_speed VARCHAR(30) NULL,feed_act_speed VARCHAR(30) NULL,feed_override VARCHAR(30) NULL,servo_load VARCHAR(300) NULL,parts_count VARCHAR(30) NULL,cnc_cycletime VARCHAR(30) NULL,cnc_alivetime VARCHAR(30) NULL,cnc_cuttime VARCHAR(30) NULL,cnc_runtime VARCHAR(30) NULL,mprog_name VARCHAR(500) NULL,mprog_num VARCHAR(30) NULL,sprog_name VARCHAR(500) NULL,sprog_num VARCHAR(30) NULL,prog_seq_num VARCHAR(30) NULL,prog_seq_content VARCHAR(1000) NULL,alarm_count VARCHAR(10) NULL,alarm_type VARCHAR(100) NULL,alarm_code VARCHAR(100) NULL,alarm_content VARCHAR(2000) NULL,alarm_time VARCHAR(200) NULL,cur_tool_num VARCHAR(20) NULL,cur_tool_len_num VARCHAR(20) NULL,cur_tool_len VARCHAR(20) NULL,cur_tool_len_val VARCHAR(20) NULL,cur_tool_x_len VARCHAR(20) NULL,cur_tool_x_len_val VARCHAR(20) NULL,cur_tool_y_len VARCHAR(20) NULL,cur_tool_y_len_val VARCHAR(20) NULL,cur_tool_z_len VARCHAR(20) NULL,cur_tool_z_len_val VARCHAR(20) NULL,cur_tool_rad_num VARCHAR(20) NULL,cur_tool_rad VARCHAR(20) NULL,cur_tool_rad_val VARCHAR(20) NULL,device_state INT4 NULL,value1 VARCHAR(10) NULL,value2 VARCHAR(10) NULL,value3 VARCHAR(10) NULL,value4 VARCHAR(10) NULL,value5 VARCHAR(10) NULL) TAGS (machine_code VARCHAR(64) NOT NULL,op_group VARCHAR(64) NOT NULL,brand VARCHAR(64) NOT NULL,number_of_molds INT4 ) PRIMARY TAGS(machine_code, op_group);

CREATE TABLE t_electmeter (k_timestamp TIMESTAMPTZ NOT NULL,elect_name VARCHAR(63) NOT NULL,vol_a FLOAT8 NOT NULL,cur_a FLOAT8 NOT NULL,powerf_a FLOAT8 NULL,allenergy_a INT4 NOT NULL,pallenergy_a INT4 NOT NULL,rallenergy_a INT4 NOT NULL,allrenergy1_a INT4 NOT NULL,allrenergy2_a INT4 NOT NULL,powera_a FLOAT8 NOT NULL,powerr_a FLOAT8 NOT NULL,powerl_a FLOAT8 NOT NULL,vol_b FLOAT8 NOT NULL,cur_b FLOAT8 NOT NULL,powerf_b FLOAT8 NOT NULL,allenergy_b INT4 NOT NULL,pallenergy_b INT4 NOT NULL,rallenergy_b INT4 NOT NULL,allrenergy1_b INT4 NOT NULL,allrenergy2_b INT4 NOT NULL,powera_b FLOAT8 NOT NULL,powerr_b FLOAT8 NOT NULL,powerl_b FLOAT8 NOT NULL,vol_c FLOAT8 NOT NULL,cur_c FLOAT8 NOT NULL,powerf_c FLOAT8 NOT NULL,allenergy_c INT4 NOT NULL,pallenergy_c INT4 NOT NULL,rallenergy_c INT4 NOT NULL,allrenergy1_c INT4 NOT NULL,allrenergy2_c INT4 NOT NULL,powera_c FLOAT8 NOT NULL,powerr_c FLOAT8 NOT NULL,powerl_c FLOAT8 NOT NULL,vol_ab FLOAT8 NULL,vol_bc FLOAT8 NULL,vol_ca FLOAT8 NULL,infre FLOAT8 NOT NULL,powerf FLOAT8 NOT NULL,allpower FLOAT8 NOT NULL,pallpower FLOAT8 NOT NULL,rallpower FLOAT8 NOT NULL,powerr FLOAT8 NOT NULL,powerl FLOAT8 NOT NULL,allrenergy1 FLOAT8 NOT NULL,allrenergy2 FLOAT8 NOT NULL) TAGS (machine_code VARCHAR(64) NOT NULL,op_group VARCHAR(64) NOT NULL,location VARCHAR(64) NOT NULL,cnc_number INT4 ) PRIMARY TAGS(machine_code);


-- test_case0001 ZDP-33543
select  
  case when cast(coalesce(case when EXISTS (
            select  
                ref_3.allrenergy2_c as c0
              from 
                public.t_electmeter as ref_3
              where cast(null as _interval) IS DISTINCT FROM cast(null as _interval)) then case when ref_2.cur_tool_rad is not NULL then cast(null as _bool) else cast(null as _bool) end
             else case when ref_2.cur_tool_rad is not NULL then cast(null as _bool) else cast(null as _bool) end
             end
          ,
        case when (EXISTS (
              select  
                  ref_2.value5 as c0, 
                  ref_4.powerl_b as c1, 
                  ref_2.cur_tool_y_len as c2, 
                  ref_4.op_group as c3
                from 
                  public.t_electmeter as ref_4
                where subq_0.c10 is NULL)) 
            or (cast(null as date) = ref_2.k_timestamp) then pg_catalog.array_append(
            cast(cast(null as _bool) as _bool),
            cast(false as bool)) else pg_catalog.array_append(
            cast(cast(null as _bool) as _bool),
            cast(false as bool)) end
          ) as _bool) IS NOT DISTINCT FROM pg_catalog.array_replace(
        cast(cast(null as _bool) as _bool),
        cast(case when cast(null as bytea) < cast(null as bytea) then false else false end
           as bool),
        cast(case when ref_2.value3 is not NULL then false else false end
           as bool)) then ref_2.alarm_code else ref_2.alarm_code end
     as c0, 
  subq_0.c4 as c1, 
  subq_0.c0 as c2, 
  case when (cast(coalesce(case when cast(null as "timetz") >= cast(null as "timetz") then cast(null as _numeric) else cast(null as _numeric) end
            ,
          cast(coalesce(cast(null as _numeric),
            cast(null as _numeric)) as _numeric)) as _numeric) IS DISTINCT FROM cast(null as _numeric)) 
      and ((case when cast(null as _oid) < cast(null as _oid) then cast(null as _time) else cast(null as _time) end
             != cast(null as _time)) 
        and (subq_0.c8 > cast(null as date))) then subq_0.c1 else subq_0.c1 end
     as c3, 
  ref_2.rel_pos as c4, 
  case when ref_2.k_timestamp != case when ref_2.sp_name is NULL then pg_catalog.localtimestamp() else pg_catalog.localtimestamp() end
         then ref_2.brand else ref_2.brand end
     as c5, 
  ref_2.parts_count as c6, 
  85 as c7, 
  ref_2.cnc_sw_mver as c8, 
  case when pg_catalog.json_object(
        cast(cast(coalesce(pg_catalog.kwdb_internal.completed_migrations(),
          cast(null as _text)) as _text) as _text)) ?| pg_catalog.kwdb_internal.completed_migrations() then ref_2.cnc_runtime else ref_2.cnc_runtime end
     as c9, 
  ref_2.axis_name as c10, 
  3 as c11, 
  subq_0.c10 as c12, 
  subq_0.c12 as c13, 
  ref_2.cnc_unuse_mem as c14, 
  subq_0.c3 as c15, 
  subq_0.c0 as c16, 
  ref_2.feed_set_speed as c17, 
  subq_0.c13 as c18, 
  subq_0.c1 as c19, 
  subq_0.c11 as c20, 
  ref_2.cnc_alivetime as c21, 
  subq_0.c10 as c22, 
  subq_0.c13 as c23, 
  ref_2.mach_pos as c24, 
  subq_0.c1 as c25, 
  17 as c26
from 
  (select  
          79 as c0, 
          ref_0.allrenergy1_c as c1, 
          ref_1.value4 as c2, 
          ref_1.servo_load as c3, 
          ref_0.allrenergy2_a as c4, 
          ref_1.servo_load as c5, 
          ref_0.allenergy_b as c6, 
          ref_1.cnc_sn as c7, 
          ref_0.k_timestamp as c8, 
          ref_1.feed_act_speed as c9, 
          ref_1.sprog_name as c10, 
          ref_1.alarm_content as c11, 
          76 as c12, 
          ref_1.axis_name as c13
        from 
          public.t_electmeter as ref_0
            inner join public.t_cnc as ref_1
            on (cast(null as int8) IS NOT DISTINCT FROM ref_0.cur_a)
        where ref_0.pallenergy_c is NULL) as subq_0
    left join public.t_cnc as ref_2
    on (ref_2.sp_override < ref_2.sp_override)
where EXISTS (
  select  
      subq_1.c10 as c0, 
      subq_0.c5 as c1, 
      subq_1.c11 as c2, 
      subq_0.c11 as c3, 
      subq_0.c0 as c4, 
      subq_1.c0 as c5, 
      subq_0.c2 as c6, 
      subq_1.c5 as c7, 
      subq_0.c10 as c8, 
      subq_0.c1 as c9, 
      ref_2.cnc_status as c10, 
      subq_0.c5 as c11
    from 
      (select  
            ref_2.cnc_unuse_mem as c0, 
            subq_0.c8 as c1, 
            ref_5.rallpower as c2, 
            subq_0.c9 as c3, 
            ref_5.allrenergy2_c as c4, 
            ref_2.feed_override as c5, 
            ref_2.value3 as c6, 
            ref_2.cnc_sw_mver as c7, 
            ref_5.machine_code as c8, 
            29 as c9, 
            ref_5.powerl as c10, 
            subq_0.c8 as c11, 
            ref_5.allpower as c12, 
            ref_5.vol_c as c13, 
            ref_2.cnc_status as c14, 
            subq_0.c13 as c15, 
            subq_0.c5 as c16
          from 
            public.t_electmeter as ref_5
          where false
          limit 70) as subq_1
    where cast(null as bytea) > pg_catalog.oidout(
        cast(pg_catalog.pg_my_temp_schema() as oid)))
limit 99;


-- test_case0002 ZDP-33496
select  
  subq_0.c2 as c0, 
  subq_0.c0 as c1, 
  subq_0.c7 as c2, 
  subq_0.c4 as c3, 
  subq_0.c1 as c4, 
  subq_0.c5 as c5, 
  case when (subq_0.c0 is not NULL) 
      and (cast(coalesce(subq_0.c7,
          subq_0.c7) as "timestamptz") is not NULL) then subq_0.c8 else subq_0.c8 end
     as c6, 
  cast(nullif(subq_0.c8,
    subq_0.c8) as float8) as c7, 
  subq_0.c1 as c8, 
  80 as c9, 
  subq_0.c0 as c10
from 
  (select distinct 
        91 as c0, 
        ref_4.allrenergy2_c as c1, 
        ref_3.pallenergy_c as c2, 
        ref_1.allrenergy1_a as c3, 
        ref_0.brand as c4, 
        ref_2.cnc_alivetime as c5, 
        ref_7.cur_c as c6, 
        ref_1.k_timestamp as c7, 
        case when 38 is not NULL then ref_7.powerf_a else ref_7.powerf_a end
           as c8
      from 
        public.t_cnc as ref_0
            inner join public.t_electmeter as ref_1
                  left join public.t_cnc as ref_2
                  on (cast(null as _time) IS NOT DISTINCT FROM cast(null as _time))
                inner join public.t_electmeter as ref_3
                on ((ref_3.allpower is not NULL) 
                    and (cast(null as _jsonb) IS NOT DISTINCT FROM cast(null as _jsonb)))
              left join public.t_electmeter as ref_4
                inner join public.t_electmeter as ref_5
                on (ref_4.k_timestamp < ref_4.k_timestamp)
              on (EXISTS (
                  select  
                      ref_3.powerr_c as c0, 
                      ref_2.cur_tool_len_val as c1, 
                      ref_3.allpower as c2, 
                      ref_4.cnc_number as c3, 
                      ref_4.rallpower as c4, 
                      ref_6.powerr_b as c5
                    from 
                      public.t_electmeter as ref_6
                    where false
                    limit 114))
            on (ref_0.cnc_unuse_mem = ref_5.elect_name )
          inner join public.t_electmeter as ref_7
          on (79 is not NULL)
      where ref_1.powerf_b IS DISTINCT FROM ref_4.powerr_b
      limit 26) as subq_0
where pg_catalog.oid(
    cast(pg_catalog.inet_client_port() as int8)) > pg_catalog.pg_my_temp_schema();



-- test_case0003 ZDP-33385
select  
  ref_1.dist_pos as c0, 
  subq_1.c1 as c1, 
  subq_1.c1 as c2, 
  ref_1.number_of_molds as c3, 
  ref_3.abs_pos as c4, 
  ref_3.value5 as c5
from 
  public.t_cnc as ref_0
        inner join public.t_cnc as ref_1
          inner join (select  
                ref_2.alarm_code as c0
              from 
                public.t_cnc as ref_2
              where ref_2.k_timestamp <= ref_2.k_timestamp
              limit 168) as subq_0
          on (47 is NULL)
        on ((ref_0.k_timestamp <= ref_0.k_timestamp) 
            or (cast(null as "interval") < cast(null as "interval")))
      inner join public.t_cnc as ref_3
      on (subq_0.c0 = ref_3.cnc_sn )
    left join (select  
          ref_5.rallenergy_c as c0, 
          ref_4.pallpower as c1, 
          cast(coalesce(ref_7.cnc_number,
            ref_7.allrenergy2_b) as int4) as c2, 
          ref_5.rallenergy_a as c3
        from 
          public.t_electmeter as ref_4
            inner join public.t_electmeter as ref_5
                inner join public.t_cnc as ref_6
                on (ref_5.allrenergy1_b is NULL)
              right join public.t_electmeter as ref_7
              on (ref_6.cnc_sn = ref_7.elect_name )
            on ((EXISTS (
                  select  
                      ref_5.allrenergy2_a as c0, 
                      ref_7.k_timestamp as c1, 
                      ref_7.powerl as c2, 
                      ref_4.powerl_c as c3, 
                      ref_6.alarm_type as c4, 
                      ref_7.rallenergy_a as c5, 
                      ref_4.powera_a as c6, 
                      ref_4.infre as c7
                    from 
                      public.t_cnc as ref_8
                    where (cast(null as _numeric) >= cast(null as _numeric)) 
                      and ((false) 
                        or (cast(null as _text) IS NOT DISTINCT FROM cast(null as _text)))
                    limit 81)) 
                or (cast(null as _uuid) <= cast(null as _uuid)))
        where ref_5.k_timestamp != pg_catalog.now()
        limit 106) as subq_1
    on (ref_3.sp_override < case when ref_1.sp_override >= cast(null as "numeric") then cast(null as int8) else cast(null as int8) end
          )
where (cast(null as "numeric") < cast(coalesce(pg_catalog.inet_client_port(),
      case when EXISTS (
          select  
              ref_9.powerf_a as c0, 
              ref_0.cur_tool_z_len as c1, 
              ref_9.powerl_a as c2, 
              subq_0.c0 as c3, 
              ref_3.feed_set_speed as c4
            from 
              public.t_electmeter as ref_9
            where true
            limit 28) then case when cast(null as _float8) IS DISTINCT FROM cast(null as _float8) then pg_catalog.inet_client_port() else pg_catalog.inet_client_port() end
           else case when cast(null as _float8) IS DISTINCT FROM cast(null as _float8) then pg_catalog.inet_client_port() else pg_catalog.inet_client_port() end
           end
        ) as int8)) 
  and (cast(null as "interval") < pg_catalog.interval_in(
      cast(ref_1.prog_seq_content as "varchar")))
limit 71;

-- test_case0004 ZDP-33591
create ts database test_select_last;
create table test_select_last.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t4,t8,t12);

insert into test_select_last.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select_last.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,-32768,-2147483648,-9223372036854775808,false,-922.123,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','\0test查询  @TEST1\0','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select_last.tb values ('2022-05-01 12:10:25','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()&^%{}','\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,400,4000,false,50.555,500.578578,'d','\test测试！！！@TEST1','e','test测试！T  EST1xaa','查询查询 ',e'\\','e','es1023_2','s_ 4','ww4096_2');

insert into test_select_last.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',32767,2147483647,9223372036854775807,true,922.123,500.578578,'','     ',' ','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_select_last.tb values ('2022-05-01 12:10:31.22','2020-05-01 22:30:11',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()&^%{}','\', 'v2551cdf~#   ', '  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,300,300,false,60.666,600.678,'','\test测试！！！@TEST1',' ','test测试！T  EST1xaa','查询查询 ',e'\\','','    ','','  ');
insert into test_select_last.tb values ('2023-05-10 09:08:19.22','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e',e'\a',null,'测试测试 ','b','test测试10_1','vwwws中文_1',null);

insert into test_select_last.tb values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'测试测试 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_select_last.tb values ('2023-05-10 09:08:18.223','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,6,100,1000,false,-10.123,100.111111,'b','\TEST1 ','f','测试！TEST1xaa','5555 5','  bdbd','y','test@测试！10_1','vwwws_1','cddde');

select k_timestamp,t5,t6 from test_select_last.tb where t5 in(-10.123,70.7077) order by k_timestamp;

--ZDP-36575
select * from (select last(e5) as lastVal,last(t6) from test_select_last.tb) where lastVal in (6000000.606,7000000.707);

drop database test_select_last cascade;


-- test_case0005 ZDP-33849
select    subq_4.c1 as c0,   subq_4.c5 as c1,   cast(coalesce(subq_4.c5,    subq_4.c0) as "varchar") as c2from   (select          subq_3.c1 as c0,         28 as c1,         65 as c2,         subq_3.c2 as c3,         subq_3.c0 as c4,         subq_3.c0 as c5      from         (select                ref_0.dist_pos as c0,               ref_0.value2 as c1,               ref_0.axis_name as c2            from               public.t_cnc as ref_0,              lateral (select                      ref_1.alarm_content as c0,                     ref_1.value1 as c1                  from                     public.t_cnc as ref_1                  where true                  limit 159) as subq_0            where (cast(null as "timetz") > cast(null as "timetz"))               or (EXISTS (                select                      subq_2.c0 as c0,                     subq_2.c0 as c1,                     subq_0.c1 as c2                  from                     public.t_electmeter as ref_2,                    lateral (select                            ref_2.rallenergy_b as c0,                           subq_1.c1 as c1,                           ref_3.cur_tool_len_val as c2                        from                           public.t_cnc as ref_3,                          lateral (select                                  71 as c0,                                 ref_2.allenergy_a as c1,                                 ref_2.elect_name as c2                              from                                 public.t_cnc as ref_4                              where true                              limit 95) as subq_1                        where ((select pg_catalog.array_agg(sp_override) from public.t_cnc)                               IS DISTINCT FROM cast(null as _float8))                           and (cast(null as text) > cast(null as text))                        limit 113) as subq_2                  where false))            limit 108) as subq_3      where cast(coalesce((select vol_a from public.t_electmeter limit 1 offset 4)            ,          case when true then (select vol_a from public.t_electmeter limit 1 offset 48)               else (select vol_a from public.t_electmeter limit 1 offset 48)               end            ) as float8) >= (select pg_catalog.variance(vol_a) from public.t_electmeter)                limit 178) as subq_4where (subq_4.c3 is not NULL)   and (case when (cast(null as "interval") != case when subq_4.c4 is NULL then cast(null as "interval") else cast(null as "interval") end            )         and ((subq_4.c1 is not NULL)           and (cast(null as "timestamp") IS NOT DISTINCT FROM cast(null as "timestamp"))) then pg_catalog.transaction_timestamp() else pg_catalog.transaction_timestamp() end       != cast(null as "timestamp"))limit 65;

-- test_case0006 ZDP-33484'
WITH jennifer_0 AS (select      ref_4.vol_a as c0,     ref_1.cnc_cycletime as c1,     ref_4.op_group as c2  from     (select                  ref_0.cnc_tol_mem as c0,                 ref_0.sp_act_speed as c1,                 ref_0.alarm_count as c2,                 ref_0.cur_tool_rad_num as c3,                 ref_0.cur_tool_y_len_val as c4,                 ref_0.axis_quantity as c5,                 ref_0.prog_seq_content as c6,                 ref_0.cur_tool_num as c7              from                 public.t_cnc as ref_0              where cast(null as bytea) IS NOT DISTINCT FROM cast(null as bytea)              limit 121) as subq_0          right join public.t_cnc as ref_1          on (cast(null as oid) < cast(null as oid))        left join public.t_electmeter as ref_2        on (subq_0.c0 is not NULL)      inner join (select                ref_3.prog_seq_num as c0            from               public.t_cnc as ref_3            where cast(null as oid) IS NOT DISTINCT FROM cast(null as oid)            limit 134) as subq_1        right join public.t_electmeter as ref_4        on ((EXISTS (              select                    ref_5.cur_tool_z_len_val as c0,                   ref_5.number_of_molds as c1                from                   public.t_cnc as ref_5                where false))             and (ref_4.k_timestamp IS NOT DISTINCT FROM cast(null as date)))      on (ref_2.powerl_b = ref_4.vol_a )  where cast(null as "numeric") IS NOT DISTINCT FROM pg_catalog.cluster_logical_timestamp()  limit 88), jennifer_1 AS (select      case when cast(nullif(cast(null as int8),                      pg_catalog.sum_int(              cast(cast(null as int8) as int8)) over (partition by subq_3.c0,subq_2.c0 order by subq_2.c2,subq_2.c0)) as int8) > cast(nullif(cast(null as "numeric"),          cast(coalesce(cast(null as "numeric"),            cast(null as "numeric")) as "numeric")) as "numeric") then subq_3.c0 else subq_3.c0 end       as c0,     subq_3.c0 as c1,     subq_4.c0 as c2,     subq_4.c1 as c3,     case when case when true then case when cast(null as _timetz) < cast(null as _timetz) then cast(null as "timestamptz") else cast(null as "timestamptz") end             else case when cast(null as _timetz) < cast(null as _timetz) then cast(null as "timestamptz") else cast(null as "timestamptz") end             end           = pg_catalog.time_bucket(          cast(cast(null as "timestamp") as "timestamp"),          cast(case when false then cast(null as text) else cast(null as text) end             as text)) then subq_3.c0 else subq_3.c0 end       as c4,     subq_2.c2 as c5,     subq_3.c0 as c6,     subq_2.c2 as c7,     subq_4.c0 as c8,     subq_2.c1 as c9,     5 as c10,     subq_3.c0 as c11,     case when EXISTS (        select              ref_21.location as c0,             ref_19.allrenergy1_a as c1,             subq_2.c2 as c2,             ref_21.cur_b as c3,             22 as c4,             ref_20.cur_tool_y_len_val as c5,             ref_21.allenergy_b as c6,             ref_21.vol_b as c7,             subq_2.c1 as c8,             ref_20.op_group as c9,             ref_22.powerl_a as c10,             subq_2.c1 as c11          from             public.t_electmeter as ref_19                inner join public.t_cnc as ref_20                on ((cast(null as int8) < cast(null as "numeric"))                     or ((cast(null as _inet) > cast(null as _inet))                       and (cast(null as _time) != cast(null as _time))))              left join public.t_electmeter as ref_21                left join public.t_electmeter as ref_22                on (ref_21.powerr_b = ref_22.vol_a )              on (ref_20.value1 is not NULL)          where cast(null as "time") > cast(null as "timetz")) then subq_3.c0 else subq_3.c0 end       as c12,     subq_2.c0 as c13,     subq_4.c1 as c14  from     (select                68 as c0,               ref_7.allpower as c1,               ref_6.powerl_a as c2            from               public.t_electmeter as ref_6                right join public.t_electmeter as ref_7                on (cast(null as anyarray) @> cast(null as anyarray))            where (EXISTS (                select                      96 as c0,                     ref_6.powerl_a as c1,                     ref_8.pallenergy_a as c2                  from                     public.t_electmeter as ref_8                  where cast(null as _interval) IS DISTINCT FROM cast(null as _interval)                  limit 47))               and ((((((cast(null as uuid) > cast(null as uuid))                         or (cast(null as _bool) != cast(null as _bool)))                       or (true))                     and ((true)                       and ((cast(null as "interval") IS DISTINCT FROM cast(null as "interval"))                         or (ref_7.allrenergy1_a is NULL))))                   or ((ref_7.cur_a is not NULL)                     or ((ref_7.k_timestamp > cast(null as date))                       and (cast(null as bytea) >= cast(null as bytea)))))                 and (cast(null as _timestamp) <= cast(null as _timestamp)))            limit 91) as subq_2        inner join (select                ref_11.cnc_cuttime as c0            from               public.t_cnc as ref_9                  left join public.t_electmeter as ref_10                  on (ref_9.cnc_status is not NULL)                left join public.t_cnc as ref_11                on (ref_10.op_group = ref_11.cnc_sn )            where true            limit 76) as subq_3        on (97 is NULL)      left join (select              ref_14.alarm_time as c0,             ref_17.value1 as c1          from             public.t_cnc as ref_12                inner join public.t_cnc as ref_13                  left join public.t_cnc as ref_14                  on ((EXISTS (                        select                              ref_14.sp_override as c0,                             ref_13.cnc_tol_mem as c1,                             ref_14.mprog_num as c2,                             ref_13.prog_seq_num as c3,                             ref_13.alarm_type as c4,                             ref_15.sp_load as c5,                             11 as c6,                             ref_14.mach_pos as c7                          from                             public.t_cnc as ref_15                          where ((true)                               or (cast(null as bytea) IS NOT DISTINCT FROM cast(null as bytea)))                             and (ref_13.axis_quantity is not NULL)))                       or (false))                on (ref_12.value4 = ref_14.cnc_sn )              inner join public.t_cnc as ref_16                right join public.t_cnc as ref_17                  left join public.t_cnc as ref_18                  on (true)                on (cast(null as _uuid) <= cast(null as _uuid))              on ((cast(null as "timestamp") >= ref_16.k_timestamp)                   or ((ref_14.k_timestamp != cast(null as "timestamp"))                     and (((cast(null as _timestamptz) IS NOT DISTINCT FROM cast(null as _timestamptz))                         and (cast(null as "timetz") != cast(null as "timetz")))                       and (true))))          where cast(null as date) IS NOT DISTINCT FROM ref_17.k_timestamp          limit 131) as subq_4      on (subq_3.c0 = subq_4.c0 )  where cast(null as "interval") < case when cast(null as "numeric") IS DISTINCT FROM pg_catalog.pi() then cast(null as "interval") else cast(null as "interval") end        limit 124), jennifer_2 AS (select      subq_5.c3 as c0,     ref_26.powerl_a as c1,     ref_28.powerf as c2,     subq_5.c6 as c3,     subq_5.c0 as c4,     ref_25.rel_pos as c5,     subq_5.c14 as c6,     ref_28.allenergy_a as c7,     ref_25.dist_pos as c8  from     (select                ref_23.vol_ab as c0,               ref_23.rallenergy_c as c1,               ref_23.rallenergy_c as c2,               ref_23.powerr_b as c3,               ref_24.cur_a as c4,               ref_23.powera_c as c5,               ref_24.powerf_c as c6,               ref_24.rallenergy_a as c7,               ref_23.allrenergy2_a as c8,               ref_23.op_group as c9,               ref_23.rallenergy_c as c10,               ref_24.allrenergy2 as c11,               ref_24.op_group as c12,               ref_24.vol_bc as c13,               ref_23.powera_b as c14,               ref_24.machine_code as c15            from               public.t_electmeter as ref_23                right join public.t_electmeter as ref_24                on (cast(null as int8) IS DISTINCT FROM cast(null as "numeric"))            where cast(null as int8) < ref_23.powerl_a            limit 11) as subq_5        inner join public.t_cnc as ref_25          inner join public.t_electmeter as ref_26          on ((cast(null as _timestamp) IS NOT DISTINCT FROM cast(null as _timestamp))               and (cast(null as bytea) >= cast(null as bytea)))        on (EXISTS (            select                  ref_27.vol_ab as c0,                 ref_27.vol_ab as c1              from                 public.t_electmeter as ref_27              where subq_5.c3 != ref_26.powerr_c              limit 113))      inner join public.t_electmeter as ref_28      on (case when cast(null as _oid) != cast(null as _oid) then cast(null as "interval") else cast(null as "interval") end             <= case when ref_25.axis_num is not NULL then cast(null as "interval") else cast(null as "interval") end            )  where pg_catalog.lastval() = cast(null as "numeric")  limit 102), jennifer_3 AS (select      ref_29.powerf as c0,     ref_29.cur_b as c1,     ref_29.allrenergy2 as c2  from     public.t_electmeter as ref_29  where case when (case when cast(null as "time") IS DISTINCT FROM cast(null as "timetz") then cast(null as "interval") else cast(null as "interval") end             = case when (cast(null as _date) <= cast(null as _date))               and (ref_29.allrenergy1 is NULL) then cast(null as "interval") else cast(null as "interval") end            )         and (true) then pg_catalog.localtimestamp() else pg_catalog.localtimestamp() end       >= ref_29.k_timestamp), jennifer_4 AS (select      subq_6.c0 as c0,     case when cast(null as _numeric) IS DISTINCT FROM cast(null as _numeric) then subq_6.c0 else subq_6.c0 end       as c1,     subq_6.c0 as c2,     subq_6.c0 as c3,     subq_6.c0 as c4,     subq_6.c0 as c5,     subq_6.c0 as c6,     subq_6.c0 as c7,     subq_6.c0 as c8  from     (select            ref_32.feed_set_speed as c0        from           public.t_electmeter as ref_30              left join public.t_cnc as ref_31                inner join public.t_cnc as ref_32                on (cast(null as "varbit") > cast(null as "varbit"))              on (ref_32.alarm_code is NULL)            right join public.t_electmeter as ref_33                inner join public.t_cnc as ref_34                on (false)              inner join public.t_electmeter as ref_35              on ((ref_33.allrenergy1 >= cast(null as int8))                   and (cast(null as _timetz) >= cast(null as _timetz)))            on (EXISTS (                select                      ref_30.powerf as c0,                     ref_35.powerl_b as c1,                     ref_32.path_quantity as c2,                     ref_33.allpower as c3                  from                     public.t_cnc as ref_36                  where (cast(null as _bytea) IS DISTINCT FROM cast(null as _bytea))                     or (false)                  limit 70))        where ref_34.cur_tool_rad_val is not NULL) as subq_6  where pg_catalog.timezone(      cast(pg_catalog.current_schema() as text),      cast(pg_catalog.current_timestamp() as "timestamptz")) > pg_catalog.current_timestamp()  limit 52), jennifer_5 AS (select      ref_37.cnc_cuttime as c0,     ref_37.k_timestamp as c1,     cast(coalesce(ref_37.value3,      case when pg_catalog.transaction_timestamp() = cast(null as "timestamp") then ref_37.cur_tool_z_len else ref_37.cur_tool_z_len end        ) as "varchar") as c2,     ref_37.parts_count as c3  from     public.t_cnc as ref_37  where cast(null as _inet) IS DISTINCT FROM pg_catalog.array_replace(      cast(pg_catalog.array_cat(        cast(pg_catalog.array_replace(          cast(cast(null as _inet) as _inet),          cast(cast(null as inet) as inet),          cast(case when cast(null as "timetz") IS DISTINCT FROM cast(null as "timetz") then cast(null as inet) else cast(null as inet) end             as inet)) as _inet),        cast(case when EXISTS (            select                  ref_37.alarm_code as c0,                 ref_38.cnc_alivetime as c1,                 ref_39.cur_tool_rad_num as c2,                 ref_39.sp_load as c3,                 ref_38.mprog_name as c4              from                 public.t_cnc as ref_38                  inner join public.t_cnc as ref_39                  on (cast(null as _int8) IS DISTINCT FROM cast(null as _int8))              where true              limit 109) then pg_catalog.array_remove(            cast(cast(null as _inet) as _inet),            cast(cast(null as inet) as inet)) else pg_catalog.array_remove(            cast(cast(null as _inet) as _inet),            cast(cast(null as inet) as inet)) end           as _inet)) as _inet),      cast(pg_catalog.inet_client_addr() as inet),      cast(case when ref_37.cnc_alivetime is NULL then case when (false)             and (false) then cast(null as inet) else cast(null as inet) end           else case when (false)             and (false) then cast(null as inet) else cast(null as inet) end           end         as inet))  limit 130), jennifer_6 AS (select      subq_7.c10 as c0  from     public.t_cnc as ref_41      right join public.t_electmeter as ref_42        inner join (select                ref_43.powerf as c0,               ref_43.cur_c as c1,               ref_43.powera_b as c2,               ref_43.allrenergy2_a as c3,               ref_43.powerr_c as c4,               ref_43.cur_a as c5,               ref_43.cur_a as c6,               ref_43.powerl_a as c7,               ref_43.powerl as c8,               ref_43.vol_b as c9,               ref_43.powerl_c as c10            from               public.t_electmeter as ref_43            where ref_43.k_timestamp >= cast(null as "timestamp")            limit 115) as subq_7        on (ref_42.powerf_c = subq_7.c0 )      on (case when cast(null as "timetz") IS NOT DISTINCT FROM cast(null as "timetz") then cast(null as "numeric") else cast(null as "numeric") end             >= case when cast(null as "interval") <= cast(null as "interval") then cast(null as int8) else cast(null as int8) end            )  where case when false then ref_41.k_timestamp else ref_41.k_timestamp end       = pg_catalog.current_timestamp())select      ref_44.c7 as c0,     ref_44.c5 as c1  from     jennifer_4 as ref_44  where cast(null as "timestamp") < pg_catalog.current_timestamp()  limit 100;

-- test_case0007 ZDP-33860
select
    subq_2.c7 as c0,
    subq_2.c8 as c1,
    subq_2.c9 as c2,
    subq_2.c3 as c3,
    case when cast(null as "time") >= cast(coalesce(pg_catalog.localtime(),
                                                    pg_catalog.current_time()) as "time") then subq_2.c9 else subq_2.c9 end
              as c4,
    subq_2.c10 as c5,
    subq_2.c12 as c6
from
    (select
         subq_1.c10 as c0,
         subq_1.c8 as c1,
         subq_1.c8 as c2,
         subq_1.c6 as c3,
         subq_1.c4 as c4,
         subq_1.c0 as c5,
         subq_1.c6 as c6,
         subq_1.c9 as c7,
         subq_1.c8 as c8,
         subq_1.c0 as c9,
         subq_1.c4 as c10,
         subq_1.c9 as c11,
         subq_1.c5 as c12,
         subq_1.c1 as c13,
         case when (cast(null as jsonb) ?& cast(null as _text))
             or (false) then subq_1.c2 else subq_1.c2 end
                    as c14,
         subq_1.c10 as c15,
         subq_1.c1 as c16
     from
         (select
              subq_0.c3 as c0,
              subq_0.c0 as c1,
              subq_0.c4 as c2,
              subq_0.c0 as c3,
              subq_0.c4 as c4,
              subq_0.c1 as c5,
              subq_0.c3 as c6,
              subq_0.c3 as c7,
              subq_0.c1 as c8,
              subq_0.c0 as c9,
              subq_0.c4 as c10
          from
              (select
                   ref_0.powerl_b as c0,
                   ref_0.powerl_a as c1,
                   ref_0.allpower as c2,
                   ref_0.powerf_c as c3,
                   ref_0.pallenergy_a as c4
               from
                   public.t_electmeter as ref_0
               where ((ref_0.vol_ab is not NULL)
                   or (ref_0.rallenergy_b is NULL))
                 and (ref_0.k_timestamp = ref_0.k_timestamp)
                   limit 60) as subq_0
          where ((true)
              and (subq_0.c0 is not NULL))
             or (EXISTS (
              select
                  ref_1.pallpower as c0,
                  ref_1.rallenergy_a as c1,
                  ref_1.allrenergy1 as c2,
                  subq_0.c3 as c3
              from
                  public.t_electmeter as ref_1
              where subq_0.c2 is NULL
              limit 124))
              limit 86) as subq_1
     where case when EXISTS (
         select
             ref_2.cnc_tol_mem as c0,
             ref_2.cnc_status as c1,
             subq_1.c5 as c2,
             ref_2.cnc_sw_sver as c3,
             ref_2.mprog_num as c4
         from
             public.t_cnc as ref_2
         where subq_1.c5 IS DISTINCT FROM subq_1.c3
         limit 142) then cast(null as _timetz) else cast(null as _timetz) end
               > pg_catalog.array_remove(
                   cast(cast(null as _timetz) as _timetz),
                   cast(cast(null as "timetz") as "timetz"))
         limit 107) as subq_2
where (pg_catalog.array_cat(
               cast(pg_catalog.array_cat(
                       cast(cast(null as _uuid) as _uuid),
                       cast(cast(null as _uuid) as _uuid)) as _uuid),
               cast(pg_catalog.array_prepend(
                       cast(pg_catalog.gen_random_uuid() as uuid),
                       cast(pg_catalog.array_append(
                               cast(pg_catalog.array_replace(
                                       cast(cast(null as _uuid) as _uuid),
                                       cast(cast(null as uuid) as uuid),
                                       cast(cast(null as uuid) as uuid)) as _uuid),
                               cast(case when cast(null as bytea) <= cast(null as bytea) then cast(null as uuid) else cast(null as uuid) end
                                   as uuid)) as _uuid)) as _uuid)) IS DISTINCT FROM pg_catalog.array_replace(
    cast(cast(nullif(cast(nullif(case when cast(null as "numeric") IS DISTINCT FROM cast(null as "numeric") then cast(null as _uuid) else cast(null as _uuid) end
    ,
    case when (subq_2.c11 is not NULL)
    or ((subq_2.c9 >= cast(null as int8))
    and (subq_2.c16 is not NULL)) then cast(null as _uuid) else cast(null as _uuid) end
    ) as _uuid),
    case when subq_2.c15 is NULL then cast(null as _uuid) else cast(null as _uuid) end
    ) as _uuid) as _uuid),
    cast(pg_catalog.kwdb_internal.cluster_id() as uuid),
    cast(pg_catalog.uuid_in(
    cast(subq_2.c11 as float8)) as uuid)))
  and ((true)
    or (false IS DISTINCT FROM pg_catalog.pg_is_xlog_replay_paused()))
    limit 51;


-- test_case0008
insert into t_cnc values (now(),'123','','','','','','','','','','1','1','6','1|1|1|1|1|1','3|0|3|3|3|3','mm|rpm|mm|mm|mm|mm','6|1|2|3|4|5','XA||XM|YM|ZM|WG','',0.1,'-800||-3410|50|538.70996|-800','-800||-5356.1997','0.3','115','','-287.74738,3.4272285','0','50','50','26.770836','','','','','',' x14_N_114H_30_P','','','','-1','','4','plc|plc|plc|plc','700238|700456','水位低','2022-07-23 10:29:56','2','','','','','','','','','0',0,'','','','','','','','',0);
WITH jennifer_0 AS (select      subq_2.c3 as c0,     subq_2.c7 as c1,     subq_2.c3 as c2,     subq_2.c1 as c3,     subq_2.c7 as c4  from     (select            subq_1.c3 as c0,           subq_1.c3 as c1,           subq_1.c2 as c2,           subq_1.c1 as c3,           subq_1.c2 as c4,           case when cast(null as _numeric) = cast(null as _numeric) then subq_1.c2 else subq_1.c2 end             as c5,           subq_1.c1 as c6,           subq_1.c0 as c7,           subq_1.c2 as c8        from           (select                  58 as c0,                 71 as c1,                 subq_0.c1 as c2,                 subq_0.c1 as c3              from                 (select                        ref_0.axis_num as c0,                       ref_0.cur_tool_len as c1                    from                       public.t_cnc as ref_0                    where cast(null as date) < cast(null as "timestamp")) as subq_0              where (true)                 and ((true)                   and (EXISTS (                    select                          ref_1.powerr_b as c0,                         subq_0.c1 as c1                      from                         public.t_electmeter as ref_1                      where false                      limit 61)))              limit 118) as subq_1        where (subq_1.c2 is NULL)           or (cast(null as _bytea) <= cast(null as _bytea))        limit 82) as subq_2  where pg_catalog.time_recv(      cast(subq_2.c2 as "varchar")) IS NOT DISTINCT FROM pg_catalog.time_recv(      cast(subq_2.c2 as "varchar"))), jennifer_1 AS (select      subq_5.c3 as c0,     subq_5.c3 as c1,     cast(nullif(subq_5.c1,      case when cast(null as _inet) != cast(null as _inet) then subq_5.c3 else subq_5.c3 end        ) as float8) as c2,     subq_5.c1 as c3  from     (select            subq_3.c4 as c0,           subq_3.c4 as c1,           subq_3.c2 as c2,           subq_3.c0 as c3        from           (select                  ref_2.cur_a as c0,                 ref_2.allrenergy2 as c1,                 ref_2.allrenergy2 as c2,                 ref_2.powerf as c3,                 ref_2.powerr_b as c4,                 ref_2.powerr_b as c5              from                 public.t_electmeter as ref_2              where EXISTS (                select                      ref_2.allrenergy1 as c0,                     ref_2.rallenergy_c as c1,                     ref_3.vol_ca as c2,                     ref_2.powerl_a as c3,                     ref_2.powera_c as c4,                     ref_3.pallenergy_c as c5,                     ref_2.powerr as c6,                     ref_3.op_group as c7,                     ref_3.cur_c as c8,                     ref_3.op_group as c9,                     ref_2.powera_a as c10,                     ref_3.pallenergy_a as c11,                     ref_2.powerr_c as c12,                     ref_2.powerf as c13,                     ref_2.pallpower as c14,                     ref_2.powerl as c15                  from                     public.t_electmeter as ref_3                  where (ref_3.allrenergy1_c is not NULL)                     or (cast(null as "numeric") <= cast(null as "numeric"))                  limit 97)) as subq_3        where (cast(null as jsonb) != cast(null as jsonb))           or (EXISTS (            select                  subq_3.c0 as c0,                 subq_4.c0 as c1,                 subq_3.c3 as c2              from                 (select                        subq_3.c4 as c0,                       subq_3.c0 as c1                    from                       public.t_cnc as ref_4                    where (EXISTS (                        select                              subq_3.c2 as c0                          from                             public.t_cnc as ref_5                          where ref_4.path_quantity is not NULL                          limit 11))                       and (ref_4.value4 is not NULL)                    limit 128) as subq_4              where true))        limit 141) as subq_5  where (subq_5.c0 is NULL)     or (pg_catalog.inet_server_port() < pg_catalog.array_position(        cast(case when (cast(null as "timestamp") = cast(null as "timestamp"))             or ((cast(null as uuid) <= cast(null as uuid))               or (cast(null as _timestamptz) IS DISTINCT FROM cast(null as _timestamptz))) then cast(nullif(cast(null as _timestamptz),            cast(null as _timestamptz)) as _timestamptz) else cast(nullif(cast(null as _timestamptz),            cast(null as _timestamptz)) as _timestamptz) end           as _timestamptz),        cast(pg_catalog.transaction_timestamp() as "timestamptz")))), jennifer_2 AS (select      subq_7.c0 as c0,     76 as c1,     subq_7.c1 as c2,     subq_7.c1 as c3  from     (select            subq_6.c1 as c0,           subq_6.c2 as c1        from           (select                  ref_6.powera_a as c0,                 ref_6.powerf_c as c1,                 ref_6.k_timestamp as c2,                 ref_6.vol_b as c3              from                 public.t_electmeter as ref_6              where cast(null as "timetz") IS DISTINCT FROM cast(null as "timetz")) as subq_6        where (subq_6.c2 is not NULL)           and (cast(null as "timestamptz") > subq_6.c2)        limit 71) as subq_7  where false  limit 153)select      subq_9.c1 as c0  from     (select distinct           subq_8.c3 as c0,           subq_8.c1 as c1        from           (select                  ref_7.c1 as c0,                 ref_7.c0 as c1,                 ref_7.c1 as c2,                 ref_7.c3 as c3,                 ref_7.c0 as c4              from                 jennifer_2 as ref_7              where ((cast(null as inet) > cast(null as inet))                   or (cast(null as _text) < cast(null as _text)))                 and ((cast(null as "timestamp") <= cast(null as "timestamp"))                   and (false))              limit 95) as subq_8        where (EXISTS (            select                  ref_8.c1 as c0,                 subq_8.c4 as c1,                 subq_8.c1 as c2,                 ref_8.c3 as c3,                 ref_8.c2 as c4,                 ref_8.c4 as c5,                 ref_8.c4 as c6,                 ref_8.c0 as c7,                 subq_8.c0 as c8,                 ref_8.c3 as c9,                 subq_8.c0 as c10,                 68 as c11,                 subq_8.c2 as c12,                 subq_8.c1 as c13,                 ref_8.c1 as c14              from                 jennifer_0 as ref_8              where (subq_8.c1 is not NULL)                 or ((false)                   and (cast(null as _int8) IS DISTINCT FROM cast(null as _int8)))              limit 174))           or (false)        limit 149) as subq_9  where cast(null as float8) IS DISTINCT FROM pg_catalog.character_length(      cast(pg_catalog.uuid_v4() as bytea))  limit 120;

-- test_case0009
create ts database test_select_function;
create table test_select_function.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t4,t8,t12);

insert into test_select_function.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select_function.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,-32768,-2147483648,-9223372036854775808,false,-922.123,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','\0test查询  @TEST1\0','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select_function.tb values ('2022-05-01 12:10:25','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()&^%{}','\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,400,4000,false,50.555,500.578578,'d','\test测试！！！@TEST1','e','test测试！T  EST1xaa','查询查询 ',e'\\','e','es1023_2','s_ 4','ww4096_2');

insert into test_select_function.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',32767,2147483647,9223372036854775807,true,922.123,500.578578,'','     ',' ','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_select_function.tb values ('2022-05-01 12:10:31.22','2020-05-01 22:30:11',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()&^%{}','\', 'v2551cdf~#   ', '  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,300,300,false,60.666,600.678,'','\test测试！！！@TEST1',' ','test测试！T  EST1xaa','查询查询 ',e'\\','','    ','','  ');
insert into test_select_function.tb values ('2023-05-10 09:08:19.22','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e',e'\a',null,'测试测试 ','b','test测试10_1','vwwws中文_1',null);
insert into test_select_function.tb values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'测试测试 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_select_function.tb values ('2023-05-10 09:08:18.223','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,6,100,1000,false,-10.123,100.111111,'b','\TEST1 ','f','测试！TEST1xaa','5555 5','  bdbd','y','test@测试！10_1','vwwws_1','cddde');
-- '
select e12 from test_select_function.tb where e12 is not null and e12 in('varchar  中文1');
drop database test_select_function cascade;

-- test_case0010
create ts database test_select;
create table test_select.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t4,t8,t12);
insert into test_select.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,-32768,-2147483648,-9223372036854775808,false,-922.123,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','\0test查询  @TEST1\0','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select.tb values ('2022-05-01 12:10:25','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()&^%{}','\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,400,4000,false,50.555,500.578578,'d','\test测试！！！@TEST1','e','test测试！T  EST1xaa','查询查询 ',e'\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_select.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',32767,2147483647,9223372036854775807,true,922.123,500.578578,'','     ',' ','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_select.tb values ('2022-05-01 12:10:31.22','2020-05-01 22:30:11',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()&^%{}','\', 'v2551cdf~#   ', '  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,300,300,false,60.666,600.678,'','\test测试！！！@TEST1',' ','test测试！T  EST1xaa','查询查询 ',e'\\','','    ','','  ');
insert into test_select.tb values ('2023-05-10 09:08:19.22','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e',e'\a',null,'测试测试 ','b','test测试10_1','vwwws中文_1',null);
insert into test_select.tb values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'测试测试 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_select.tb values ('2023-05-10 09:08:18.223','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,6,100,1000,false,-10.123,100.111111,'b','\TEST1 ','f','测试！TEST1xaa','5555 5','  bdbd','y','test@测试！10_1','vwwws_1','cddde');
select t6,e5 from test_select.tb order by t6, e5;
select t6,e5 from test_select.tb where e5 in(-500000.5,500000.5,null) and t6 not in(700.5675675) order by t6, e5;
explain select t6,e5 from test_select.tb where e5 in(-500000.5,500000.5,null) and e2=500 order by t6, e5;
select t6,e5 from test_select.tb where e5 in(-500000.5,500000.5,null) and e2=500 order by t6, e5;
select t4,e7 from test_select.tb where t4 in(true,false) and e7 in(true) order by t4,e7;
use defaultdb;
drop database test_select cascade;


-- test_case0011
use test;
select  
  subq_1.c1 as c0, 
  case when cast(null as _timestamp) < cast(null as _timestamp) then subq_1.c0 else subq_1.c0 end
     as c1, 
  subq_1.c0 as c2, 
  subq_1.c0 as c3, 
  subq_1.c1 as c4
from 
  (select  
        subq_0.c5 as c0, 
        subq_0.c6 as c1
      from 
        (select  
              ref_1.value2 as c0, 
              ref_1.parts_count as c1, 
              ref_0.powerl_c as c2, 
              ref_0.vol_ca as c3, 
              ref_0.powerf_b as c4, 
              ref_1.mach_pos as c5, 
              ref_0.powerl_a as c6, 
              ref_0.k_timestamp as c7, 
              ref_0.powera_b as c8, 
              ref_0.rallenergy_c as c9
            from 
              public.t_electmeter as ref_0
                inner join public.t_cnc as ref_1
                on (ref_0.powerr_b = ref_1.sp_override )
            where ((ref_0.rallpower != cast(null as "numeric")) 
                or (cast(null as "numeric") IS NOT DISTINCT FROM cast(null as int8))) 
              or (cast(null as anyarray) && cast(null as anyarray))
            limit 154) as subq_0
      where subq_0.c4 != subq_0.c6
      limit 125) as subq_1
where subq_1.c1 IS DISTINCT FROM subq_1.c1
limit 92;

-- test_case0012 ZDP-33967
select  
  ref_6.cur_tool_z_len as c0, 
  subq_0.c1 as c1, 
  cast(nullif(ref_8.allrenergy1_b,
    ref_5.rallenergy_c) as int4) as c2, 
  ref_2.servo_load as c3, 
  ref_2.prog_seq_num as c4, 
  ref_2.cur_tool_len as c5
from 
  public.t_cnc as ref_0
          left join public.t_electmeter as ref_1
          on (ref_1.rallenergy_a is NULL)
        left join public.t_cnc as ref_2
            right join public.t_cnc as ref_3
                inner join public.t_electmeter as ref_4
                on (ref_4.k_timestamp < cast(null as "timestamp"))
              inner join public.t_electmeter as ref_5
              on (true)
            on (cast(null as jsonb) != (select pg_catalog.json_agg(k_timestamp) from public.t_electmeter)
                  )
          inner join public.t_cnc as ref_6
          on (ref_2.k_timestamp IS NOT DISTINCT FROM cast(null as "timestamp"))
        on (cast(null as text) IS NOT DISTINCT FROM cast(null as text))
      left join (select  
              ref_7.sprog_num as c0, 
              ref_7.mprog_num as c1
            from 
              public.t_cnc as ref_7
            where (ref_7.mach_pos is NULL) 
              or ((cast(null as _date) <= cast(null as _date)) 
                and (cast(null as int8) != cast(null as "numeric")))
            limit 70) as subq_0
        right join public.t_electmeter as ref_8
        on (((false) 
              or (cast(null as _jsonb) <= cast(null as _jsonb))) 
            and (cast(null as "numeric") <= cast(null as int8)))
      on ((ref_5.powerl_b is NULL) 
          and ((cast(null as _float8) >= cast(null as _float8)) 
            and (ref_6.feed_set_speed is not NULL)))
    left join public.t_electmeter as ref_9
    on (pg_catalog.array_remove(
          cast((select pg_catalog.array_agg(vol_a) from public.t_electmeter)
             as _float8),
          cast(ref_1.powerl_a as float8)) IS DISTINCT FROM (select pg_catalog.array_agg(vol_a) from public.t_electmeter)
          )
where true
limit 61;

-- test_case0013 ZDP-33921
select  
  ref_6.cur_tool_z_len as c0, 
  subq_0.c1 as c1, 
  cast(nullif(ref_8.allrenergy1_b,
    ref_5.rallenergy_c) as int4) as c2, 
  ref_2.servo_load as c3, 
  ref_2.prog_seq_num as c4, 
  ref_2.cur_tool_len as c5
from 
  public.t_cnc as ref_0
          left join public.t_electmeter as ref_1
          on (ref_1.rallenergy_a is NULL)
        left join public.t_cnc as ref_2
            right join public.t_cnc as ref_3
                inner join public.t_electmeter as ref_4
                on (ref_4.k_timestamp < cast(null as "timestamp"))
              inner join public.t_electmeter as ref_5
              on (true)
            on (cast(null as jsonb) != (select pg_catalog.json_agg(k_timestamp) from public.t_electmeter)
                  )
          inner join public.t_cnc as ref_6
          on (ref_2.k_timestamp IS NOT DISTINCT FROM cast(null as "timestamp"))
        on (cast(null as text) IS NOT DISTINCT FROM cast(null as text))
      left join (select  
              ref_7.sprog_num as c0, 
              ref_7.mprog_num as c1
            from 
              public.t_cnc as ref_7
            where (ref_7.mach_pos is NULL) 
              or ((cast(null as _date) <= cast(null as _date)) 
                and (cast(null as int8) != cast(null as "numeric")))
            limit 70) as subq_0
        right join public.t_electmeter as ref_8
        on (((false) 
              or (cast(null as _jsonb) <= cast(null as _jsonb))) 
            and (cast(null as "numeric") <= cast(null as int8)))
      on ((ref_5.powerl_b is NULL) 
          and ((cast(null as _float8) >= cast(null as _float8)) 
            and (ref_6.feed_set_speed is not NULL)))
    left join public.t_electmeter as ref_9
    on (pg_catalog.array_remove(
          cast((select pg_catalog.array_agg(vol_a) from public.t_electmeter)
             as _float8),
          cast(ref_1.powerl_a as float8)) IS DISTINCT FROM (select pg_catalog.array_agg(vol_a) from public.t_electmeter)
          )
where true
limit 61;

-- test_case0014
select  
  (select vol_c from public.t_electmeter limit 1 offset 10)
     as c0, 
  case when (case when cast(null as "timetz") != cast(null as "time") then cast(null as "timetz") else cast(null as "timetz") end
           != pg_catalog.current_time()) 
      or (subq_1.c0 is NULL) then 81 else 81 end
     as c1, 
  subq_1.c0 as c2, 
  cast(nullif(subq_1.c0,
    subq_1.c0) as "varchar") as c3, 
  3 as c4, 
  subq_1.c0 as c5, 
  subq_1.c0 as c6
from 
  (select  
        (select dist_pos from public.t_cnc limit 1 offset 4)
           as c0
      from 
        public.t_cnc as ref_0
          right join public.t_cnc as ref_1
          on (ref_0.brand = ref_1.cnc_sn ),
        lateral (select  
              ref_2.powerl_b as c0, 
              ref_0.cur_tool_len as c1, 
              ref_0.sp_name as c2
            from 
              public.t_electmeter as ref_2
            where cast(null as _timestamptz) = cast(null as _timestamptz)
            limit 132) as subq_0
      where pg_catalog.array_cat(
          cast(case when ref_0.k_timestamp > cast(null as date) then cast(null as _date) else cast(null as _date) end
             as _date),
          cast(cast(null as _date) as _date)) <= cast(coalesce(cast(null as _date),
          pg_catalog.array_replace(
            cast(cast(null as _date) as _date),
            cast(cast(null as date) as date),
            cast(cast(null as date) as date))) as _date)
      limit 185) as subq_1
where case when (cast(coalesce(cast(coalesce(cast(null as _varbit),
            cast(null as _varbit)) as _varbit),
          cast(null as _varbit)) as _varbit) >= pg_catalog.array_replace(
          cast(cast(null as _varbit) as _varbit),
          cast(cast(null as "varbit") as "varbit"),
          cast(cast(null as "varbit") as "varbit"))) 
      or ((cast(null as "timestamp") >= case when true then cast(null as "timestamptz") else cast(null as "timestamptz") end
            ) 
        and ((((true) 
              or (cast(null as _varbit) IS NOT DISTINCT FROM cast(null as _varbit))) 
            or (subq_1.c0 is NULL)) 
          and (subq_1.c0 is NULL))) then pg_catalog.current_time() else pg_catalog.current_time() end
     = pg_catalog.localtime();

--test_case0015
select 1 from t_cnc as ref_0 join t_electmeter as ref_2 on true
where
cast (
nullif(
cast(null as _jsonb),
case when(ref_2.infre is not NULL)
then
case when
EXISTS (select 1 from t_cnc where ref_2.powerr is NULL limit 3)
then cast(null as _jsonb) else cast(null as _jsonb) end
else
case when
EXISTS (select 1 from t_cnc where ref_2.powerr is NULL limit 3)
then cast(null as _jsonb) else cast(null as _jsonb) end
end
) as _jsonb ) > case when
EXISTS (select 1 from t_electmeter where EXISTS (select 1 from t_cnc, lateral (select 1 from t_electmeter) limit 5) limit 6)
then cast(null as _jsonb) else cast(null as _jsonb) end;
select  
  ref_26.axis_name as c0, 
  ref_81.elect_name as c1, 
  78 as c2, 
  ref_64.k_timestamp as c3, 
  ref_37.mprog_name as c4, 
  ref_20.elect_name as c5, 
  ref_74.axis_path as c6, 
  ref_19.sprog_name as c7, 
  ref_53.value2 as c8, 
  ref_28.powerl_a as c9, 
  71 as c10, 
  ref_13.rallenergy_a as c11, 
  ref_60.vol_ca as c12
from 
  public.t_cnc as ref_0
            right join public.t_electmeter as ref_1
            on ((false) 
                and (cast(null as "numeric") >= cast(null as int8)))
          inner join public.t_electmeter as ref_2
                  inner join public.t_cnc as ref_3
                  on (cast(null as _text) IS DISTINCT FROM cast(null as _text))
                inner join public.t_electmeter as ref_4
                on ((((ref_2.powerr_b < cast(null as "numeric")) 
                        or (ref_3.alarm_code is NULL)) 
                      or ((select pg_catalog.jsonb_agg(k_timestamp) from public.t_electmeter)
                           ?& cast(null as _text))) 
                    and (cast(null as "timetz") IS NOT DISTINCT FROM cast(null as "timetz")))
              right join public.t_electmeter as ref_5
              on (ref_2.vol_ca = cast(null as int8))
            inner join public.t_electmeter as ref_6
              left join public.t_cnc as ref_7
                inner join public.t_cnc as ref_8
                on (ref_7.cur_tool_z_len = ref_8.cnc_sn )
              on (EXISTS (
                  select  
                      92 as c0, 
                      ref_6.vol_bc as c1, 
                      ref_6.allpower as c2, 
                      ref_7.op_group as c3, 
                      ref_7.sp_name as c4
                    from 
                      public.t_cnc as ref_9
                    where (cast(null as _oid) = cast(null as _oid)) 
                      and (true)
                    limit 43))
            on (ref_3.cnc_cuttime = ref_7.cnc_sn )
          on (true)
        left join public.t_electmeter as ref_10
              inner join public.t_electmeter as ref_11
                  left join public.t_electmeter as ref_12
                  on (cast(null as "timestamp") != cast(null as "timestamp"))
                left join public.t_electmeter as ref_13
                  inner join public.t_electmeter as ref_14
                  on (((cast(null as _time) IS NOT DISTINCT FROM cast(null as _time)) 
                        or ((select pg_catalog.count(k_timestamp) from public.t_electmeter)
                             >= cast(null as int8))) 
                      or (cast(null as "timestamp") = cast(null as date)))
                on (cast(null as _timestamptz) != cast(null as _timestamptz))
              on (cast(null as inet) << cast(null as inet))
            left join public.t_cnc as ref_15
            on ((select pg_catalog.jsonb_agg(k_timestamp) from public.t_cnc)
                   IS NOT DISTINCT FROM cast(null as jsonb))
          right join public.t_cnc as ref_16
              left join public.t_electmeter as ref_17
                inner join public.t_electmeter as ref_18
                on (cast(null as _varbit) IS DISTINCT FROM cast(null as _varbit))
              on (cast(null as "time") = cast(null as "timetz"))
            inner join public.t_cnc as ref_19
                left join public.t_electmeter as ref_20
                on (false)
              inner join public.t_cnc as ref_21
              on (cast(null as _oid) >= cast(null as _oid))
            on (cast(null as "time") IS NOT DISTINCT FROM cast(null as "timetz"))
          on (cast(null as text) NOT ILIKE cast(null as text))
        on (ref_6.allrenergy1_c = ref_21.device_state )
      inner join public.t_electmeter as ref_22
          right join public.t_electmeter as ref_23
          on (ref_22.powerl_a = ref_23.vol_a )
        left join public.t_electmeter as ref_24
          inner join public.t_electmeter as ref_25
              inner join public.t_cnc as ref_26
              on ((select powerl_b from public.t_electmeter limit 1 offset 5)
                     is NULL)
            left join public.t_electmeter as ref_27
                  inner join public.t_electmeter as ref_28
                  on (false)
                left join public.t_electmeter as ref_29
                on ((((true) 
                        or ((ref_29.elect_name is NULL) 
                          or (true))) 
                      or (EXISTS (
                        select  
                            ref_30.cur_tool_z_len_val as c0, 
                            (select powerf_a from public.t_electmeter limit 1 offset 4)
                               as c1
                          from 
                            public.t_cnc as ref_30
                          where cast(null as date) >= ref_27.k_timestamp))) 
                    and ((cast(null as "numeric") != cast(null as int8)) 
                      and (true)))
              right join public.t_cnc as ref_31
              on (cast(null as "interval") IS DISTINCT FROM cast(null as "interval"))
            on (ref_25.allenergy_c = ref_29.allenergy_a )
          on (false)
        on (cast(null as "numeric") < cast(null as int8))
      on (ref_11.cur_b = ref_22.vol_a )
    left join public.t_electmeter as ref_32
              right join public.t_cnc as ref_33
                  inner join public.t_cnc as ref_34
                  on (ref_34.sp_act_speed is NULL)
                right join public.t_cnc as ref_35
                on (EXISTS (
                    select  
                        ref_34.feed_act_speed as c0, 
                        ref_34.k_timestamp as c1, 
                        ref_33.cnc_sn as c2, 
                        (select servo_load from public.t_cnc limit 1 offset 4)
                           as c3, 
                        ref_36.number_of_molds as c4, 
                        ref_34.machine_code as c5, 
                        91 as c6, 
                        ref_34.value1 as c7, 
                        ref_34.servo_load as c8
                      from 
                        public.t_cnc as ref_36
                      where cast(null as jsonb) ? cast(null as text)
                      limit 23))
              on (ref_33.cur_tool_rad_num is not NULL)
            inner join public.t_cnc as ref_37
            on (cast(null as _int8) != cast(null as _int8))
          inner join public.t_cnc as ref_38
          on (cast(null as inet) && cast(null as inet))
        left join public.t_cnc as ref_39
                left join public.t_cnc as ref_40
                  inner join public.t_electmeter as ref_41
                  on ((true) 
                      and ((select pg_catalog.json_agg(k_timestamp) from public.t_electmeter)
                           IS NOT DISTINCT FROM cast(null as jsonb)))
                on (cast(null as _date) != cast(null as _date))
              inner join public.t_cnc as ref_42
              on (ref_41.powerf_a < ref_41.powera_c)
            inner join public.t_electmeter as ref_43
              inner join public.t_cnc as ref_44
                inner join public.t_cnc as ref_45
                on (cast(null as _numeric) > cast(null as _numeric))
              on ((cast(null as "time") < cast(null as "timetz")) 
                  or (cast(null as "time") IS NOT DISTINCT FROM cast(null as "time")))
            on (cast(null as _time) >= cast(null as _time))
          inner join public.t_cnc as ref_46
                left join public.t_electmeter as ref_47
                on (ref_46.cur_tool_rad = ref_47.elect_name )
              right join public.t_electmeter as ref_48
              on (ref_47.pallpower is not NULL)
            right join public.t_cnc as ref_49
                right join public.t_cnc as ref_50
                  inner join public.t_cnc as ref_51
                  on (ref_50.value1 = ref_51.cnc_sn )
                on (cast(null as uuid) IS DISTINCT FROM cast(null as uuid))
              inner join public.t_electmeter as ref_52
                  left join public.t_cnc as ref_53
                  on (ref_52.powera_c = ref_53.sp_override )
                right join public.t_cnc as ref_54
                on (ref_53.cnc_sw_mver is NULL)
              on ((EXISTS (
                    select  
                        ref_55.cur_tool_len as c0, 
                        ref_51.cnc_sn as c1, 
                        90 as c2, 
                        (select cnc_cycletime from public.t_cnc limit 1 offset 5)
                           as c3, 
                        (select abs_pos from public.t_cnc limit 1 offset 97)
                           as c4, 
                        ref_51.cur_tool_y_len_val as c5, 
                        ref_50.sprog_num as c6, 
                        ref_51.rel_pos as c7, 
                        ref_52.powerr as c8, 
                        subq_0.c0 as c9, 
                        ref_50.cur_tool_z_len as c10, 
                        ref_55.sp_override as c11, 
                        ref_54.dist_pos as c12
                      from 
                        public.t_cnc as ref_55,
                        lateral (select  
                              ref_49.axis_type as c0, 
                              ref_52.k_timestamp as c1
                            from 
                              public.t_electmeter as ref_56
                            where true
                            limit 159) as subq_0
                      where ref_50.k_timestamp >= cast(null as "timestamp"))) 
                  and (cast(null as _text) = cast(null as _text)))
            on (cast(null as _oid) IS NOT DISTINCT FROM cast(null as _oid))
          on ((cast(null as bytea) >= cast(null as bytea)) 
              or (((cast(null as date) IS DISTINCT FROM (select k_timestamp from public.t_electmeter limit 1 offset 1)
                      ) 
                  and (cast(null as _int8) = cast(null as _int8))) 
                or (EXISTS (
                  select  
                      ref_40.abs_pos as c0, 
                      (select rallenergy_a from public.t_electmeter limit 1 offset 2)
                         as c1, 
                      ref_44.alarm_content as c2, 
                      ref_42.axis_type as c3, 
                      ref_40.op_group as c4, 
                      (select machine_code from public.t_electmeter limit 1 offset 6)
                         as c5, 
                      ref_39.mprog_name as c6, 
                      ref_48.powerr_a as c7, 
                      ref_41.allrenergy2_c as c8, 
                      ref_54.cnc_sw_mver as c9, 
                      ref_45.number_of_molds as c10, 
                      ref_49.mach_pos as c11, 
                      ref_51.device_state as c12, 
                      ref_57.rallenergy_c as c13, 
                      ref_42.alarm_type as c14, 
                      (select cur_c from public.t_electmeter limit 1 offset 3)
                         as c15
                    from 
                      public.t_electmeter as ref_57
                    where cast(null as "numeric") < ref_43.infre
                    limit 159))))
        on (ref_50.alarm_code is not NULL)
      left join public.t_electmeter as ref_58
                inner join public.t_cnc as ref_59
                  right join public.t_electmeter as ref_60
                  on ((ref_60.k_timestamp <= cast(null as date)) 
                      and ((cast(null as _time) >= cast(null as _time)) 
                        and (ref_59.k_timestamp != ref_60.k_timestamp)))
                on ((cast(null as _inet) IS NOT DISTINCT FROM cast(null as _inet)) 
                    and ((false) 
                      and (cast(null as int8) <= cast(null as "numeric"))))
              left join public.t_cnc as ref_61
              on (cast(null as _text) > cast(null as _text))
            right join public.t_cnc as ref_62
              inner join public.t_cnc as ref_63
                inner join public.t_electmeter as ref_64
                  inner join public.t_electmeter as ref_65
                  on (ref_65.allrenergy2_b is not NULL)
                on (cast(null as text) IS DISTINCT FROM cast(null as text))
              on (ref_63.device_state is NULL)
            on (cast(null as _time) < cast(null as _time))
          left join public.t_electmeter as ref_66
                  inner join public.t_electmeter as ref_67
                  on (ref_66.allrenergy1_c = ref_67.allenergy_a )
                left join public.t_cnc as ref_68
                on (ref_68.cur_tool_z_len_val is NULL)
              inner join public.t_cnc as ref_69
                left join public.t_cnc as ref_70
                on ((((select pg_catalog.array_agg(k_timestamp) from public.t_cnc)
                           != (select pg_catalog.array_agg(k_timestamp) from public.t_cnc)
                          ) 
                      or (EXISTS (
                        select  
                            ref_69.value2 as c0, 
                            ref_70.cur_tool_len_num as c1, 
                            ref_69.brand as c2, 
                            ref_70.cnc_status as c3, 
                            ref_70.cur_tool_rad as c4, 
                            ref_69.cur_tool_rad_num as c5, 
                            ref_70.axis_quantity as c6
                          from 
                            public.t_electmeter as ref_71
                          where cast(null as _varbit) IS DISTINCT FROM cast(null as _varbit)
                          limit 54))) 
                    or (ref_70.k_timestamp = ref_69.k_timestamp))
              on (ref_67.powerl_b = ref_69.sp_override )
            inner join public.t_cnc as ref_72
            on (ref_66.powerl = ref_67.vol_ca)
          on ((cast(null as _interval) IS DISTINCT FROM cast(null as _interval)) 
              or (cast(null as date) > ref_72.k_timestamp))
        inner join public.t_cnc as ref_73
              left join public.t_cnc as ref_74
              on (cast(null as "timetz") IS NOT DISTINCT FROM cast(null as "timetz"))
            inner join public.t_electmeter as ref_75
                right join public.t_electmeter as ref_76
                on (ref_75.k_timestamp IS DISTINCT FROM cast(null as "timestamp"))
              inner join public.t_cnc as ref_77
                inner join public.t_cnc as ref_78
                on (ref_78.sp_name is NULL)
              on (cast(null as int8) != cast(null as "numeric"))
            on (cast(null as _varbit) IS NOT DISTINCT FROM cast(null as _varbit))
          inner join public.t_electmeter as ref_79
                inner join public.t_electmeter as ref_80
                on ((cast(null as text) != cast(null as text)) 
                    or (cast(null as date) = ref_79.k_timestamp))
              inner join public.t_electmeter as ref_81
                left join public.t_cnc as ref_82
                on ((select pg_catalog.array_agg(k_timestamp) from public.t_electmeter)
                       <= cast(null as _timestamptz))
              on (cast(null as _varbit) > cast(null as _varbit))
            inner join public.t_electmeter as ref_83
            on ((select infre from public.t_electmeter limit 1 offset 3)
                   is not NULL)
          on ((select k_timestamp from public.t_electmeter limit 1 offset 3)
                 < ref_74.k_timestamp)
        on (true)
      on (cast(null as _text) IS DISTINCT FROM cast(null as _text))
    on (ref_1.pallenergy_c is NULL)
where (cast(nullif(cast(coalesce(cast(null as _jsonb),
        cast(coalesce(cast(null as _jsonb),
          cast(null as _jsonb)) as _jsonb)) as _jsonb),
      case when ((cast(null as uuid) = cast(null as uuid)) 
            or (false)) 
          and (ref_64.infre is not NULL) then case when cast(null as uuid) < cast(null as uuid) then case when EXISTS (
              select  
                  ref_80.allenergy_c as c0, 
                  ref_2.allenergy_b as c1, 
                  ref_44.cur_tool_len_val as c2, 
                  ref_12.allenergy_c as c3, 
                  ref_46.mprog_name as c4, 
                  ref_31.mprog_num as c5, 
                  ref_45.feed_set_speed as c6, 
                  ref_41.vol_ab as c7, 
                  96 as c8
                from 
                  public.t_cnc as ref_84
                where ref_76.powerr is NULL
                limit 163) then cast(null as _jsonb) else cast(null as _jsonb) end
             else case when EXISTS (
              select  
                  ref_80.allenergy_c as c0, 
                  ref_2.allenergy_b as c1, 
                  ref_44.cur_tool_len_val as c2, 
                  ref_12.allenergy_c as c3, 
                  ref_46.mprog_name as c4, 
                  ref_31.mprog_num as c5, 
                  ref_45.feed_set_speed as c6, 
                  ref_41.vol_ab as c7, 
                  96 as c8
                from 
                  public.t_cnc as ref_84
                where ref_76.powerr is NULL
                limit 163) then cast(null as _jsonb) else cast(null as _jsonb) end
             end
           else case when cast(null as uuid) < cast(null as uuid) then case when EXISTS (
              select  
                  ref_80.allenergy_c as c0, 
                  ref_2.allenergy_b as c1, 
                  ref_44.cur_tool_len_val as c2, 
                  ref_12.allenergy_c as c3, 
                  ref_46.mprog_name as c4, 
                  ref_31.mprog_num as c5, 
                  ref_45.feed_set_speed as c6, 
                  ref_41.vol_ab as c7, 
                  96 as c8
                from 
                  public.t_cnc as ref_84
                where ref_76.powerr is NULL
                limit 163) then cast(null as _jsonb) else cast(null as _jsonb) end
             else case when EXISTS (
              select  
                  ref_80.allenergy_c as c0, 
                  ref_2.allenergy_b as c1, 
                  ref_44.cur_tool_len_val as c2, 
                  ref_12.allenergy_c as c3, 
                  ref_46.mprog_name as c4, 
                  ref_31.mprog_num as c5, 
                  ref_45.feed_set_speed as c6, 
                  ref_41.vol_ab as c7, 
                  96 as c8
                from 
                  public.t_cnc as ref_84
                where ref_76.powerr is NULL
                limit 163) then cast(null as _jsonb) else cast(null as _jsonb) end
             end
           end
        ) as _jsonb) > case when EXISTS (
        select  
            ref_17.vol_ca as c0, 
            ref_50.cur_tool_rad_val as c1, 
            ref_51.alarm_content as c2, 
            ref_65.allpower as c3, 
            ref_0.prog_seq_num as c4, 
            ref_83.powera_b as c5, 
            ref_7.alarm_content as c6, 
            ref_66.vol_bc as c7, 
            53 as c8, 
            ref_29.pallenergy_a as c9, 
            ref_72.cnc_sw_mver as c10, 
            ref_25.powerl_c as c11, 
            ref_23.powerl_b as c12, 
            ref_21.cur_tool_rad_val as c13, 
            ref_17.rallenergy_a as c14, 
            83 as c15, 
            ref_81.vol_bc as c16, 
            ref_43.powerr as c17, 
            ref_18.allpower as c18, 
            ref_34.cur_tool_x_len_val as c19
          from 
            public.t_electmeter as ref_85
                right join public.t_electmeter as ref_86
                on (false)
              inner join public.t_electmeter as ref_87
              on ((EXISTS (
                    select  
                        (select powerf_b from public.t_electmeter limit 1 offset 6)
                           as c0, 
                        ref_37.cur_tool_len_num as c1, 
                        ref_26.prog_seq_num as c2, 
                        subq_1.c1 as c3
                      from 
                        public.t_cnc as ref_88,
                        lateral (select  
                              ref_45.cur_tool_rad as c0, 
                              ref_49.rel_pos as c1, 
                              ref_0.cur_tool_len_num as c2
                            from 
                              public.t_electmeter as ref_89
                            where cast(null as _timestamp) <= cast(null as _timestamp)) as subq_1
                      where true
                      limit 94)) 
                  or (((cast(null as "time") != cast(null as "time")) 
                      or (cast(null as "numeric") IS DISTINCT FROM cast(null as int8))) 
                    and (cast(null as _int8) >= cast(null as _int8))))
          where false IS DISTINCT FROM false
          limit 119) then cast(null as _jsonb) else cast(null as _jsonb) end
      ) 
  or (EXISTS (
    select  
        ref_46.feed_override as c0, 
        ref_47.cur_c as c1, 
        ref_53.cnc_use_mem as c2, 
        (select allrenergy1 from public.t_electmeter limit 1 offset 5)
           as c3, 
        75 as c4, 
        ref_62.feed_override as c5
      from 
        public.t_cnc as ref_90
              inner join public.t_cnc as ref_91
              on (ref_90.cur_tool_x_len = ref_91.cnc_sn )
            right join public.t_cnc as ref_92
                left join public.t_cnc as ref_93
                  inner join public.t_electmeter as ref_94
                  on (((false) 
                        or (cast(null as int8) <= cast(null as int8))) 
                      or (true))
                on (cast(null as "numeric") < ref_14.vol_ab)
              left join public.t_electmeter as ref_95
                right join public.t_electmeter as ref_96
                on ((select pg_catalog.array_agg(k_timestamp) from public.t_cnc)
                       > cast(null as _timestamptz))
              on (true)
            on (cast(null as date) >= cast(null as "timestamp"))
          left join public.t_cnc as ref_97
                inner join public.t_electmeter as ref_98
                on (ref_15.cnc_unuse_mem is not NULL)
              right join public.t_electmeter as ref_99
              on (ref_97.cnc_sw_mver = ref_99.elect_name )
            inner join public.t_electmeter as ref_100
            on (ref_41.powerf_b IS NOT DISTINCT FROM cast(null as "numeric"))
          on (true)
      where cast(coalesce(cast(coalesce(cast(null as int8),
            cast(null as int8)) as int8),
          cast(null as int8)) as int8) != ref_29.vol_bc
      limit 147));
	  
-- test_case0016 ZDP-34475 
select  
  subq_1.c0 as c0, 
  subq_1.c0 as c1, 
  subq_1.c0 as c2, 
  subq_1.c0 as c3, 
  subq_1.c0 as c4, 
  subq_1.c0 as c5
from 
  (select  
        subq_0.c6 as c0
      from 
        (select  
              ref_0.abs_pos as c0, 
              93 as c1, 
              ref_0.cur_tool_len as c2, 
              ref_0.sp_load as c3, 
              ref_0.cur_tool_len_val as c4, 
              ref_0.alarm_code as c5, 
              ref_0.axis_unit as c6, 
              ref_0.cnc_sw_mver as c7, 
              ref_0.sprog_num as c8, 
              ref_0.axis_unit as c9, 
              ref_0.value5 as c10, 
              ref_0.prog_seq_num as c11
            from 
              public.t_cnc as ref_0
            where ref_0.axis_path is NULL
            limit 106) as subq_0
      where cast(coalesce(cast(null as "timestamp"),
          pg_catalog.transaction_timestamp()) as "timestamp") IS NOT DISTINCT FROM cast(coalesce(case when (cast(null as _bytea) IS NOT DISTINCT FROM cast(null as _bytea)) 
              or (cast(null as "timestamptz") >= cast(null as "timestamp")) then cast(null as "timestamp") else cast(null as "timestamp") end
            ,
          case when false then cast(null as "timestamp") else cast(null as "timestamp") end
            ) as "timestamp")) as subq_1
where subq_1.c0 is NULL;

-- test_case0017 ZDP-33982
select  
  ref_0.powerf_a as c0, 
  ref_0.allrenergy2_c as c1, 
  61 as c2, 
  ref_0.vol_ab as c3, 
  (select powerf_a from public.t_electmeter limit 1 offset 1)
     as c4, 
  ref_0.powerf as c5, 
  ref_0.vol_a as c6, 
  ref_0.k_timestamp as c7, 
  ref_0.elect_name as c8, 
  ref_0.op_group as c9, 
  ref_0.op_group as c10, 
  ref_0.cur_c as c11, 
  ref_0.infre as c12, 
  case when true then ref_0.powerr_c else ref_0.powerr_c end
     as c13
from 
  public.t_electmeter as ref_0
where case when (cast(null as _int8) IS DISTINCT FROM cast(coalesce(cast(null as _int8),
          pg_catalog.array_positions(
            cast(cast(null as _float8) as _float8),
            cast(ref_0.powerl_b as float8))) as _int8)) 
      and (((cast(null as _timetz) = cast(null as _timetz)) 
          or (ref_0.k_timestamp >= cast(null as date))) 
        and (pg_catalog.pg_client_encoding() IS DISTINCT FROM pg_catalog.split_part(
            cast(cast(null as text) as text),
            cast(cast(null as text) as text),
            cast(cast(null as int8) as int8)))) then pg_catalog.kwdb_internal.is_admin() else pg_catalog.kwdb_internal.is_admin() end
     < pg_catalog.not_like_escape(
    cast(cast(null as text) as text),
    cast(case when (ref_0.allrenergy1_a is not NULL) 
        and ((cast(null as bytea) < cast(null as bytea)) 
          or (cast(null as _inet) > cast(null as _inet))) then pg_catalog.pg_get_constraintdef(
        cast(pg_catalog.pg_my_temp_schema() as oid)) else pg_catalog.pg_get_constraintdef(
        cast(pg_catalog.pg_my_temp_schema() as oid)) end
       as text),
    cast(case when (false) 
        or (cast(null as int8) = cast(null as int8)) then cast(nullif(pg_catalog.btrim(
          cast(cast(nullif(cast(null as text),
            cast(null as text)) as text) as text)),
        cast(nullif(pg_catalog.kwdb_internal.cluster_name(),
          case when cast(null as "interval") >= cast(null as "interval") then cast(null as text) else cast(null as text) end
            ) as text)) as text) else cast(nullif(pg_catalog.btrim(
          cast(cast(nullif(cast(null as text),
            cast(null as text)) as text) as text)),
        cast(nullif(pg_catalog.kwdb_internal.cluster_name(),
          case when cast(null as "interval") >= cast(null as "interval") then cast(null as text) else cast(null as text) end
            ) as text)) as text) end
       as text));

select 1 from t_electmeter as ref_0 where pg_catalog.kwdb_internal.is_admin() < pg_catalog.not_like_escape(cast(cast(null as text) as text),cast(pg_catalog.pg_get_constraintdef(cast(pg_catalog.pg_my_temp_schema() as oid)) as text),pg_catalog.btrim(cast(null as text)));
	   
-- test_case0018 ZDP-36559
select  
  cast(coalesce(subq_0.c1,
    subq_0.c1) as "varchar") as c0, 
  cast(coalesce(subq_0.c0,
    subq_0.c1) as "varchar") as c1, 
  subq_0.c0 as c2, 
  subq_0.c1 as c3, 
  subq_0.c1 as c4, 
  subq_0.c0 as c5, 
  subq_0.c1 as c6, 
  subq_0.c1 as c7, 
  subq_0.c0 as c8, 
  case when pg_catalog.current_timestamp() != case when (cast(null as "timestamp") < cast(null as date)) 
          or (cast(null as _int8) > cast(null as _int8)) then cast(null as date) else cast(null as date) end
         then subq_0.c1 else subq_0.c1 end
     as c9, 
  case when pg_catalog.transaction_timestamp() < pg_catalog.timezone(
        cast(case when cast(null as date) < cast(null as date) then pg_catalog.timeofday() else pg_catalog.timeofday() end
           as text),
        cast(
          pg_catalog.string_agg(
            cast(cast(null as text) as text),
            cast(cast(null as text) as text)) over (partition by subq_0.c0 order by subq_0.c1) as text)) then subq_0.c0 else subq_0.c0 end
     as c10, 
  subq_0.c1 as c11
from 
  (select  
        ref_0.sp_name as c0, 
        ref_0.machine_code as c1
      from 
        public.t_cnc as ref_0
      where true
      limit 104) as subq_0
where subq_0.c1 is not NULL
limit 122;

-- test_case1000
use defaultdb;
drop database test cascade;