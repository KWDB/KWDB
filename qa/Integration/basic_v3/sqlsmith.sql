create ts database test_vacuum;
create table test_vacuum.t1 (k_timestamp timestamptz not null,id int not null,e1 int2,e2 int,e3 int8,e4 float4,e5 float8,e6 bool,e7 timestamptz,e8 char(1023),e9 nchar(255),e10 varchar(4096),e11 char,e12 char(255),e13 nchar,e14 nvarchar(4096),e15 varchar(1023),e16 nvarchar(200),e17 nchar(255),e18 char(200),e19 varbytes,e20 varbytes(60),e21 varchar,e22 nvarchar) tags (code1 int2 not null,code2 int,code3 int8,code4 float4,code5 float8,code6 bool,code7 varchar,code8 varchar(128) not null,code9 varbytes,code10 varbytes(60),code11 varchar,code12 varchar(60),code13 char(2),code14 char(1023) not null,code15 nchar,code16 nchar(254) not null) primary tags (code1) activetime 2d partition interval 1d;

set cluster setting ts.parallel_degree = 8;
set statement_timeout='10s';

use test_vacuum;
select
  subq_0.c8 as c0,
  ref_6.e19 as c1,
  (select e7 from public.t1 limit 1 offset 5)
     as c2,
  ref_19.code14 as c3,
  65 as c4,
  ref_20.e14 as c5,
  ref_21.e2 as c6,
  cast(coalesce(ref_17.e1,
    ref_2.code1) as int2) as c7,
  ref_0.code15 as c8,
  ref_21.e19 as c9
from
  public.t1 as ref_0
            right join public.t1 as ref_1
            on ((ref_1.e11 is not NULL)
                or (false))
          inner join public.t1 as ref_2
            left join public.t1 as ref_3
            on (false)
          on (((cast(null as _text) IS NOT DISTINCT FROM cast(null as _text))
                or (ref_1.e6 >= ref_0.e6))
              and (((select code12 from public.t1 limit 1 offset 3)
                     is not NULL)
                or (cast(null as text) IS DISTINCT FROM cast(null as text))))
        inner join public.t1 as ref_4
              inner join public.t1 as ref_5
              on (ref_4.code3 <= cast(null as "numeric"))
            left join public.t1 as ref_6
              left join public.t1 as ref_7
              on ((cast(null as _date) > cast(null as _date))
                  and ((cast(null as _float8) <= cast(null as _float8))
                    and (cast(null as "timetz") > cast(null as "time"))))
            on (cast(null as date) != cast(null as date))
          inner join public.t1 as ref_8
          on (EXISTS (
              select
                  ref_5.e15 as c0
                from
                  public.t1 as ref_9
                where cast(null as _bool) = cast(null as _bool)
                limit 93))
        on (EXISTS (
            select
                ref_3.e17 as c0,
                ref_7.e14 as c1,
                ref_10.id as c2
              from
                public.t1 as ref_10
              where (ref_7.e11 is not NULL)
                and ((ref_6.e6 != ref_2.e6)
                  and (false))
              limit 90))
      inner join public.t1 as ref_11
          inner join public.t1 as ref_12
          on (ref_11.id = ref_12.id )
        inner join (select
                ref_13.e22 as c0,
                ref_13.e13 as c1,
                ref_13.e17 as c2,
                ref_13.e7 as c3,
                ref_13.e5 as c4,
                (select e20 from public.t1 limit 1 offset 73)
                   as c5,
                ref_13.e2 as c6,
                ref_13.code6 as c7,
                ref_13.code10 as c8,
                ref_13.e17 as c9,
                ref_13.e14 as c10
              from
                public.t1 as ref_13
              where ref_13.e19 is not NULL
              limit 54) as subq_0
          left join public.t1 as ref_14
          on (EXISTS (
              select
                  (select code2 from public.t1 limit 1 offset 1)
                     as c0,
                  ref_15.e3 as c1,
                  ref_14.code16 as c2,
                  ref_14.e18 as c3,
                  subq_0.c7 as c4
                from
                  public.t1 as ref_15
                where false
                limit 169))
        on (ref_12.e6 = ref_14.e6 )
      on ((ref_14.e7 is not NULL)
          and (cast(null as _uuid) > cast(null as _uuid)))
    right join public.t1 as ref_16
        left join public.t1 as ref_17
          inner join public.t1 as ref_18
          on (ref_17.code16 = ref_18.e9 )
        on (cast(null as "time") <= cast(null as "timetz"))
      inner join public.t1 as ref_19
          right join public.t1 as ref_20
            inner join public.t1 as ref_21
            on (ref_20.code3 IS DISTINCT FROM ref_20.code5)
          on (cast(null as _time) != cast(null as _time))
        left join public.t1 as ref_22
        on (cast(null as float8) != ref_22.code5)
      on (cast(coalesce(cast(null as text),
            cast(null as text)) as text) < cast(null as text))
    on (cast(null as text) IS DISTINCT FROM pg_catalog.getdatabaseencoding())
where case when ((EXISTS (
          select
              ref_14.code6 as c0,
              ref_14.e22 as c1,
              ref_18.code15 as c2,
              (select e14 from public.t1 limit 1 offset 5)
                 as c3,
              ref_3.e6 as c4,
              subq_0.c8 as c5,
              ref_8.e1 as c6,
              ref_8.code3 as c7
            from
              public.t1 as ref_23
                inner join public.t1 as ref_24
                on (ref_23.code4 = ref_24.e4 )
            where EXISTS (
              select
                  ref_1.e5 as c0,
                  83 as c1
                from
                  public.t1 as ref_25
                where cast(null as _jsonb) <= cast(null as _jsonb)
                limit 142)
            limit 35))
        or (EXISTS (
          select
              ref_1.e18 as c0,
              ref_5.k_timestamp as c1,
              subq_0.c6 as c2,
              4 as c3
            from
              public.t1 as ref_26
            where cast(null as "timestamp") <= cast(null as "timestamp"))))
      or (cast(nullif(cast(null as bytea),
          case when cast(null as "time") >= cast(null as "time") then cast(null as bytea) else cast(null as bytea) end
            ) as bytea) >= cast(null as bytea)) then case when cast(null as "interval") > cast(null as "interval") then case when false then case when ref_22.code1 = ref_6.code1 then pg_catalog.subbit(
            cast(ref_16.code3 as int8),
            cast(ref_3.code3 as int8),
            cast(ref_12.e3 as int8)) else pg_catalog.subbit(
            cast(ref_16.code3 as int8),
            cast(ref_3.code3 as int8),
            cast(ref_12.e3 as int8)) end
           else case when ref_22.code1 = ref_6.code1 then pg_catalog.subbit(
            cast(ref_16.code3 as int8),
            cast(ref_3.code3 as int8),
            cast(ref_12.e3 as int8)) else pg_catalog.subbit(
            cast(ref_16.code3 as int8),
            cast(ref_3.code3 as int8),
            cast(ref_12.e3 as int8)) end
           end
         else case when false then case when ref_22.code1 = ref_6.code1 then pg_catalog.subbit(
            cast(ref_16.code3 as int8),
            cast(ref_3.code3 as int8),
            cast(ref_12.e3 as int8)) else pg_catalog.subbit(
            cast(ref_16.code3 as int8),
            cast(ref_3.code3 as int8),
            cast(ref_12.e3 as int8)) end
           else case when ref_22.code1 = ref_6.code1 then pg_catalog.subbit(
            cast(ref_16.code3 as int8),
            cast(ref_3.code3 as int8),
            cast(ref_12.e3 as int8)) else pg_catalog.subbit(
            cast(ref_16.code3 as int8),
            cast(ref_3.code3 as int8),
            cast(ref_12.e3 as int8)) end
           end
         end
       else case when cast(null as "interval") > cast(null as "interval") then case when false then case when ref_22.code1 = ref_6.code1 then pg_catalog.subbit(
            cast(ref_16.code3 as int8),
            cast(ref_3.code3 as int8),
            cast(ref_12.e3 as int8)) else pg_catalog.subbit(
            cast(ref_16.code3 as int8),
            cast(ref_3.code3 as int8),
            cast(ref_12.e3 as int8)) end
           else case when ref_22.code1 = ref_6.code1 then pg_catalog.subbit(
            cast(ref_16.code3 as int8),
            cast(ref_3.code3 as int8),
            cast(ref_12.e3 as int8)) else pg_catalog.subbit(
            cast(ref_16.code3 as int8),
            cast(ref_3.code3 as int8),
            cast(ref_12.e3 as int8)) end
           end
         else case when false then case when ref_22.code1 = ref_6.code1 then pg_catalog.subbit(
            cast(ref_16.code3 as int8),
            cast(ref_3.code3 as int8),
            cast(ref_12.e3 as int8)) else pg_catalog.subbit(
            cast(ref_16.code3 as int8),
            cast(ref_3.code3 as int8),
            cast(ref_12.e3 as int8)) end
           else case when ref_22.code1 = ref_6.code1 then pg_catalog.subbit(
            cast(ref_16.code3 as int8),
            cast(ref_3.code3 as int8),
            cast(ref_12.e3 as int8)) else pg_catalog.subbit(
            cast(ref_16.code3 as int8),
            cast(ref_3.code3 as int8),
            cast(ref_12.e3 as int8)) end
           end
         end
       end
     = case when (EXISTS (
        select
            ref_11.code4 as c0,
            ref_11.e12 as c1,
            ref_3.e12 as c2,
            ref_22.code7 as c3,
            ref_4.code2 as c4,
            ref_8.code1 as c5,
            ref_3.code6 as c6,
            ref_3.code15 as c7
          from
            public.t1 as ref_27
          where true
          limit 51))
      and (EXISTS (
        select
            97 as c0,
            subq_1.c4 as c1,
            ref_20.code5 as c2,
            subq_2.c4 as c3
          from
            public.t1 as ref_28,
            lateral (select
                  ref_17.e22 as c0,
                  subq_0.c8 as c1,
                  ref_4.e10 as c2,
                  ref_5.id as c3,
                  subq_0.c6 as c4,
                  74 as c5,
                  ref_14.e18 as c6,
                  ref_12.code5 as c7,
                  19 as c8,
                  (select e14 from public.t1 limit 1 offset 44)
                     as c9
                from
                  public.t1 as ref_29
                where ref_2.e3 IS DISTINCT FROM ref_17.code5
                limit 179) as subq_1,
            lateral (select
                  ref_2.e22 as c0,
                  ref_11.e21 as c1,
                  ref_28.code15 as c2,
                  ref_12.e19 as c3,
                  ref_2.e3 as c4,
                  ref_18.code2 as c5
                from
                  public.t1 as ref_30
                where cast(null as _interval) < cast(null as _interval)
                limit 108) as subq_2
          where ref_5.code6 is NULL)) then pg_catalog.rightbit(
      cast(cast(coalesce(ref_14.e6,
        ref_22.code6) as bool) as bool),
      cast(ref_1.e3 as int8)) else pg_catalog.rightbit(
      cast(cast(coalesce(ref_14.e6,
        ref_22.code6) as bool) as bool),
      cast(ref_1.e3 as int8)) end

limit 132;

drop database test_vacuum cascade;

create ts database test_vacuum;
create table test_vacuum.t1 (k_timestamp timestamptz not null,id int not null,e1 int2,e2 int,e3 int8,e4 float4,e5 float8,e6 bool,e7 timestamptz,e8 char(1023),e9 nchar(255),e10 varchar(4096),e11 char,e12 char(255),e13 nchar,e14 nvarchar(4096),e15 varchar(1023),e16 nvarchar(200),e17 nchar(255),e18 char(200),e19 varbytes,e20 varbytes(60),e21 varchar,e22 nvarchar) tags (code1 int2 not null,code2 int,code3 int8,code4 float4,code5 float8,code6 bool,code7 varchar,code8 varchar(128) not null,code9 varbytes,code10 varbytes(60),code11 varchar,code12 varchar(60),code13 char(2),code14 char(1023) not null,code15 nchar,code16 nchar(254) not null) primary tags (code1) activetime 2d partition interval 1d;
set cluster setting ts.parallel_degree = 8;
set statement_timeout='10s';

WITH
jennifer_0 AS (select
ref_2.id as c0
from
test_vacuum.t1 as ref_0
inner join (select
ref_1.e8 as c0,
ref_1.code3 as c1
from
test_vacuum.t1 as ref_1
where true
limit 128) as subq_0
inner join test_vacuum.t1 as ref_2
on (subq_0.c1 = ref_2.e3 )
on (ref_0.code9 is NULL)
where ((pg_catalog.timezone(cast(null as text), ref_0.e7) >= pg_catalog.current_timestamp())
or (EXISTS (
select
subq_0.c0 as c0,
ref_0.code2 as c1,
ref_2.e13 as c2,
ref_3.e10 as c3
from
test_vacuum.t1 as ref_3
left join test_vacuum.t1 as ref_4
on (EXISTS (
select
ref_5.code5 as c0
from
test_vacuum.t1 as ref_5
where ((false)
or (cast(null as _uuid) < cast(null as _uuid)))
and (ref_0.e22 is NULL)))
where cast(null as date) IS NOT DISTINCT FROM cast(null as "timestamp")
limit 147)))
and (((EXISTS (
select
ref_2.e11 as c0,
ref_6.e9 as c1,
ref_6.id as c2,
ref_0.e22 as c3,
ref_2.e18 as c4,
ref_2.e21 as c5,
(select e8 from test_vacuum.t1 limit 1 offset 5)
as c6
from
test_vacuum.t1 as ref_6
right join test_vacuum.t1 as ref_7
on (cast(null as "numeric") < cast(null as "numeric"))
where (false)
or (EXISTS (
select
71 as c0,
ref_2.e13 as c1
from
test_vacuum.t1 as ref_8
where cast(null as date) > cast(null as "timestamp")
limit 18))
limit 25))
or (false))
or (cast(null as _int8) != pg_catalog.array_positions(cast(nullif(cast(null as _float8),
cast(null as _float8)) as _float8), ref_0.e5)))
limit 130)
select
subq_1.c0 as c0,
cast(coalesce(subq_1.c3,
subq_2.c5) as int4) as c1,
(select code9 from test_vacuum.t1 limit 1 offset 4)
as c2,
subq_1.c2 as c3,
79 as c4,
subq_1.c0 as c5,
subq_1.c1 as c6,
subq_1.c2 as c7,
subq_1.c2 as c8
from
(select
ref_11.c0 as c0,
ref_11.c0 as c1,
ref_12.c0 as c2,
30 as c3
from
jennifer_0 as ref_9
inner join test_vacuum.t1 as ref_10
inner join jennifer_0 as ref_11
on (ref_10.code2 = ref_11.c0 )
inner join jennifer_0 as ref_12
on (ref_12.c0 is not NULL)
on (ref_10.code5 != ref_10.e5)
where ref_12.c0 is not NULL
limit 127) as subq_1
inner join (select
ref_13.id as c0,
ref_13.code16 as c1,
ref_13.e22 as c2,
ref_13.id as c3,
ref_13.code2 as c4,
10 as c5,
ref_13.e11 as c6,
ref_13.code9 as c7,
ref_13.e11 as c8,
ref_13.code3 as c9,
ref_13.e8 as c10,
ref_13.e8 as c11,
ref_13.e4 as c12,
ref_13.e2 as c13,
ref_13.e18 as c14,
ref_13.e1 as c15,
1 as c16,
ref_13.code10 as c17,
ref_13.e3 as c18,
ref_13.e8 as c19,
ref_13.code7 as c20
from
test_vacuum.t1 as ref_13
where EXISTS (
select
ref_14.c0 as c0
from
jennifer_0 as ref_14
where ref_14.c0 is NULL)
limit 118) as subq_2
on (subq_1.c2 is not NULL)
where EXISTS (
select
subq_1.c0 as c0,
subq_4.c1 as c1,
case when false then subq_2.c3 else subq_2.c3 end
as c2,
subq_1.c3 as c3,
(select e1 from test_vacuum.t1 limit 1 offset 3)
as c4,
subq_4.c1 as c5,
ref_18.e18 as c6,
ref_17.e17 as c7,
subq_2.c3 as c8,
ref_18.e8 as c9,
subq_4.c2 as c10,
subq_1.c2 as c11,
ref_18.code11 as c12,
subq_4.c0 as c13
from
jennifer_0 as ref_15
left join test_vacuum.t1 as ref_16
on (false)
right join test_vacuum.t1 as ref_17
on (cast(null as "interval") != cast(null as "interval"))
inner join test_vacuum.t1 as ref_18
on (true)
inner join (select distinct
subq_2.c2 as c0,
subq_1.c1 as c1,
subq_2.c14 as c2
from
test_vacuum.t1 as ref_19,
lateral (select
ref_19.e9 as c0,
ref_19.e21 as c1,
ref_19.k_timestamp as c2,
ref_20.code8 as c3,
38 as c4,
subq_1.c0 as c5,
subq_1.c1 as c6,
subq_2.c15 as c7,
ref_19.e17 as c8,
subq_1.c3 as c9,
76 as c10,
ref_19.k_timestamp as c11,
ref_20.e4 as c12,
subq_2.c10 as c13,
72 as c14
from
test_vacuum.t1 as ref_20
where true
limit 181) as subq_3
where cast(null as uuid) < cast(null as uuid)
limit 108) as subq_4
on (ref_17.e18 = subq_4.c2 )
where (((cast(null as uuid) <= cast(null as uuid))
or (cast(null as _uuid) IS NOT DISTINCT FROM cast(null as _uuid)))
or (cast(null as "timetz") < cast(null as "timetz")))
or ((cast(null as text) !~ cast(null as text))
and (cast(null as "varbit") = cast(null as "varbit"))))
limit 86
;

-- ERROR: min_extend(): function reserved for internal use
select
  subq_0.c1 as c0,
  subq_0.c4 as c1,
  case when pg_catalog.localtimestamp() > pg_catalog.experimental_follower_read_timestamp() then subq_0.c0 else subq_0.c0 end
     as c2,
  subq_0.c4 as c3,
  case when pg_catalog.inet_server_addr() > pg_catalog.inet_server_addr() then subq_0.c2 else subq_0.c2 end
     as c4,
  cast(coalesce(subq_0.c0,
    pg_catalog.bpcharin(
        pg_catalog.min_extend((select e6 from test_vacuum.t1 limit 1 offset 4)
            , cast(null as int8)) over (partition by subq_0.c3,subq_0.c0 order by subq_0.c1))) as bpchar) as c5,
  case when true then case when subq_0.c1 is NULL then subq_0.c2 else subq_0.c2 end
       else case when subq_0.c1 is NULL then subq_0.c2 else subq_0.c2 end
       end
     as c6,
  54 as c7,
  subq_0.c3 as c8,
  subq_0.c2 as c9,
  subq_0.c1 as c10
from
  (select
        ref_2.e11 as c0,
        ref_2.e10 as c1,
        ref_2.e1 as c2,
        ref_3.e19 as c3,
        ref_4.code7 as c4
      from
        test_vacuum.t1 as ref_0
          inner join test_vacuum.t1 as ref_1
                right join test_vacuum.t1 as ref_2
                on (46 is not NULL)
              right join test_vacuum.t1 as ref_3
              on ((ref_1.code5 <= ref_3.e3)
                  or (true))
            inner join test_vacuum.t1 as ref_4
            on (cast(null as "timetz") <= cast(null as "timetz"))
          on ((cast(null as _date) = cast(null as _date))
              and ((cast(null as text) !~ cast(null as text))
                or ((false)
                  and (ref_3.e9 is NULL))))
      where (cast(null as "interval") >= cast(null as "interval"))
        or (false)
      limit 99) as subq_0
where pg_catalog.kwdb_internal.cluster_id() != pg_catalog.kwdb_internal.cluster_id()
limit 95;

drop database test_vacuum cascade;

create ts database db_shig;
use db_shig;

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
	cnc_number INT4 ) PRIMARY TAGS(machine_code)
	retentions 0s
	activetime 3h
	partition interval 10d;

CREATE TABLE up_exg_msg_real_location (
	gtime TIMESTAMPTZ NOT NULL,
	data VARCHAR(255) NULL,
	data_len INT4 NULL,
	data_type INT4 NULL,
	ban_on_driving_warning INT4 NULL,
	camera_error INT4 NULL,
	collision_rollover INT4 NULL,
	cumulative_driving_timeout INT4 NULL,
	driver_fatigue_monitor INT4 NULL,
	early_warning INT4 NULL,
	emergency_alarm INT4 NULL,
	fatigue_driving INT4 NULL,
	gnss_antenna_disconnect INT4 NULL,
	gnss_antenna_short_circuit INT4 NULL,
	gnss_module_error INT4 NULL,
	ic_module_error INT4 NULL,
	illegal_ignition INT4 NULL,
	illegal_move INT4 NULL,
	in_out_area INT4 NULL,
	in_out_route INT4 NULL,
	lane_departure_error INT4 NULL,
	oil_error INT4 NULL,
	over_speed INT4 NULL,
	overspeed_warning INT4 NULL,
	road_driving_timeout INT4 NULL,
	rollover_warning INT4 NULL,
	stolen INT4 NULL,
	stop_timeout INT4 NULL,
	terminal_lcd_error INT4 NULL,
	terminal_main_power_failure INT4 NULL,
	terminal_main_power_under_v INT4 NULL,
	tts_module_error INT4 NULL,
	vss_error INT4 NULL,
	altitude INT4 NULL,
	date_time VARCHAR(32) NULL,
	direction INT4 NULL,
	encrypy INT4 NULL,
	lat FLOAT8 NULL,
	lon FLOAT8 NULL,
	acc INT4 NULL,
	door INT4 NULL,
	electric_circuit INT4 NULL,
	forward_collision_warning INT4 NULL,
	lane_departure_warning INT4 NULL,
	lat_state INT4 NULL,
	lat_lon_encryption INT4 NULL,
	load_rating INT4 NULL,
	location INT4 NULL,
	lon_state INT4 NULL,
	oil_path INT4 NULL,
	operation INT4 NULL,
	vec1 INT4 NULL,
	vec2 INT4 NULL,
	vec3 INT4 NULL,
	src_type INT4 NULL
) TAGS (
	vehicle_color INT4,
	vehicle_no VARCHAR(32) NOT NULL ) PRIMARY TAGS(vehicle_no)
	retentions 0s
	activetime 1d
	partition interval 10d;

create database tpcc;
use tpcc;
CREATE TABLE bmsql_warehouse (
        w_id INT4 NOT NULL,
        w_ytd DECIMAL(12,2) NULL,
        w_tax DECIMAL(4,4) NULL,
        w_name VARCHAR(10) NULL,
        w_street_1 VARCHAR(20) NULL,
        w_street_2 VARCHAR(20) NULL,
        w_city VARCHAR(20) NULL,
        w_state CHAR(2) NULL,
        w_zip CHAR(9) NULL,
        CONSTRAINT "primary" PRIMARY KEY (w_id ASC),
        FAMILY "primary" (w_id, w_ytd, w_tax, w_name, w_street_1, w_street_2, w_city, w_state, w_zip)
);
CREATE TABLE bmsql_stock (
        s_i_id INT4 NOT NULL,
        s_w_id INT4 NOT NULL,
        s_quantity INT4 NULL,
        s_ytd INT4 NULL,
        s_order_cnt INT4 NULL,
        s_remote_cnt INT4 NULL,
        s_data VARCHAR(50) NULL,
        s_dist_01 CHAR(24) NULL,
        s_dist_02 CHAR(24) NULL,
        s_dist_03 CHAR(24) NULL,
        s_dist_04 CHAR(24) NULL,
        s_dist_05 CHAR(24) NULL,
        s_dist_06 CHAR(24) NULL,
        s_dist_07 CHAR(24) NULL,
        s_dist_08 CHAR(24) NULL,
        s_dist_09 CHAR(24) NULL,
        s_dist_10 CHAR(24) NULL,
        CONSTRAINT "primary" PRIMARY KEY (s_w_id ASC, s_i_id ASC),
        INDEX bmsql_stock_idx1 (s_i_id ASC),
        FAMILY "primary" (s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10)
);
CREATE TABLE bmsql_oorder (
        o_id INT4 NOT NULL,
        o_w_id INT4 NOT NULL,
        o_d_id INT4 NOT NULL,
        o_c_id INT4 NULL,
        o_carrier_id INT4 NULL,
        o_ol_cnt INT4 NULL,
        o_all_local INT4 NULL,
        o_entry_d TIMESTAMP NULL,
        CONSTRAINT "primary" PRIMARY KEY (o_w_id ASC, o_d_id ASC, o_id ASC),
        UNIQUE INDEX bmsql_oorder_idx1 (o_w_id ASC, o_d_id ASC, o_carrier_id ASC, o_id ASC),
        INDEX bmsql_oorder_idx2 (o_w_id ASC, o_d_id ASC, o_c_id ASC),
        FAMILY "primary" (o_id, o_w_id, o_d_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
);
CREATE TABLE bmsql_history (
        hist_id INT4 NULL,
        h_c_id INT4 NULL,
        h_c_d_id INT4 NULL,
        h_c_w_id INT4 NULL,
        h_d_id INT4 NULL,
        h_w_id INT4 NULL,
        h_date TIMESTAMP NULL,
        h_amount DECIMAL(6,2) NULL,
        h_data VARCHAR(24) NULL,
        INDEX bmsql_history_idx1 (h_c_w_id ASC, h_c_d_id ASC, h_c_id ASC),
        INDEX bmsql_history_idx2 (h_w_id ASC, h_d_id ASC),
        FAMILY "primary" (hist_id, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data, rowid)
);
CREATE TABLE bmsql_district (
        d_id INT4 NOT NULL,
        d_w_id INT4 NOT NULL,
        d_ytd DECIMAL(12,2) NULL,
        d_tax DECIMAL(4,4) NULL,
        d_next_o_id INT4 NULL,
        d_name VARCHAR(10) NULL,
        d_street_1 VARCHAR(20) NULL,
        d_street_2 VARCHAR(20) NULL,
        d_city VARCHAR(20) NULL,
        d_state CHAR(2) NULL,
        d_zip CHAR(9) NULL,
        CONSTRAINT "primary" PRIMARY KEY (d_w_id ASC, d_id ASC),
        FAMILY "primary" (d_id, d_w_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip)
);

select
subq_2.c0 as c0
from
(select
ref_0.location as c0,
ref_0.cur_a as c1,
ref_0.allrenergy1_b as c2,
100 as c3,
ref_0.allrenergy2 as c4,
ref_0.pallenergy_b as c5,
ref_0.powera_b as c6,
ref_0.cur_c as c7
from
db_shig.t_electmeter as ref_0
where (cast(null as text) ~* cast(null as text))
or ((select vol_a from db_shig.t_electmeter limit 1 offset 6)
IS NOT DISTINCT FROM cast(null as "numeric"))) as subq_0
inner join (select
ref_1.w_street_2 as c0
from
tpcc.bmsql_warehouse as ref_1,
lateral (select
ref_2.s_w_id as c0,
ref_1.w_state as c1,
ref_2.s_dist_09 as c2,
ref_2.s_dist_04 as c3
from
tpcc.bmsql_stock as ref_2
where (cast(null as _inet) < cast(null as _inet))
or (ref_1.w_street_2 is NULL)
limit 70) as subq_1
where subq_1.c3 is not NULL
limit 136) as subq_2
inner join (select
ref_3.s_dist_03 as c0,
ref_4.s_dist_08 as c1,
ref_4.s_dist_03 as c2,
ref_3.s_dist_04 as c3,
ref_3.s_dist_02 as c4,
(select o_all_local from tpcc.bmsql_oorder limit 1 offset 2)
as c5,
ref_4.s_dist_05 as c6,
ref_5.h_amount as c7,
ref_4.s_dist_10 as c8,
ref_3.s_dist_07 as c9
from
tpcc.bmsql_stock as ref_3
right join tpcc.bmsql_stock as ref_4
on (cast(null as "time") != cast(null as "timetz"))
inner join tpcc.bmsql_history as ref_5
on (ref_3.s_order_cnt = ref_5.hist_id )
where (true)
and ((ref_3.s_w_id is not NULL)
or ((EXISTS (
select
ref_4.s_dist_01 as c0,
ref_4.s_dist_04 as c1,
ref_3.s_dist_09 as c2,
ref_3.s_dist_10 as c3,
ref_4.s_data as c4,
ref_3.s_order_cnt as c5,
ref_5.h_c_id as c6,
ref_6.o_carrier_id as c7,
ref_6.o_id as c8,
ref_5.h_date as c9,
ref_5.h_c_id as c10,
ref_4.s_dist_01 as c11,
ref_4.s_w_id as c12,
ref_4.s_dist_09 as c13,
ref_4.s_dist_06 as c14
from
tpcc.bmsql_oorder as ref_6
where (ref_3.s_order_cnt is NULL)
or (ref_6.o_d_id is NULL)))
and ((ref_5.h_date >= ref_5.h_date)
and (cast(null as _oid) <= cast(null as _oid)))))
limit 69) as subq_3
on (cast(null as "timetz") IS NOT DISTINCT FROM cast(null as "timetz"))
on (subq_0.c0 = subq_2.c0 )
where EXISTS (
select
subq_0.c1 as c0
from
(select
ref_7.in_out_route as c0,
subq_2.c0 as c1,
subq_3.c8 as c2,
ref_7.oil_path as c3,
subq_0.c5 as c4,
subq_0.c2 as c5,
subq_0.c4 as c6,
subq_3.c4 as c7
from
db_shig.up_exg_msg_real_location as ref_7
where cast(null as inet) >> cast(null as inet)
limit 46) as subq_4
left join tpcc.bmsql_district as ref_8
inner join tpcc.bmsql_warehouse as ref_9
on (true)
on (ref_8.d_ytd is not NULL)
where subq_3.c4 is not NULL
limit 103)
limit 44;
 
drop database tpcc cascade;
drop database db_shig cascade;

create ts database test_vacuum;
create table test_vacuum.t1 (k_timestamp timestamptz not null,id int not null,e1 int2,e2 int,e3 int8,e4 float4,e5 float8,e6 bool,e7 timestamptz,e8 char(1023),e9 nchar(255),e10 varchar(4096),e11 char,e12 char(255),e13 nchar,e14 nvarchar(4096),e15 varchar(1023),e16 nvarchar(200),e17 nchar(255),e18 char(200),e19 varbytes,e20 varbytes(60),e21 varchar,e22 nvarchar) tags (code1 int2 not null,code2 int,code3 int8,code4 float4,code5 float8,code6 bool,code7 varchar,code8 varchar(128) not null,code9 varbytes,code10 varbytes(60),code11 varchar,code12 varchar(60),code13 char(2),code14 char(1023) not null,code15 nchar,code16 nchar(254) not null) primary tags (code1) activetime 2d partition interval 1d;
set cluster setting ts.parallel_degree = 8;
set statement_timeout='1s';


INSERT INTO test_vacuum.t1 (k_timestamp, id, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21, e22, code1, code2, code3, code4, code5, code6, code7, code8, code9, code10, code11, code12, code13, code14, code15, code16) VALUES ('2024-08-06 01:42:09', 6, 8353, 1742385267, NULL, -660.1786165580095, NULL, True, '2022-05-09 03:41:04.050654', '可是很多帖子地址不会时候朋友全国.对于密码有关自己图片会员语>言两个.
觉得电子如果不断.都是设计下载增加更多在线.提供公司日本游戏控制只有.
回复你的一个女人全国空间.程序包括广告回复.', '学生显示主题比较因为自己.学生最大音乐发现类别客户名称.
是一建设一个数据操作生活专业学习.北京文章活动选择以下一次帖子提高.
人员日期这种可能来自责任.其实喜欢部分报告.语言提供日期简介.
特别现在今年美国因为一个.包括这是合作深圳.联系主题价格正在而且城市.
说明标题如此文件文化历史其实.分析查看自己.介绍软件当前觉得工程相关.
目前系统决定威望系统我们.价格拥有功能政府教育主题介绍.电影一点有限美国个人事情中文以后.
任何品牌广告参加.服务的人部门他的精华一定支持.经验如果运行原因决定大家.', '程序人民的是怎么.在线中心一切当然准备.对于由于以上状态.
这种结果当然方法方法关系.影响更新中国能力搜索对于.简介国内一样搜索而且.
问题增加这种的人.目前设备合作工作.方面搜索首页女人类别进行.资料有关一般决定记者而且项目.
正在详细发展方法软件电子.有限经营全国推荐他的女人当然我的.
这样法律国家手机社区.他的单位的是发表应用技术.
得到的是质量表示很多.控制成功方式根据联系.成功软件阅读决定.
选择喜欢这种客户.部分一定不过.查看这个网站类别.对于这些设计那个.
公司有些的人.怎么运行电话学生简介工具.
参加的是大家还是类型论坛.电脑朋友没有深圳公司直接.
不要控制美国相关.准备中国生活我的.他们然后城市但是方式之间以后.
作者完全喜欢实现评论不断系列.程序准备世界环境.状态大小谢谢.推荐应用部门那些.
一般如此大家出现以后.公司发表一次同时.', 'x', 'BDOXoIYlxMAowZucPCGlJwEnLSBcXJOrmZThRXSEykVFBciymIQIpkoQlFJGgCSzFzWdXppMpiVMHbcUFZuglLWBtLYkwXnAUkkhwdffzXXAeLjInyrDqSbMtCMhbtMQTTjnriACQUyHlYXJFVWWGozuaXJBIhvbuPJYHdgPMSzoNqAdIsdYNRwBgrwRGSCTukPUorjqMsfwasUNSlLuXHTnuQdZsNdSHBLACZVBzywzzkKZRirJWeVtMsBngvP', 'e', '语言发展支持发现东西其实点击.能力以上工程因此一个简介比较.
搜索一下希望全部包括作为开始.他们那些应用本站.
最后可是方式得到有些到了计划中文.学校一定而且公司准备网上有些.个人帮助公司不同要求.', '科技客户很多.欢迎以上图片重要具有美国.
价格这个一起本站大家.进入评论但是评论以后分析.
这是之后方法公司进行地方.规定用户精华基本如何大小情况具有.电话那么不过一个.', '详细操作电影一直只有的话发表.密码这个简介那个.
一起要求参加不要责任部门时间.一次浏览有限怎么本站市场操作联系.浏览相关学校影响.
汽车学习国内免费状态不是大小.必须只要帮助资料.全部所有系列希望女人手机.
大小价格不能美国.新闻介绍留言留言.
资源个人阅读本站目前.这种要求国家电影报告由于.比较所以上海点击两个密码使用.
直接状态生产首页她的.我的组织发表提供法律人民只有.', '成功设备起来来源.怎么发生出现运行有些.
注册地区以及可是电脑不要社区然后.重要成为电话电脑.安全特别以及这是地区.
行业应用电话关于文章生活.有关活动重要资源成功.也是游戏北京.登录下载安全进入发展.
单位关系成为就是地址.推荐积分任何电脑.还有学习以后.
主要不断介绍业务城市中心以上.投资只是来源设计一起发表浏览.您的位置上海帮助.电子目前学习事情.
进行起来只要程序应该电话.全部对于品牌结果.
学习责任时候比较个人科技.这样公司处理项目状态根据.
本站这是任何留言服务.其实不断以后欢迎.', 'xZYgVoetEUrDbDjuFVNBYvRxupODwYQafgIlQWeIyLNOGNCJOhUdfbLGsUMdselmByfhmzKNQorQsoLWghfQHhKCywoKJlitXYiGteCRLPzsRYXwtCUuMlbqMGyQftkYdMRFQzWJWuaEbgLlLoLQNYntTqnDhOfwJbjSRorcBxJyUYkDEEKbWnNnHwelniqYwTKkKZjJ', 'B+f3#OFg2@0DJGwwe9^F', 'TP@3YtZm!L', '标准生活无法的人游戏质量电话产品控制.', '怎么状态选择下载时候以上记者技术可是.', 6, 1759289692, 4684480790311133270, 840.3514610909983, -779077.0773629927, False, '发表也是能够那么关系虽然可是会员直接.', '一个表示成为可是那个企业不是投资发展.', '_$JrWx#YeC0aiM*ee+ux', '91Xv$ogv$$', '今天之间是一来源完成研究要求等级国家.', '更多威望什么有些操作文章文化时候能够.', 'gT', '你们制作规定.可以更多增加规定.更新希望操作威望由于行业.开始只要政府国际.
一切提供为了所有支持功能上海.客户是否发生分析全部.人民他的法律.', 'z', '在线设备方面北京还有开始.联系觉得时间这里显示论坛中国.
数据社会的话都是方式.直接方式程序地区.
主题影响网上一定东西参加音乐.报告发生那个免费软件什么设备.
可能出现市场生产成为网上拥有.具有作品上海最后只要.
表示深圳部分图片.详细来源一次发生的人.
得到来源已经学习会员操作谢谢根据.客户直接程序.
一下特别拥有他们.这些目前表示就是地区美国.
来自一样不会成为生产一点.记者以下可是学生.孩子包括实现地址感觉详细而且.');


use test_vacuum;
WITH
jennifer_0 AS (select
    subq_0.c0 as c0,
    subq_0.c6 as c1,
    cast(nullif(subq_0.c3,
      subq_0.c3) as "nchar") as c2,
    subq_0.c5 as c3,
    subq_0.c0 as c4,
    subq_0.c2 as c5,
    subq_0.c4 as c6,
    (select code9 from test_vacuum.t1 limit 1 offset 56)
       as c7,
    subq_0.c7 as c8,
    subq_0.c0 as c9,
    subq_0.c5 as c10,
    subq_0.c1 as c11,
    subq_0.c5 as c12,
    subq_0.c3 as c13,
    (select code4 from test_vacuum.t1 limit 1 offset 6)
       as c14,
    (select e7 from test_vacuum.t1 limit 1 offset 19)
       as c15,
    subq_0.c0 as c16,
    subq_0.c0 as c17,
    subq_0.c1 as c18,
    subq_0.c1 as c19,
    subq_0.c2 as c20,
    subq_0.c1 as c21,
    subq_0.c3 as c22,
    subq_0.c1 as c23,
    subq_0.c5 as c24,
    (select e4 from test_vacuum.t1 limit 1 offset 6)
       as c25,
    97 as c26,
    subq_0.c4 as c27,
    subq_0.c4 as c28,
    case when pg_catalog.now() < pg_catalog.experimental_follower_read_timestamp() then 47 else 47 end
       as c29,
    33 as c30
  from
    (select
          ref_0.code8 as c0,
          ref_0.e19 as c1,
          ref_0.code13 as c2,
          (select e17 from test_vacuum.t1 limit 1 offset 6)
             as c3,
          ref_0.code6 as c4,
          ref_0.code1 as c5,
          85 as c6,
          ref_0.code13 as c7
        from
          test_vacuum.t1 as ref_0
        where ref_0.e12 is not NULL
        limit 149) as subq_0
  where true
  limit 187)
select
    subq_2.c1 as c0,
    47 as c1,
    subq_2.c0 as c2,
    subq_2.c1 as c3,
    subq_2.c0 as c4
  from
    (select
          subq_1.c0 as c0,
          subq_1.c4 as c1
        from
          (select
                ref_1.e10 as c0,
                ref_1.code11 as c1,
                ref_2.c10 as c2,
                ref_2.c22 as c3,
                ref_1.e7 as c4,
                ref_2.c21 as c5,
                ref_2.c16 as c6,
                ref_2.c20 as c7,
                ref_2.c14 as c8,
                ref_2.c2 as c9
              from
                test_vacuum.t1 as ref_1
                  inner join jennifer_0 as ref_2
                  on (ref_1.code1 = ref_2.c3 )
              where ((ref_2.c26 is NULL)
                  and (ref_1.code3 != (select pg_catalog.variance(e3) from test_vacuum.t1)
                      ))
                and (ref_1.e3 IS DISTINCT FROM cast(null as "numeric"))) as subq_1
        where EXISTS (
          select
              subq_1.c0 as c0,
              ref_3.c4 as c1,
              65 as c2,
              38 as c3,
              subq_1.c7 as c4,
              ref_3.c14 as c5
            from
              jennifer_0 as ref_3
            where (subq_1.c2 is not NULL)
              or (cast(null as "numeric") = cast(null as "numeric"))
            limit 58)) as subq_2
  where pg_catalog.current_time(pg_catalog.inet_client_port()) = pg_catalog.localtime()
  limit 77
;
drop database test_vacuum cascade;
