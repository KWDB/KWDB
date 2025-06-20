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

drop database test_vacuum cascade;
