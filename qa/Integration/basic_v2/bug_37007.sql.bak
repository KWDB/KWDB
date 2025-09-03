create ts database test;
use test;
CREATE TABLE t_point (
    k_timestamp TIMESTAMPTZ NOT NULL,
    measure_value FLOAT8 NULL
) TAGS (
 point_sn VARCHAR(64) NOT NULL,
 work_area_sn VARCHAR(16),
 sub_com_sn VARCHAR(32),
 station_sn VARCHAR(16),
 pipeline_sn VARCHAR(16),
 measure_type INT2 ) PRIMARY TAGS(point_sn);

explain select t.point_sn as point_sn  from t_point t
where k_timestamp > '2024-05-01 00:00:00'::timestamptz  and k_timestamp < '2024-06-01 00:00:00'::timestamptz  and t.point_sn = 'GZZYQ'
group by t.point_sn  order by count(*) desc  limit 1;

explain select t.point_sn as point_sn  from t_point t
where k_timestamp > '2024-05-01 00:00:00'::timestamptz  and k_timestamp < '2024-06-01 00:00:00'::timestamptz  and t.work_area_sn = 'GZZYQ'
group by t.point_sn  order by count(*) desc  limit 1;

drop database test cascade;