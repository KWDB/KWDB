CREATE TS DATABASE db_shig retentions 0s partition interval 10d;
use db_shig;

CREATE TABLE t_electmeter (
                              k_timestamp TIMESTAMPTZ NOT NULL,
                              elect_name CHAR(63) NOT NULL,
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
     workshop_id INT2 NOT NULL,
     cnc_number INT4 ) PRIMARY TAGS(machine_code)
     retentions 5d
     activetime 1h
     partition interval 1d;

prepare my_plan (varchar, varchar) as SELECT machine_code, max(cur_a) as max_cur,min(cur_a) as min_cur,avg(cur_a) as avg_cur, sum(cur_a) as sum_cur, count(cur_a) as count_cur FROM db_shig.t_electmeter where 1=1 and machine_code = $1 and elect_name = $2 and k_timestamp > ('2025-01-01 00:00:00')::timestamptz and k_timestamp < ('2025-01-31 23:59:59')::timestamptz group by machine_code order by machine_code asc;
execute my_plan('17_6', '7608');
prepare my_plan2 (varchar, varchar) as SELECT machine_code, max(cur_a) as max_cur,min(cur_a) as min_cur,avg(cur_a) as avg_cur, sum(cur_a) as sum_cur, count(cur_a) as count_cur FROM db_shig.t_electmeter where 1=1 and machine_code = $1 and elect_name = $2 and k_timestamp > ('2025-01-01 00:00:00')::timestamptz and k_timestamp < ('2025-01-31 23:59:59')::timestamptz group by machine_code order by machine_code asc;
execute my_plan2('17_6', '7608');

drop table t_electmeter;
drop database db_shig;