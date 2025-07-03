drop database if exists db_shig cascade;
create ts database db_shig;
use db_shig;

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
    number_of_molds INT4
) PRIMARY TAGS (
    machine_code,
    op_group
);

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
    vehicle_no VARCHAR(32) NOT NULL ) PRIMARY TAGS(vehicle_no);

select
    subq_3.c1 as c0,
    subq_3.c0 as c1,
    subq_3.c1 as c2,
    cast(nullif(subq_3.c1,
                subq_3.c1) as "varchar") as c3,
    subq_3.c0 as c4,
    subq_3.c1 as c5,
    subq_3.c1 as c6,
    subq_3.c1 as c7,
    (select allrenergy2_a from public.t_electmeter limit 1 offset 4)
     as c8,
  subq_3.c1 as c9,
  subq_3.c1 as c10,
  (select allrenergy2_a from public.t_electmeter limit 1 offset 6)
     as c11,
  subq_3.c0 as c12
from
    (select
    subq_2.c0 as c0,
    subq_2.c3 as c1
    from
    (select
    subq_1.c4 as c0,
    ref_0.cnc_sw_sver as c1,
    ref_0.feed_set_speed as c2,
    ref_0.cur_tool_z_len_val as c3,
    ref_0.sp_act_speed as c4
    from
    public.t_cnc as ref_0,
    lateral (select
    ref_1.vec2 as c0,
    subq_0.c6 as c1,
    (select allrenergy2_c from public.t_electmeter limit 1 offset 4)
    as c2,
    (select cur_a from public.t_electmeter limit 1 offset 3)
    as c3,
    subq_0.c1 as c4,
    ref_1.acc as c5
    from
    public.up_exg_msg_real_location as ref_1,
    lateral (select
    ref_0.parts_count as c0,
    ref_2.brand as c1,
    ref_1.in_out_route as c2,
    ref_2.cur_tool_len_val as c3,
    ref_1.emergency_alarm as c4,
    ref_1.tts_module_error as c5,
    ref_0.value3 as c6
    from
    public.t_cnc as ref_2
    where cast(null as text) < cast(null as text)
    limit 87) as subq_0
    where cast(null as _oid) > cast(null as _oid)
    limit 101) as subq_1
    where false) as subq_2
    where pg_catalog.current_timestamp() < (select k_timestamp from public.t_cnc limit 1 offset 6)

    limit 73) as subq_3
where pg_catalog.array_prepend(
    cast(cast(coalesce(case when subq_3.c1 is not NULL then cast(null as "varbit") else cast(null as "varbit") end
    ,
    pg_catalog.leftbit(
    cast((select pg_catalog.count(gtime) from public.up_exg_msg_real_location)
    as int8),
    cast(case when cast(null as int8) <= cast(null as int8) then cast(null as int8) else cast(null as int8) end
    as int8))) as "varbit") as "varbit"),
    cast(cast(null as _varbit) as _varbit)) IS NOT DISTINCT FROM cast(null as _varbit);

insert into t_cnc values('2024-11-25 12:00:00','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a',1.2,'a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a',1,'a','a','a','a','a','a','','',1);

select ts from (select k_timestamp as ts from db_shig.t_cnc where false) where now() < '2000-01-01' and (select count(*) from db_shig.t_cnc) = 3;

select count(*) from t_cnc where pg_get_function_result(1) is not null;

select avg(device_state), pg_get_function_result(1) is not null from t_cnc;

select sum(device_state), pg_get_function_result(1) is not null from t_cnc;

select last(device_state), pg_get_function_result(1) is not null from t_cnc;

select last(device_state,'2022-10-1 10:00:00.123'), pg_get_function_result(1) is not null from t_cnc;

drop database if exists db_shig cascade;
