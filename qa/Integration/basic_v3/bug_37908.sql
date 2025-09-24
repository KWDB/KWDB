drop database if exists test_function_2 cascade;
create ts database test_function_2;

create table test_function_2.t1(k_timestamp timestamp not null,e1 timestamp,e2 int2,e3 int4,e4 int8,e5 float4,e6 float8) ATTRIBUTES (code1 INT2 NOT NULL,code2 INT4,code3 INT8,code4 FLOAT4 ,code5 FLOAT8,code6 BOOL,code7 VARCHAR,code8 VARCHAR(128) NOT NULL,code9 VARBYTES,code10 VARBYTES(60),code11 VARCHAR,code12 VARCHAR(60),code13 CHAR(2),code14 CHAR(1023) NOT NULL,code15 NCHAR,code16 NCHAR(254) NOT NULL) PRIMARY TAGS(code1,code14,code8,code16);

insert into test_function_2.t1 values ('2021-04-01 15:00:00',111111110000,1000,1000000,100000000,100000.101,1000000.10101111,-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');

select ceiling(e2), ceiling(e3), ceiling(e4), ceiling(e5), ceiling(e6) from test_function_2.t1 where e2 < 5000 group by e2,e3,e4,e5,e6 order by e2,e3,e4,e5,e6;

select round(e5,1), ceiling(e5) from test_function_2.t1 where e3 > 3000000 group by e5 having e5 < 500000.505 order by e5 desc;

select abs(e2) from test_function_2.t1;

select abs(e2) from test_function_2.t1 group by e2;

select abs(e2) from test_function_2.t1 group by e2 order by e2;

select abs(max(e2)) from test_function_2.t1;

select abs(max(e2)) from test_function_2.t1 group by e2;

select abs(max(e2)) from test_function_2.t1 group by e2 order by e2;

select coalesce(e2, 1) from test_function_2.t1;

explain select coalesce(e2, 1) from test_function_2.t1;

select coalesce(e2, 1, 2) from test_function_2.t1;

explain select coalesce(e2, 1, 2) from test_function_2.t1;

drop database test_function_2 cascade;

USE defaultdb;
DROP DATABASE IF EXISTS test_data_pipe cascade;
CREATE ts DATABASE test_data_pipe;
DROP TABLE IF EXISTS test_data_pipe.t1 CASCADE;
CREATE TABLE test_data_pipe.t1(
                                  k_timestamp TIMESTAMPTZ NOT NULL,
                                  id INT NOT NULL,
                                  e1 INT2,
                                  e2 INT,
                                  e3 INT8,
                                  e4 FLOAT4,
                                  e5 FLOAT8,
                                  e6 BOOL,
                                  e7 TIMESTAMPTZ,
                                  e8 CHAR(1023),
                                  e9 NCHAR(255),
                                  e10 VARCHAR(4096),
                                  e11 CHAR,
                                  e12 CHAR(255),
                                  e13 NCHAR,
                                  e14 NVARCHAR(4096),
                                  e15 VARCHAR(1023),
                                  e16 NVARCHAR(200),
                                  e17 NCHAR(255),
                                  e18 CHAR(200),
                                  e19 VARBYTES,
                                  e20 VARBYTES(60),
                                  e21 VARCHAR,
                                  e22 NVARCHAR)
    ATTRIBUTES (
code1 INT2 NOT NULL,code2 INT,code3 INT8,
code4 FLOAT4 ,code5 FLOAT8,
code6 BOOL,
code7 VARCHAR,code8 VARCHAR(128) NOT NULL,
code9 VARBYTES,code10 VARBYTES(60),
code11 VARCHAR,code12 VARCHAR(60),
code13 CHAR(2),code14 CHAR(1023) NOT NULL,
code15 NCHAR,code16 NCHAR(254) NOT NULL)
PRIMARY TAGS(code1,code14,code8,code16);

INSERT INTO test_data_pipe.t1 VALUES('2024-6-5 00:01:00',31,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,' ' ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,3,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\0\0' ,'test数据库语法查询测试！！！@TEST3-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST3-14','' ,'test数据库语法查询测试！！！@TEST3-16');

SELECT k_timestamp,id,e1,code1 FROM test_data_pipe.t1 WHERE code8 SIMILAR TO 'test数据库语法查询测试！！！@TEST3-8' AND k_timestamp > '-292275055-05-16 16:47:04.192 +0000' ORDER BY k_timestamp LIMIT 100000;

select k_timestamp,id,e1,code1 FROM test_data_pipe.t1 WHERE code8 = 'test数据库语法查询测试！！！@TEST3-8' AND k_timestamp > '-292275055-05-16 16:47:04.192 +0000' ORDER BY k_timestamp LIMIT 100000;

select k_timestamp,id,e1,code1 FROM test_data_pipe.t1 WHERE code8 = 'test 据库语法查询测试！！！@TEST3-8' AND k_timestamp > '-292275055-05-16 16:47:04.192 +0000' ORDER BY k_timestamp LIMIT 100000;

drop database test_data_pipe cascade;

USE defaultdb;
DROP DATABASE IF EXISTS d1 cascade;
CREATE ts DATABASE d1;
use d1;
CREATE TABLE d1.t1(ts TIMESTAMPTZ NOT NULL, e1 INT, e2 int) tags (tag1 INT NOT NULL) PRIMARY TAGS (tag1);
INSERT INTO d1.t1 VALUES('2025-01-01 12:34:56', 1, 2, 10);
select e1 from d1.t1 where ts > '-292275055-05-16 16:47:04.192 +0000';
select e1 from d1.t1 where ts > '-292275055-05-16 16:47:04.192 +0000' and e1 is not null;
explain select e1 from d1.t1 where ts > '-292275055-05-16 16:47:04.192 +0000' and e1 is not null;
select e1 from d1.t1 where ts > '-292275055-05-16 16:47:04.192 +0000' and e1 is not null and e2 = 2;
explain select e1 from d1.t1 where ts > '-292275055-05-16 16:47:04.192 +0000' and e1 is not null and e2 = 2;

drop database d1 cascade;

create database d2;
create table d2.t1 (e1 int);
select e1 from d2.t1 where to_english(e1) = 'one' and (e1 = 2.2);
drop database d2 cascade;


create database tpcc;
CREATE TABLE tpcc.bmsql_order_line (
 ol_w_id INT4 NOT NULL,
 ol_d_id INT4 NOT NULL,
 ol_o_id INT4 NOT NULL,
 ol_number INT4 NOT NULL,
 ol_i_id INT4 NOT NULL,
 ol_delivery_d TIMESTAMP NULL,
 ol_amount DECIMAL(6,2) NULL,
 ol_supply_w_id INT4 NULL,
 ol_quantity INT4 NULL,
 ol_dist_info CHAR(24) NULL,
 CONSTRAINT "primary" PRIMARY KEY (ol_w_id ASC, ol_d_id ASC, ol_o_id ASC, ol_number ASC),
 INDEX bmsql_order_line_idx1 (ol_supply_w_id ASC, ol_i_id ASC),
 FAMILY "primary" (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
);

CREATE TABLE tpcc.bmsql_history (
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

CREATE TABLE tpcc.bmsql_item (
 i_id INT4 NOT NULL,
 i_name VARCHAR(24) NULL,
 i_price DECIMAL(5,2) NULL,
 i_data VARCHAR(50) NULL,
 i_im_id INT4 NULL,
 CONSTRAINT "primary" PRIMARY KEY (i_id ASC),
 FAMILY "primary" (i_id, i_name, i_price, i_data, i_im_id)
);


create ts database db_shig;
CREATE TABLE db_shig.t_cnc (
  k_timestamp TIMESTAMPTZ(3) NOT NULL,
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
) PRIMARY TAGS(machine_code, op_group);

CREATE TABLE db_shig.up_exg_msg_real_location (
 gtime TIMESTAMPTZ(3) NOT NULL,
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

CREATE TABLE db_shig.t_electmeter (
 k_timestamp TIMESTAMPTZ(3) NOT NULL,
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

select
    (select src_type from db_shig.up_exg_msg_real_location limit 1 offset 6)
     as c0,
  subq_2.c5 as c1,
  subq_2.c3 as c2
from
    (select
    ref_0.h_data as c0,
    subq_1.c2 as c1,
    (select i_im_id from tpcc.bmsql_item limit 1 offset 4)
    as c2,
    ref_0.rowid as c3,
    subq_1.c1 as c4,
    subq_1.c2 as c5
    from
    tpcc.bmsql_history as ref_0,
    lateral (select
    subq_0.c0 as c0,
    ref_0.h_date as c1,
    subq_0.c0 as c2
    from
    db_shig.t_electmeter as ref_1
    inner join db_shig.up_exg_msg_real_location as ref_2
    on (((select load_rating from db_shig.up_exg_msg_real_location limit 1 offset 2)
    is NULL)
    and (ref_0.h_date IS DISTINCT FROM (select ol_delivery_d from tpcc.bmsql_order_line limit 1 offset 3)
    )),
    lateral (select
    ref_3.cur_tool_rad_val as c0
    from
    db_shig.t_cnc as ref_3
    where ref_1.k_timestamp <= ref_0.h_date
    limit 107) as subq_0
    where ref_0.h_d_id is NULL) as subq_1
    where case when cast(null as text) >= cast(null as text) then cast(null as _interval) else cast(null as _interval) end
    >= pg_catalog.array_prepend(
    cast(cast(null as "interval") as "interval"),
    cast(cast(null as _interval) as _interval))
    limit 128) as subq_2
where (pg_catalog.to_english(
    cast(subq_2.c3 as int8)) ~ cast(coalesce(pg_catalog.getdatabaseencoding(),
    pg_catalog.kwdb_internal.cluster_name()) as text))
  and (subq_2.c3 = pg_catalog.pi())
    limit 87;
drop database tpcc cascade;
drop database db_shig cascade;


drop database if exists tpcc cascade;
create database tpcc;
use tpcc;
CREATE TABLE bmsql_stock
(
    s_i_id       INT4 NOT NULL,
    s_w_id       INT4 NOT NULL,
    s_quantity   INT4 NULL,
    s_ytd        INT4 NULL,
    s_order_cnt  INT4 NULL,
    s_remote_cnt INT4 NULL,
    s_data       VARCHAR(50) NULL,
    s_dist_01    CHAR(24) NULL,
    s_dist_02    CHAR(24) NULL,
    s_dist_03    CHAR(24) NULL,
    s_dist_04    CHAR(24) NULL,
    s_dist_05    CHAR(24) NULL,
    s_dist_06    CHAR(24) NULL,
    s_dist_07    CHAR(24) NULL,
    s_dist_08    CHAR(24) NULL,
    s_dist_09    CHAR(24) NULL,
    s_dist_10    CHAR(24) NULL,
    CONSTRAINT "primary" PRIMARY KEY (s_w_id ASC, s_i_id ASC),
    INDEX        bmsql_stock_idx1 (s_i_id ASC),
    FAMILY       "primary" (s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10)
);

CREATE TABLE bmsql_item
(
    i_id    INT4 NOT NULL,
    i_name  VARCHAR(24) NULL,
    i_price DECIMAL(5, 2) NULL,
    i_data  VARCHAR(50) NULL,
    i_im_id INT4 NULL,
    CONSTRAINT "primary" PRIMARY KEY (i_id ASC),
    FAMILY  "primary" (i_id, i_name, i_price, i_data, i_im_id)
);

CREATE TABLE bmsql_customer
(
    c_id           INT4 NOT NULL,
    c_d_id         INT4 NOT NULL,
    c_w_id         INT4 NOT NULL,
    c_discount     DECIMAL(4, 4) NULL,
    c_credit       CHAR(2) NULL,
    c_last         VARCHAR(16) NULL,
    c_first        VARCHAR(16) NULL,
    c_credit_lim   DECIMAL(12, 2) NULL,
    c_balance      DECIMAL(12, 2) NULL,
    c_ytd_payment  DECIMAL(12, 2) NULL,
    c_payment_cnt  INT4 NULL,
    c_delivery_cnt INT4 NULL,
    c_street_1     VARCHAR(20) NULL,
    c_street_2     VARCHAR(20) NULL,
    c_city         VARCHAR(20) NULL,
    c_state        CHAR(2) NULL,
    c_zip          CHAR(9) NULL,
    c_phone        CHAR(16) NULL,
    c_since        TIMESTAMP NULL,
    c_middle       CHAR(2) NULL,
    c_data         VARCHAR(500) NULL,
    CONSTRAINT "primary" PRIMARY KEY (c_w_id ASC, c_d_id ASC, c_id ASC),
    INDEX          bmsql_customer_idx1 (c_w_id ASC, c_d_id ASC, c_last ASC, c_first ASC),
    FAMILY         "primary" (c_id, c_d_id, c_w_id, c_discount, c_credit, c_last, c_first, c_credit_lim, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_middle, c_data)
);

CREATE TABLE bmsql_district
(
    d_id        INT4 NOT NULL,
    d_w_id      INT4 NOT NULL,
    d_ytd       DECIMAL(12, 2) NULL,
    d_tax       DECIMAL(4, 4) NULL,
    d_next_o_id INT4 NULL,
    d_name      VARCHAR(10) NULL,
    d_street_1  VARCHAR(20) NULL,
    d_street_2  VARCHAR(20) NULL,
    d_city      VARCHAR(20) NULL,
    d_state     CHAR(2) NULL,
    d_zip       CHAR(9) NULL,
    CONSTRAINT "primary" PRIMARY KEY (d_w_id ASC, d_id ASC),
    FAMILY      "primary" (d_id, d_w_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip)
);

CREATE TABLE bmsql_oorder
(
    o_id         INT4 NOT NULL,
    o_w_id       INT4 NOT NULL,
    o_d_id       INT4 NOT NULL,
    o_c_id       INT4 NULL,
    o_carrier_id INT4 NULL,
    o_ol_cnt     INT4 NULL,
    o_all_local  INT4 NULL,
    o_entry_d    TIMESTAMP NULL,
    CONSTRAINT "primary" PRIMARY KEY (o_w_id ASC, o_d_id ASC, o_id ASC),
    UNIQUE INDEX bmsql_oorder_idx1 (o_w_id ASC, o_d_id ASC, o_carrier_id ASC, o_id ASC),
    INDEX        bmsql_oorder_idx2 (o_w_id ASC, o_d_id ASC, o_c_id ASC),
    FAMILY       "primary" (o_id, o_w_id, o_d_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
);

CREATE TABLE bmsql_history
(
    hist_id  INT4 NULL,
    h_c_id   INT4 NULL,
    h_c_d_id INT4 NULL,
    h_c_w_id INT4 NULL,
    h_d_id   INT4 NULL,
    h_w_id   INT4 NULL,
    h_date   TIMESTAMP NULL,
    h_amount DECIMAL(6, 2) NULL,
    h_data   VARCHAR(24) NULL,
    INDEX    bmsql_history_idx1 (h_c_w_id ASC, h_c_d_id ASC, h_c_id ASC),
    INDEX    bmsql_history_idx2 (h_w_id ASC, h_d_id ASC),
    FAMILY   "primary" (hist_id, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data, rowid)
);

SELECT
    ref_5.h_c_d_id AS c0,
    ref_6.d_ytd AS c1,
    CASE WHEN kwdb_internal.cluster_id() = uuid_recv(ref_4.s_order_cnt)
             THEN ref_4.s_order_cnt
         ELSE ref_4.s_order_cnt
        END AS c2,
    ref_6.d_name AS c3,
    83 AS c4,
    ref_4.s_data AS c5,
    subq_0.c9 AS c6,
    ref_0.s_dist_04 AS c7,
    ref_0.s_dist_09 AS c8,
    ref_6.d_ytd AS c9,
    subq_1.c1 AS c10
FROM
    tpcc.bmsql_stock AS ref_0
        INNER JOIN
    (SELECT
         ref_1.i_id AS c0,
         ref_1.i_price AS c1,
         ref_1.i_name AS c2,
         ref_1.i_im_id AS c3,
         ref_1.i_name AS c4,
         ref_1.i_id AS c5,
         ref_1.i_id AS c6,
         ref_1.i_data AS c7,
         ref_1.i_name AS c8,
         ref_1.i_im_id AS c9,
         ref_1.i_price AS c10,
         ref_1.i_im_id AS c11
     FROM
         tpcc.bmsql_item AS ref_1
     WHERE
         true
         LIMIT 72) AS subq_0
        INNER JOIN
    (SELECT
         ref_2.c_discount AS c0,
         ref_3.i_data AS c1,
         ref_2.c_since AS c2,
         ref_2.c_since AS c3,
         ref_3.i_data AS c4
     FROM
         tpcc.bmsql_customer AS ref_2
             INNER JOIN
         tpcc.bmsql_item AS ref_3
         ON
             (ref_2.c_payment_cnt IS NULL)
     WHERE
         CAST(NULL AS BYTES) IS NOT DISTINCT FROM CAST(NULL AS BYTES)
         LIMIT
         53) AS subq_1
        LEFT JOIN
    tpcc.bmsql_stock AS ref_4
        INNER JOIN
    tpcc.bmsql_history AS ref_5
        RIGHT JOIN
    tpcc.bmsql_district AS ref_6
    ON
        (false >= false)
    ON
        (ref_4.s_i_id = ref_5.hist_id)
    ON
        (subq_1.c4 = ref_5.h_data)
    ON
        (subq_0.c5 = ref_6.d_id)
    ON
        ((CASE WHEN
                   (ref_6.d_tax >= CAST(NULL AS FLOAT8))
                       OR (CAST(NULL AS INTERVAL) = CAST(NULL AS INTERVAL))
                   THEN
                   CAST(NULL AS TIMETZ)
               ELSE
                   CAST(NULL AS TIMETZ)
              END
            IS DISTINCT FROM CAST(NULL AS TIME))
            OR
         (false = CASE WHEN (CAST(NULL AS TIMESTAMPTZ) >= CAST(NULL AS DATE))
             OR
                            (CAST(NULL AS TIMESTAMPTZ) >= CAST(NULL AS TIMESTAMPTZ))
                           THEN false
                       ELSE false
             END)
            ),
    LATERAL (
             SELECT
                 ref_6.d_w_id AS c0
             FROM
                 tpcc.bmsql_oorder AS ref_7
             WHERE
                 (ref_5.h_c_id IS NOT NULL)
               AND
                 (ref_4.s_w_id >= subq_0.c11)
        ) AS subq_2
WHERE
    array_cat(CAST(NULL AS UUID[]),
              array_append(
                      array_remove(array_replace(CAST(NULL AS UUID[]),
                                                 CAST(NULL AS UUID), CAST(NULL AS UUID)),
                                   gen_random_uuid()),
                      uuid_recv(ref_6.d_w_id))
    )
        IS NOT DISTINCT FROM
    array_replace(
    array_replace(array_replace(
    array_replace(CAST(NULL AS UUID[]),
    CAST(NULL AS UUID),
    CAST(COALESCE(CAST(NULL AS UUID),
    CAST(NULL AS UUID)) AS UUID)),
    kwdb_internal.cluster_id(),
    gen_random_uuid()),
    CASE WHEN
    ref_6.d_w_id >= ref_5.h_c_d_id
    THEN uuid_in(subq_1.c0)
    ELSE uuid_in(subq_1.c0)
    END,
    gen_random_uuid()),
    kwdb_internal.cluster_id(),
    gen_random_uuid());

set cluster setting default_transaction_read_only.enabled=true;

SELECT
    ref_5.h_c_d_id AS c0,
    ref_6.d_ytd AS c1,
    CASE WHEN kwdb_internal.cluster_id() = uuid_recv(ref_4.s_order_cnt)
             THEN ref_4.s_order_cnt
         ELSE ref_4.s_order_cnt
        END AS c2,
    ref_6.d_name AS c3,
    83 AS c4,
    ref_4.s_data AS c5,
    subq_0.c9 AS c6,
    ref_0.s_dist_04 AS c7,
    ref_0.s_dist_09 AS c8,
    ref_6.d_ytd AS c9,
    subq_1.c1 AS c10
FROM
    tpcc.bmsql_stock AS ref_0
        INNER JOIN
    (SELECT
         ref_1.i_id AS c0,
         ref_1.i_price AS c1,
         ref_1.i_name AS c2,
         ref_1.i_im_id AS c3,
         ref_1.i_name AS c4,
         ref_1.i_id AS c5,
         ref_1.i_id AS c6,
         ref_1.i_data AS c7,
         ref_1.i_name AS c8,
         ref_1.i_im_id AS c9,
         ref_1.i_price AS c10,
         ref_1.i_im_id AS c11
     FROM
         tpcc.bmsql_item AS ref_1
     WHERE
         true
         LIMIT 72) AS subq_0
        INNER JOIN
    (SELECT
         ref_2.c_discount AS c0,
         ref_3.i_data AS c1,
         ref_2.c_since AS c2,
         ref_2.c_since AS c3,
         ref_3.i_data AS c4
     FROM
         tpcc.bmsql_customer AS ref_2
             INNER JOIN
         tpcc.bmsql_item AS ref_3
         ON
             (ref_2.c_payment_cnt IS NULL)
     WHERE
         CAST(NULL AS BYTES) IS NOT DISTINCT FROM CAST(NULL AS BYTES)
         LIMIT
         53) AS subq_1
        LEFT JOIN
    tpcc.bmsql_stock AS ref_4
        INNER JOIN
    tpcc.bmsql_history AS ref_5
        RIGHT JOIN
    tpcc.bmsql_district AS ref_6
    ON
        (false >= false)
    ON
        (ref_4.s_i_id = ref_5.hist_id)
    ON
        (subq_1.c4 = ref_5.h_data)
    ON
        (subq_0.c5 = ref_6.d_id)
    ON
        ((CASE WHEN
                   (ref_6.d_tax >= CAST(NULL AS FLOAT8))
                       OR (CAST(NULL AS INTERVAL) = CAST(NULL AS INTERVAL))
                   THEN
                   CAST(NULL AS TIMETZ)
               ELSE
                   CAST(NULL AS TIMETZ)
              END
            IS DISTINCT FROM CAST(NULL AS TIME))
            OR
         (false = CASE WHEN (CAST(NULL AS TIMESTAMPTZ) >= CAST(NULL AS DATE))
             OR
                            (CAST(NULL AS TIMESTAMPTZ) >= CAST(NULL AS TIMESTAMPTZ))
                           THEN false
                       ELSE false
             END)
            ),
    LATERAL (
             SELECT
                 ref_6.d_w_id AS c0
             FROM
                 tpcc.bmsql_oorder AS ref_7
             WHERE
                 (ref_5.h_c_id IS NOT NULL)
               AND
                 (ref_4.s_w_id >= subq_0.c11)
        ) AS subq_2
WHERE
    array_cat(CAST(NULL AS UUID[]),
              array_append(
                      array_remove(array_replace(CAST(NULL AS UUID[]),
                                                 CAST(NULL AS UUID), CAST(NULL AS UUID)),
                                   gen_random_uuid()),
                      uuid_recv(ref_6.d_w_id))
    )
        IS NOT DISTINCT FROM
    array_replace(
    array_replace(array_replace(
    array_replace(CAST(NULL AS UUID[]),
    CAST(NULL AS UUID),
    CAST(COALESCE(CAST(NULL AS UUID),
    CAST(NULL AS UUID)) AS UUID)),
    kwdb_internal.cluster_id(),
    gen_random_uuid()),
    CASE WHEN
    ref_6.d_w_id >= ref_5.h_c_d_id
    THEN uuid_in(subq_1.c0)
    ELSE uuid_in(subq_1.c0)
    END,
    gen_random_uuid()),
    kwdb_internal.cluster_id(),
    gen_random_uuid());

set cluster setting default_transaction_read_only.enabled=default;
drop database tpcc cascade;

drop database if exists test_agg_distinct cascade;
create ts database test_agg_distinct;
use test_agg_distinct;
create table t1 (ts timestamp not null, e1 int8, e2 int8) tags (tag1 int not null, tag2 int not null) primary tags (tag1);
insert into t1 values ('2021-04-01 15:00:01', 1, 11, 10, 101);
insert into t1 values ('2021-04-01 15:00:02', 2, 12, 20, 102);
insert into t1 values ('2021-04-01 15:00:03', 3, 13, 30, 103);
insert into t1 values ('2021-04-01 15:00:04', 4, 14, 40, 104);
insert into t1 values ('2021-04-01 15:00:05', 5, 15, 50, 105);
insert into t1 values ('2021-04-01 15:00:06', 1, 11, 60, 101);
insert into t1 values ('2021-04-01 15:00:07', 2, 12, 70, 102);
insert into t1 values ('2021-04-01 15:00:08', 3, 13, 80, 103);
insert into t1 values ('2021-04-01 15:00:09', 4, 14, 90, 104);
insert into t1 values ('2021-04-01 15:00:10', 5, 15, 91, 105);
select count(distinct e1), count(distinct e2) from test_agg_distinct.t1;
select count(distinct tag2) from test_agg_distinct.t1;

select count(distinct e1), count(distinct e2) from test_agg_distinct.t1 group by e1;
select count(distinct tag2) from test_agg_distinct.t1 group by tag2;

select count(e1), count(distinct e2) from test_agg_distinct.t1;
select count(e1), count(distinct e2) from test_agg_distinct.t1 group by e1;

drop database if exists test_agg_distinct cascade;