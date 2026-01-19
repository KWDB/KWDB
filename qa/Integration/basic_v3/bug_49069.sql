create ts database test;

CREATE TABLE test.t_data(ts timestamp(6) NOT NULL, value VARCHAR(16)) TAGS (device VARCHAR(16) not NULL, item VARCHAR(16) not NULL,tag1 int, tag2 int) PRIMARY TAGS(device, item);

explain SELECT last(value) AS value, device, item FROM test.t_data WHERE device = 'Device01' GROUP BY device, item;

explain SELECT last(value) AS value, device, item FROM test.t_data WHERE device = 'Device01' and tag1=11 GROUP BY device, item;

CREATE TABLE test.t_cnc (
 k_timestamp TIMESTAMPTZ(3) NOT NULL,
 cnc_sn VARCHAR(200) NULL,
 cnc_sw_mver CHAR(30) NULL,
 cnc_sw_sver CHAR(30) NULL,
 cnc_tol_mem CHAR(10) NULL,
 cnc_use_mem CHAR(10) NULL,
 cnc_unuse_mem CHAR(10) NULL,
 cnc_status CHAR(2) NULL,
 path_quantity CHAR(30) NULL,
 axis_quantity CHAR(30) NULL,
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
 sp_set_speed CHAR(30) NULL,
 sp_act_speed CHAR(30) NULL,
 sp_load VARCHAR(300) NULL,
 feed_set_speed CHAR(30) NULL,
 feed_act_speed CHAR(30) NULL,
 feed_override CHAR(30) NULL,
 servo_load VARCHAR(300) NULL,
 parts_count CHAR(30) NULL,
 cnc_cycletime CHAR(30) NULL,
 cnc_alivetime CHAR(30) NULL,
 cnc_cuttime CHAR(30) NULL,
 cnc_runtime CHAR(30) NULL,
 mprog_name VARCHAR(500) NULL,
 mprog_num CHAR(30) NULL,
 sprog_name VARCHAR(500) NULL,
 sprog_num CHAR(30) NULL,
 prog_seq_num CHAR(30) NULL,
 prog_seq_content VARCHAR(1000) NULL,
 alarm_count CHAR(10) NULL,
 alarm_type VARCHAR(100) NULL,
 alarm_code VARCHAR(100) NULL,
 alarm_content VARCHAR(2000) NULL,
 alarm_time VARCHAR(200) NULL,
 cur_tool_num CHAR(20) NULL,
 cur_tool_len_num CHAR(20) NULL,
 cur_tool_len CHAR(20) NULL,
 cur_tool_len_val CHAR(20) NULL,
 cur_tool_x_len CHAR(20) NULL,
 cur_tool_x_len_val CHAR(20) NULL,
 cur_tool_y_len CHAR(20) NULL,
 cur_tool_y_len_val CHAR(20) NULL,
 cur_tool_z_len CHAR(20) NULL,
 cur_tool_z_len_val CHAR(20) NULL,
 cur_tool_rad_num CHAR(20) NULL,
 cur_tool_rad CHAR(20) NULL,
 cur_tool_rad_val CHAR(20) NULL,
 device_state INT4 NULL,
 value1 CHAR(10) NULL,
 value2 CHAR(10) NULL,
 value3 CHAR(10) NULL,
 value4 CHAR(10) NULL,
 value5 CHAR(10) NULL
) TAGS (
 machine_code VARCHAR(64) NOT NULL,
 op_group VARCHAR(64) NOT NULL,
 sub_company_id INT2 NOT NULL,
 factory_id INT2 NOT NULL,
 workshop_id INT2 NOT NULL,
 number_of_molds INT4 ) PRIMARY TAGS(machine_code, op_group)
 retentions 5d
 activetime 1h;
EXPLAIN SELECT op_group, count(*) FROM test.t_cnc WHERE machine_code LIKE '0_%' GROUP BY op_group;

drop database test cascade;