CREATE TS DATABASE IF NOT EXISTS test_dist2;
DROP TABLE IF EXISTS test_dist2.tab;
CREATE TABLE test_dist2.tab(
k_timestamp TIMESTAMPTZ NOT NULL,
id INT NOT NULL,
int16_col INT2,
int32_col INT4,
int64_col INT8,
temperature FLOAT4,
humidity FLOAT8,
bool_col BOOL,
timestamp_col TIMESTAMPTZ,
char_large CHAR(1023),
nchar_large NCHAR(255),
varchar_large VARCHAR(4096),
char_single CHAR,
char_medium CHAR(255),
nchar_single NCHAR,
nvarchar_large NVARCHAR(4096),
varchar_medium VARCHAR(1023),
nvarchar_medium NVARCHAR(200),
nchar_medium NCHAR(255),
char_small CHAR(200),
varbytes_large VARBYTES(254),
varbytes_small VARBYTES(60),
varchar_small VARCHAR(254),
nvarchar_small NVARCHAR(63)
) ATTRIBUTES (
tag_int16 INT2 NOT NULL,
tag_int32 INT4,
tag_int64 INT8,
tag_float32 FLOAT4,
tag_float64 FLOAT8,
tag_bool BOOL,
tag_factory VARCHAR,
tag_workshop VARCHAR(128) NOT NULL,
tag_varbytes VARBYTES,
tag_varbytes_60 VARBYTES(60),
tag_varchar VARCHAR,
tag_varchar_60 VARCHAR(60),
tag_char_2 CHAR(2),
device_id CHAR(1023) NOT NULL,
tag_nchar NCHAR,
tag_device_type NCHAR(254) NOT NULL
) PRIMARY TAGS(tag_int16, device_id, tag_workshop, tag_device_type);
COMMENT ON COLUMN test_dist2.tab.temperature IS '温度';
COMMENT ON COLUMN test_dist2.tab.humidity IS '湿度';
COMMENT ON COLUMN test_dist2.tab.device_id IS 'device_id';
INSERT INTO test_dist2.tab VALUES
('2025-12-31T18:30:00Z', 200, NULL, NULL, NULL, 25, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,100, NULL, NULL, NULL, NULL, NULL, '工厂1', 'workshop_A', NULL, NULL, NULL, NULL, NULL, 'device_ts1', NULL, 'type_temp'),
('2026-04-20T16:00:00Z', 1, 100, 1000, 1000000, 22, 48.5, TRUE, '2026-04-20T16:00:00Z', 'sensor_e8_001', '传感器_001', '温度采集数据_001', 't', 'sensor_e12_001', '中', '设备采集数据_001', 'sensor_e15_001', '温度指标_001', '设备_001_指标', 'sensor_e18_001', '\x0102', '\x0304', 'sensor_e21_001', 'sensor_e22_001',1001, 100, 1000000, 22, 48.5, TRUE, '工厂1', 'workshop_A', '\x0506', '\x0708', 'attr_001', '工厂1_车间A', '01', 'device_001', '中', 'type_temp'),
('2026-04-20T16:01:00Z', 2, 101, 1001, 1000001, 22.200001, 47.8, FALSE, '2026-04-20T16:01:00Z', 'sensor_e8_002', '传感器_002', '湿度采集数据_002', 't', 'sensor_e12_002', '中', '设备采集数据_002', 'sensor_e15_002', '湿度指标_002', '设备_002_指标', 'sensor_e18_002', '\x0102', '\x0304', 'sensor_e21_002', 'sensor_e22_002',1002, 200, 2000000, 22.200001, 47.8, FALSE, '工厂2', 'workshop_B', '\x0506', '\x0708', 'attr_002', '工厂2_车间B', '01', 'device_002', '中', 'type_humid'),
('2026-04-20T16:02:00Z', 3, 102, 1002, 1000002, 22.5, 47.2, TRUE, '2026-04-20T16:02:00Z', 'sensor_e8_003', '传感器_003', '压力采集数据_003', 't', 'sensor_e12_003', '中', '设备采集数据_003', 'sensor_e15_003', '压力指标_003', '设备_003_指标', 'sensor_e18_003', '\x0102', '\x0304', 'sensor_e21_003', 'sensor_e22_003',1003, 300, 3000000, 22.5, 47.2, TRUE, '工厂3', 'workshop_C', '\x0506', '\x0708', 'attr_003', '工厂3_车间C', '01', 'device_003', '中', 'type_pressure'),
('2026-04-20T16:03:00Z', 4, 103, 1003, 1000003, 22.700001, 46.5, FALSE, '2026-04-20T16:03:00Z', 'sensor_e8_004', '传感器_004', '温度采集数据_004', 't', 'sensor_e12_004', '中', '设备采集数据_004', 'sensor_e15_004', '温度指标_004', '设备_004_指标', 'sensor_e18_004', '\x0102', '\x0304', 'sensor_e21_004', 'sensor_e22_004',1001, 400, 4000000, 22.700001, 46.5, FALSE, '工厂1', 'workshop_A', '\x0506', '\x0708', 'attr_004', '工厂1_车间A', '01', 'device_004', '中', 'type_temp'),
('2026-04-20T16:04:00Z', 5, 104, 1004, 1000004, 22.9, 45.9, TRUE, '2026-04-20T16:04:00Z', 'sensor_e8_005', '传感器_005', '湿度采集数据_005', 't', 'sensor_e12_005', '中', '设备采集数据_005', 'sensor_e15_005', '湿度指标_005', '设备_005_指标', 'sensor_e18_005', '\x0102', '\x0304', 'sensor_e21_005', 'sensor_e22_005',1002, 500, 5000000, 22.9, 45.9, TRUE, '工厂2', 'workshop_B', '\x0506', '\x0708', 'attr_005', '工厂2_车间B', '01', 'device_005', '中', 'type_humid'),
('2026-04-20T16:05:00Z', 6, 105, 1005, 1000005, 23.1, 45.3, FALSE, '2026-04-20T16:05:00Z', 'sensor_e8_006', '传感器_006', '压力采集数据_006', 't', 'sensor_e12_006', '中', '设备采集数据_006', 'sensor_e15_006', '压力指标_006', '设备_006_指标', 'sensor_e18_006', '\x0102', '\x0304', 'sensor_e21_006', 'sensor_e22_006',1003, 600, 6000000, 23.1, 45.3, FALSE, '工厂3', 'workshop_C', '\x0506', '\x0708', 'attr_006', '工厂3_车间C', '01', 'device_006', '中', 'type_pressure'),
('2026-04-20T16:06:00Z', 7, 106, 1006, 1000006, 23.299999, 44.7, TRUE, '2026-04-20T16:06:00Z', 'sensor_e8_007', '传感器_007', '温度采集数据_007', 't', 'sensor_e12_007', '中', '设备采集数据_007', 'sensor_e15_007', '温度指标_007', '设备_007_指标', 'sensor_e18_007', '\x0102', '\x0304', 'sensor_e21_007', 'sensor_e22_007',1001, 700, 7000000, 23.299999, 44.7, TRUE, '工厂1', 'workshop_A', '\x0506', '\x0708', 'attr_007', '工厂1_车间A', '01', 'device_007', '中', 'type_temp'),
('2026-04-20T16:07:00Z', 8, 107, 1007, 1000007, 23.5, 44.1, FALSE, '2026-04-20T16:07:00Z', 'sensor_e8_008', '传感器_008', '湿度采集数据_008', 't', 'sensor_e12_008', '中', '设备采集数据_008', 'sensor_e15_008', '湿度指标_008', '设备_008_指标', 'sensor_e18_008', '\x0102', '\x0304', 'sensor_e21_008', 'sensor_e22_008',1002, 800, 8000000, 23.5, 44.1, FALSE, '工厂2', 'workshop_B', '\x0506', '\x0708', 'attr_008', '工厂2_车间B', '01', 'device_008', '中', 'type_humid'),
('2026-04-20T16:08:00Z', 9, 108, 1008, 1000008, 23.700001, 43.5, TRUE, '2026-04-20T16:08:00Z', 'sensor_e8_009', '传感器_009', '压力采集数据_009', 't', 'sensor_e12_009', '中', '设备采集数据_009', 'sensor_e15_009', '压力指标_009', '设备_009_指标', 'sensor_e18_009', '\x0102', '\x0304', 'sensor_e21_009', 'sensor_e22_009',1003, 900, 9000000, 23.700001, 43.5, TRUE, '工厂3', 'workshop_C', '\x0506', '\x0708', 'attr_009', '工厂3_车间C', '01', 'device_009', '中', 'type_pressure'),
('2026-04-20T16:09:00Z', 10, 109, 1009, 1000009, 23.9, 42.9, FALSE, '2026-04-20T16:09:00Z', 'sensor_e8_010', '传感器_010', '温度采集数据_010', 't', 'sensor_e12_010', '中', '设备采集数据_010', 'sensor_e15_010', '温度指标_010', '设备_010_指标', 'sensor_e18_010', '\x0102', '\x0304', 'sensor_e21_010', 'sensor_e22_010',1001, 1000, 10000000, 23.9, 42.9, FALSE, '工厂1', 'workshop_A', '\x0506', '\x0708', 'attr_010', '工厂1_车间A', '01', 'device_010', '中', 'type_temp'),
('2026-04-20T16:10:00Z', 1, 110, 1010, 10000010, 24.1, 42.3, TRUE, '2026-04-20T16:10:00Z', 'sensor_e8_011', '传感器_011', '湿度采集数据_011', 't', 'sensor_e12_011', '中', '设备采集数据_011', 'sensor_e15_011', '湿度指标_011', '设备_011_指标', 'sensor_e18_011', '\x0102', '\x0304', 'sensor_e21_011', 'sensor_e22_011',1002, 1100, 11000000, 24.1, 42.3, TRUE, '工厂2', 'workshop_B', '\x0506', '\x0708', 'attr_011', '工厂2_车间B', '01', 'device_001', '中', 'type_humid'),
('2026-04-20T16:11:00Z', 2, 111, 1011, 10000011, 24.299999, 41.7, FALSE, '2026-04-20T16:11:00Z', 'sensor_e8_012', '传感器_012', '压力采集数据_012', 't', 'sensor_e12_012', '中', '设备采集数据_012', 'sensor_e15_012', '压力指标_012', '设备_012_指标', 'sensor_e18_012', '\x0102', '\x0304', 'sensor_e21_012', 'sensor_e22_012',1003, 1200, 12000000, 24.299999, 41.7, FALSE, '工厂3', 'workshop_C', '\x0506', '\x0708', 'attr_012', '工厂3_车间C', '01', 'device_002', '中', 'type_pressure'),
('2026-04-20T16:12:00Z', 3, 112, 1012, 10000012, 24.5, 41.1, TRUE, '2026-04-20T16:12:00Z', 'sensor_e8_013', '传感器_013', '温度采集数据_013', 't', 'sensor_e12_013', '中', '设备采集数据_013', 'sensor_e15_013', '温度指标_013', '设备_013_指标', 'sensor_e18_013', '\x0102', '\x0304', 'sensor_e21_013', 'sensor_e22_013',1001, 1300, 13000000, 24.5, 41.1, TRUE, '工厂1', 'workshop_A', '\x0506', '\x0708', 'attr_013', '工厂1_车间A', '01', 'device_003', '中', 'type_temp'),
('2026-04-20T16:13:00Z', 4, 113, 1013, 10000013, 24.700001, 40.5, FALSE, '2026-04-20T16:13:00Z', 'sensor_e8_014', '传感器_014', '湿度采集数据_014', 't', 'sensor_e12_014', '中', '设备采集数据_014', 'sensor_e15_014', '湿度指标_014', '设备_014_指标', 'sensor_e18_014', '\x0102', '\x0304', 'sensor_e21_014', 'sensor_e22_014',1002, 1400, 14000000, 24.700001, 40.5, FALSE, '工厂2', 'workshop_B', '\x0506', '\x0708', 'attr_014', '工厂2_车间B', '01', 'device_004', '中', 'type_humid'),
('2026-04-20T16:14:00Z', 5, 114, 1014, 10000014, 24.9, 40, TRUE, '2026-04-20T16:14:00Z', 'sensor_e8_015', '传感器_015', '压力采集数据_015', 't', 'sensor_e12_015', '中', '设备采集数据_015', 'sensor_e15_015', '压力指标_015', '设备_015_指标', 'sensor_e18_015', '\x0102', '\x0304', 'sensor_e21_015', 'sensor_e22_015',1003, 1500, 15000000, 24.9, 40, TRUE, '工厂3', 'workshop_C', '\x0506', '\x0708', 'attr_015', '工厂3_车间C', '01', 'device_005', '中', 'type_pressure'),
('2026-04-20T16:15:00Z', 6, 115, 1015, 10000016, 25.1, 40.6, FALSE, '2026-04-20T16:15:00Z', 'sensor_e8_016', '传感器_016', '温度采集数据_016', 't', 'sensor_e12_016', '中', '设备采集数据_016', 'sensor_e15_016', '温度指标_016', '设备_016_指标', 'sensor_e18_016', '\x0102', '\x0304', 'sensor_e21_016', 'sensor_e22_016',1001, 1600, 16000000, 25.1, 40.6, FALSE, '工厂1', 'workshop_A', '\x0506', '\x0708', 'attr_016', '工厂1_车间A', '01', 'device_006', '中', 'type_temp'),
('2026-04-20T16:16:00Z', 7, 116, 1016, 10000017, 25.299999, 41.2, TRUE, '2026-04-20T16:16:00Z', 'sensor_e8_017', '传感器_017', '湿度采集数据_017', 't', 'sensor_e12_017', '中', '设备采集数据_017', 'sensor_e15_017', '湿度指标_017', '设备_017_指标', 'sensor_e18_017', '\x0102', '\x0304', 'sensor_e21_017', 'sensor_e22_017',1002, 1700, 17000000, 25.299999, 41.2, TRUE, '工厂2', 'workshop_B', '\x0506', '\x0708', 'attr_017', '工厂2_车间B', '01', 'device_007', '中', 'type_humid'),
('2026-04-20T16:17:00Z', 8, 117, 1017, 10000018, 25.5, 41.8, FALSE, '2026-04-20T16:17:00Z', 'sensor_e8_018', '传感器_018', '压力采集数据_018', 't', 'sensor_e12_018', '中', '设备采集数据_018', 'sensor_e15_018', '压力指标_018', '设备_018_指标', 'sensor_e18_018', '\x0102', '\x0304', 'sensor_e21_018', 'sensor_e22_018',1003, 1800, 18000000, 25.5, 41.8, FALSE, '工厂3', 'workshop_C', '\x0506', '\x0708', 'attr_018', '工厂3_车间C', '01', 'device_008', '中', 'type_pressure'),
('2026-04-20T16:18:00Z', 9, 118, 1018, 10000019, 25.700001, 42.4, TRUE, '2026-04-20T16:18:00Z', 'sensor_e8_019', '传感器_019', '温度采集数据_019', 't', 'sensor_e12_019', '中', '设备采集数据_019', 'sensor_e15_019', '温度指标_019', '设备_019_指标', 'sensor_e18_019', '\x0102', '\x0304', 'sensor_e21_019', 'sensor_e22_019',1001, 1900, 19000000, 25.700001, 42.4, TRUE, '工厂1', 'workshop_A', '\x0506', '\x0708', 'attr_019', '工厂1_车间A', '01', 'device_009', '中', 'type_temp'),
('2026-04-20T16:19:00Z', 10, 119, 1019, 10000020, 25.9, 43, FALSE, '2026-04-20T16:19:00Z', 'sensor_e8_020', '传感器_020', '湿度采集数据_020', 't', 'sensor_e12_020', '中', '设备采集数据_020', 'sensor_e15_020', '湿度指标_020', '设备_020_指标', 'sensor_e18_020', '\x0102', '\x0304', 'sensor_e21_020', 'sensor_e22_020',1002, 2000, 20000000, 25.9, 43, FALSE, '工厂2', 'workshop_B', '\x0506', '\x0708', 'attr_020', '工厂2_车间B', '01', 'device_010', '中', 'type_humid'),
('2026-04-20T16:20:00Z', 1, 120, 1020, 10000021, 25.700001, 43.6, TRUE, '2026-04-20T16:20:00Z', 'sensor_e8_021', '传感器_021', '压力采集数据_021', 't', 'sensor_e12_021', '中', '设备采集数据_021', 'sensor_e15_021', '压力指标_021', '设备_021_指标', 'sensor_e18_021', '\x0102', '\x0304', 'sensor_e21_021', 'sensor_e22_021',1003, 2100, 21000000, 25.700001, 43.6, TRUE, '工厂3', 'workshop_C', '\x0506', '\x0708', 'attr_021', '工厂3_车间C', '01', 'device_001', '中', 'type_pressure'),
('2026-04-20T16:21:00Z', 2, 121, 1021, 10000022, 25.5, 44.2, FALSE, '2026-04-20T16:21:00Z', 'sensor_e8_022', '传感器_022', '温度采集数据_022', 't', 'sensor_e12_022', '中', '设备采集数据_022', 'sensor_e15_022', '温度指标_022', '设备_022_指标', 'sensor_e18_022', '\x0102', '\x0304', 'sensor_e21_022', 'sensor_e22_022',1001, 2200, 22000000, 25.5, 44.2, FALSE, '工厂1', 'workshop_A', '\x0506', '\x0708', 'attr_022', '工厂1_车间A', '01', 'device_002', '中', 'type_temp'),
('2026-04-20T16:22:00Z', 3, 122, 1022, 10000023, 25.299999, 44.8, TRUE, '2026-04-20T16:22:00Z', 'sensor_e8_023', '传感器_023', '湿度采集数据_023', 't', 'sensor_e12_023', '中', '设备采集数据_023', 'sensor_e15_023', '湿度指标_023', '设备_023_指标', 'sensor_e18_023', '\x0102', '\x0304', 'sensor_e21_023', 'sensor_e22_023',1002, 2300, 23000000, 25.299999, 44.8, TRUE, '工厂2', 'workshop_B', '\x0506', '\x0708', 'attr_023', '工厂2_车间B', '01', 'device_003', '中', 'type_humid'),
('2026-04-20T16:23:00Z', 4, 123, 1023, 10000024, 25.1, 45.4, FALSE, '2026-04-20T16:23:00Z', 'sensor_e8_024', '传感器_024', '压力采集数据_024', 't', 'sensor_e12_024', '中', '设备采集数据_024', 'sensor_e15_024', '压力指标_024', '设备_024_指标', 'sensor_e18_024', '\x0102', '\x0304', 'sensor_e21_024', 'sensor_e22_024',1003, 2400, 24000000, 25.1, 45.4, FALSE, '工厂3', 'workshop_C', '\x0506', '\x0708', 'attr_024', '工厂3_车间C', '01', 'device_004', '中', 'type_pressure'),
('2026-04-20T16:24:00Z', 5, 124, 1024, 10000025, 24.9, 46, TRUE, '2026-04-20T16:24:00Z', 'sensor_e8_025', '传感器_025', '温度采集数据_025', 't', 'sensor_e12_025', '中', '设备采集数据_025', 'sensor_e15_025', '温度指标_025', '设备_025_指标', 'sensor_e18_025', '\x0102', '\x0304', 'sensor_e21_025', 'sensor_e22_025',1001, 2500, 25000000, 24.9, 46, TRUE, '工厂1', 'workshop_A', '\x0506', '\x0708', 'attr_025', '工厂1_车间A', '01', 'device_005', '中', 'type_temp'),
('2026-04-20T16:25:00Z', 6, 125, 1025, 10000026, 24.700001, 46.6, FALSE, '2026-04-20T16:25:00Z', 'sensor_e8_026', '传感器_026', '湿度采集数据_026', 't', 'sensor_e12_026', '中', '设备采集数据_026', 'sensor_e15_026', '湿度指标_026', '设备_026_指标', 'sensor_e18_026', '\x0102', '\x0304', 'sensor_e21_026', 'sensor_e22_026',1002, 2600, 26000000, 24.700001, 46.6, FALSE, '工厂2', 'workshop_B', '\x0506', '\x0708', 'attr_026', '工厂2_车间B', '01', 'device_006', '中', 'type_humid'),
('2026-04-20T16:26:00Z', 7, 126, 1026, 10000027, 24.5, 47.2, TRUE, '2026-04-20T16:26:00Z', 'sensor_e8_027', '传感器_027', '压力采集数据_027', 't', 'sensor_e12_027', '中', '设备采集数据_027', 'sensor_e15_027', '压力指标_027', '设备_027_指标', 'sensor_e18_027', '\x0102', '\x0304', 'sensor_e21_027', 'sensor_e22_027',1003, 2700, 27000000, 24.5, 47.2, TRUE, '工厂3', 'workshop_C', '\x0506', '\x0708', 'attr_027', '工厂3_车间C', '01', 'device_007', '中', 'type_pressure'),
('2026-04-20T16:27:00Z', 8, 127, 1027, 10000028, 24.299999, 47.8, FALSE, '2026-04-20T16:27:00Z', 'sensor_e8_028', '传感器_028', '温度采集数据_028', 't', 'sensor_e12_028', '中', '设备采集数据_028', 'sensor_e15_028', '温度指标_028', '设备_028_指标', 'sensor_e18_028', '\x0102', '\x0304', 'sensor_e21_028', 'sensor_e22_028',1001, 2800, 28000000, 24.299999, 47.8, FALSE, '工厂1', 'workshop_A', '\x0506', '\x0708', 'attr_028', '工厂1_车间A', '01', 'device_008', '中', 'type_temp'),
('2026-04-20T16:28:00Z', 9, 128, 1028, 10000029, 24.1, 48.4, TRUE, '2026-04-20T16:28:00Z', 'sensor_e8_029', '传感器_029', '湿度采集数据_029', 't', 'sensor_e12_029', '中', '设备采集数据_029', 'sensor_e15_029', '湿度指标_029', '设备_029_指标', 'sensor_e18_029', '\x0102', '\x0304', 'sensor_e21_029', 'sensor_e22_029',1002, 2900, 29000000, 24.1, 48.4, TRUE, '工厂2', 'workshop_B', '\x0506', '\x0708', 'attr_029', '工厂2_车间B', '01', 'device_009', '中', 'type_humid'),
('2026-04-20T16:29:00Z', 10, 129, 1029, 10000030, 24, 49, FALSE, '2026-04-20T16:29:00Z', 'sensor_e8_030', '传感器_030', '压力采集数据_030', 't', 'sensor_e12_030', '中', '设备采集数据_030', 'sensor_e15_030', '压力指标_030', '设备_030_指标', 'sensor_e18_030', '\x0102', '\x0304', 'sensor_e21_030', 'sensor_e22_030',1003, 3000, 30000000, 24, 49, FALSE, '工厂3', 'workshop_C', '\x0506', '\x0708', 'attr_030', '工厂3_车间C', '01', 'device_010', '中', 'type_pressure'),
('2026-05-12T00:00:00Z', 100, NULL, NULL, NULL, 26.5, 50, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,1, NULL, NULL, NULL, NULL, NULL, '工厂1', 'workshop_A', NULL, NULL, NULL, NULL, NULL, 'device_100', NULL, 'type_temp'),
('2026-05-12T02:00:00Z', 300, NULL, NULL, NULL, 1001, 24.366666158040363, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,101, NULL, NULL, NULL, NULL, NULL, '工厂1', 'workshop_A', NULL, NULL, NULL, NULL, NULL, 'device_007', NULL, 'type_temp'),
('2026-05-12T02:00:00Z', 300, NULL, NULL, NULL, 1001, 24.599999745686848, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,101, NULL, NULL, NULL, NULL, NULL, '工厂1', 'workshop_A', NULL, NULL, NULL, NULL, NULL, 'device_010', NULL, 'type_temp'),
('2026-05-12T02:00:00Z', 300, NULL, NULL, NULL, 1003, 24.099999745686848, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,101, NULL, NULL, NULL, NULL, NULL, '工厂3', 'workshop_C', NULL, NULL, NULL, NULL, NULL, 'device_003', NULL, 'type_pressure');
SELECT count(*) AS total_rows FROM test_dist2.tab;
CREATE PROCEDURE test_dist2.p58() BEGIN SELECT time_bucket_gapfill(k_timestamp, '10m') AS bucket, device_id, interpolate(AVG(temperature), PREV) AS avg_temp_filled, COUNT(*) AS actual_records FROM test_dist2.tab WHERE k_timestamp >= '2026-04-20T16:00:00Z' AND k_timestamp < '2026-04-20T17:00:00Z' GROUP BY bucket, device_id ORDER BY bucket, device_id; END;
CALL test_dist2.p58();
drop PROCEDURE IF EXISTS test_dist2.p58;
DROP TABLE IF EXISTS test_dist2.tab;
drop DATABASE test_dist2 cascade;
