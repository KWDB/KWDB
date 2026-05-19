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

SELECT 
  time_bucket(bucket, '1h') AS hour,
  avg(avg_temp) AS hourly_of_5min_avg
FROM ( 
    SELECT 
      time_bucket(k_timestamp, '5m') AS bucket, 
      avg(temperature) AS avg_temp 
    FROM 
      test_dist2.tab 
    WHERE 
      k_timestamp >= '2026-04-20T16:00:00Z' AND k_timestamp < '2026-04-20T17:00:00Z' 
    GROUP BY bucket
) sub
GROUP BY hour
ORDER BY hour;

drop database test_dist2 cascade;