USE defaultdb;
DROP DATABASE IF EXISTS fuzz_ts_db cascade;
CREATE TS DATABASE IF NOT EXISTS fuzz_ts_db
RETENTIONS 30d
PARTITION INTERVAL 7d;

CREATE TABLE fuzz_ts_db.sensor_data (
                                        k_timestamp TIMESTAMPTZ NOT NULL,
                                        temperature FLOAT8,
                                        humidity FLOAT8,
                                        count_value INT8,
                                        healthy BOOL,
                                        note_text NVARCHAR(64)
) TAGS (
sensor_id INT8 NOT NULL,
location VARCHAR(64)
) PRIMARY TAGS (sensor_id);

INSERT INTO fuzz_ts_db.sensor_data VALUES
                                       ('2026-01-01 08:00:00', 21.5, 40.0, 10, true, 'morning', 1001, 'north'),
                                       ('2026-01-01 09:00:00', 22.0, 41.5, 12, true, 'late_morning', 1001, 'north'),
                                       ('2026-01-01 10:00:00', 23.1, 43.0, 15, true, 'midday', 1001, 'north'),
                                       ('2026-01-01 11:00:00', 24.5, 45.2, 18, false, 'noon', 1001, 'north'),
                                       ('2026-01-01 08:00:00', 19.8, 37.2, 8, true, 'morning', 1002, 'south'),
                                       ('2026-01-01 09:00:00', 20.3, 38.0, 9, true, 'late_morning', 1002, 'south'),
                                       ('2026-01-01 10:00:00', 21.0, 39.5, 11, false, 'midday', 1002, 'south'),
                                       ('2026-01-01 11:00:00', 22.2, 41.0, 13, true, 'noon', 1002, 'south'),
                                       ('2026-01-01 08:00:00', 18.5, 35.5, 5, true, 'morning', 1003, 'east'),
                                       ('2026-01-01 09:00:00', 19.0, 36.2, 6, true, 'late_morning', 1003, 'east'),
                                       ('2026-01-01 10:00:00', 20.1, 38.0, 8, true, 'midday', 1003, 'east'),
                                       ('2026-01-01 11:00:00', 21.3, 40.1, 10, false, 'noon', 1003, 'east');

SELECT k_timestamp, temperature, humidity, count_value, healthy, note_text
FROM fuzz_ts_db.sensor_data
WHERE sensor_id = 1001 AND k_timestamp = '2026-01-01 09:30:00'
    FILL (PREVIOUS, 600000);

SELECT k_timestamp, temperature, humidity, count_value, healthy, note_text
FROM fuzz_ts_db.sensor_data
WHERE sensor_id = 1001 AND k_timestamp = '2026-01-01 10:30:00'
    FILL (NEXT, 600000);

USE defaultdb;
DROP DATABASE IF EXISTS fuzz_ts_db cascade;