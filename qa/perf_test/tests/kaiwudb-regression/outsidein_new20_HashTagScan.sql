set hash_scan_mode=1;
SELECT time_bucket(t.k_timestamp, '10m') AS timebucket,
       dp.adr,
       AVG(t.measure_value) AS avg_value,
       COUNT(t.measure_value) AS total_count
FROM db_pipec.t_point t
JOIN runba_tra.cd_device_point dp ON cast(dp.index_id as int2) = t.measure_type
WHERE dp.point_name LIKE 'PI%'
  AND t.k_timestamp BETWEEN '2024-01-01' AND '2024-01-04 14:30:50'
GROUP BY timebucket, dp.adr;