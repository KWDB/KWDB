set hash_scan_mode=1;
SELECT li.pipeline_name,
       dp.adr,
       time_bucket(t.k_timestamp, '5m') AS timebucket,
       COUNT(t.measure_value) AS total_records,
       SUM(t.measure_value) AS total_value,
       AVG(t.measure_value) AS avg_value
FROM db_pipec.t_point t
JOIN pipec_r.pipeline_info li ON t.pipeline_sn = li.pipeline_sn
JOIN runba_tra.cd_device_point dp ON cast(dp.index_id as int2) = t.measure_type
WHERE t.k_timestamp BETWEEN '2024-01-04 14:29:50' AND '2024-01-04 14:30:10'
GROUP BY li.pipeline_name, dp.adr, timebucket
ORDER BY li.pipeline_name, timebucket;