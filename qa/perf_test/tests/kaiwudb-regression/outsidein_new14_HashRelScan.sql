set hash_scan_mode=3;
SELECT li.pipeline_name,
       time_bucket(t.k_timestamp, '5m') AS timebucket,
       AVG(t.measure_value) AS avg_value
FROM pipec_r.pipeline_info li
JOIN db_pipec.t_point t ON li.pipeline_sn = t.pipeline_sn
WHERE li.pipeline_name IN ('pipeline_1', 'pipeline_2')
GROUP BY li.pipeline_name, timebucket
ORDER BY li.pipeline_name, timebucket;