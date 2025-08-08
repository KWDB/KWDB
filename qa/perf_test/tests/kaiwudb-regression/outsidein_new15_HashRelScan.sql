set hash_scan_mode=3;
SELECT t.measure_type,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value
FROM db_pipec.t_point t
JOIN pipec_r.pipeline_info li ON t.pipeline_sn = li.pipeline_sn
WHERE t.k_timestamp > '2024-01-04 00:05:30'::timestamp - INTERVAL '1 HOUR'
GROUP BY t.measure_type
ORDER BY t.measure_type;