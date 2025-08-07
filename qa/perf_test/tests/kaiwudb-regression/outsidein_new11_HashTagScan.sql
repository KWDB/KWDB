set hash_scan_mode=1;
SELECT li.pipeline_name,
       t.measure_value
FROM pipec_r.pipeline_info li
JOIN db_pipec.t_point t ON t.measure_location LIKE '%87001%'
WHERE li.pipeline_sn = t.pipeline_sn AND t.k_timestamp > '2024-01-07'::timestamp - INTERVAL '7 DAY';