set hash_scan_mode=3;
SELECT li.pipeline_name,
       dp.adr,
       SUM(t.measure_value) AS total_value
FROM pipec_r.pipeline_info li
JOIN db_pipec.t_point t ON li.pipeline_sn = t.pipeline_sn
JOIN runba_tra.cd_device_point dp ON cast(dp.index_id as int2) = t.measure_type
WHERE t.k_timestamp BETWEEN '2024-01-01' AND '2024-01-05'
  AND t.measure_value BETWEEN 50 AND 51
  AND dp.index_upper_value > 1.5
GROUP BY li.pipeline_name, dp.adr
ORDER BY li.pipeline_name, total_value;
