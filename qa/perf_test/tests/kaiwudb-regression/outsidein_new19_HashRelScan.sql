set hash_scan_mode=3;
SELECT li.pipeline_name,
       SUM(t.measure_value * dp.index_upper_value) AS weighted_sum,
       COUNT(t.measure_value) AS total_count
FROM db_pipec.t_point t
JOIN pipec_r.pipeline_info li ON li.pipeline_sn = t.pipeline_sn
JOIN runba_tra.cd_device_point dp ON dp.index_id = cast(t.measure_type as INT4)
WHERE dp.point_name LIKE 'LI%'
  AND t.measure_value > 50
  AND dp.index_upper_value > 0.5
  AND t.k_timestamp BETWEEN '2024-01-01' AND '2024-01-04 14:30:02'
GROUP BY li.pipeline_name
ORDER BY li.pipeline_name, weighted_sum, total_count;