set hash_scan_mode=3;
SELECT li.pipeline_name,
       t.measure_type,
       CASE 
           WHEN t.measure_value > 100 THEN 'High'
           WHEN t.measure_value BETWEEN 50 AND 100 THEN 'Medium'
           ELSE 'Low'
       END AS measure_category,
       CASE 
           WHEN t.k_timestamp > NOW() - INTERVAL '1 DAY' THEN 'Recent'
           ELSE 'Old'
       END AS time_category
FROM pipec_r.pipeline_info li
JOIN db_pipec.t_point t ON li.pipeline_sn = t.pipeline_sn
WHERE t.k_timestamp BETWEEN '2024-01-04 14:34:00' AND '2024-01-04 14:34:12'
ORDER BY li.pipeline_name, t.measure_type, measure_category, time_category
LIMIT 100;