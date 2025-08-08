set hash_scan_mode=3;
SELECT li.pipeline_name,
       AVG(t.measure_value) AS avg_value,
       MIN(t.measure_value) AS min_value
FROM pipec_r.pipeline_info li
JOIN db_pipec.t_point t ON li.pipeline_sn = t.pipeline_sn
WHERE t.k_timestamp BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY li.pipeline_name
ORDER BY li.pipeline_name, avg_value, min_value;