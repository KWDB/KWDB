set hash_scan_mode=3;
SELECT li.pipeline_name,
       time_bucket(t.k_timestamp, '5s') AS timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.pipeline_info li
JOIN db_pipec.t_point t ON li.pipeline_sn = t.pipeline_sn
WHERE t.k_timestamp BETWEEN '2024-01-04 14:30:00' AND '2024-01-04 14:30:30'
GROUP BY li.pipeline_name, timebucket
ORDER BY li.pipeline_name, timebucket
LIMIT 100;