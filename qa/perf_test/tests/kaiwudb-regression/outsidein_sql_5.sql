set hash_scan_mode=0;
SELECT li.pipeline_name,
       t.measure_type,
       SUM(t.measure_value) AS total_measure,
       COUNT(t.measure_value) AS count_measure
FROM pipec_r.pipeline_info li
JOIN db_pipec.t_point t ON li.pipeline_sn = t.pipeline_sn
WHERE t.measure_type = 1
GROUP BY li.pipeline_name, t.measure_type
ORDER BY li.pipeline_name, t.measure_type, total_measure;