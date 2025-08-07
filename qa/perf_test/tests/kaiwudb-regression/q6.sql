SELECT ROUND(AVG(t.measure_value), 2) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.pipeline_info li,
     db_pipec.t_point t
WHERE li.pipeline_sn = t.pipeline_sn
GROUP BY li.pipeline_name,
         t.measure_type
ORDER BY li.pipeline_name,
         t.measure_type;
