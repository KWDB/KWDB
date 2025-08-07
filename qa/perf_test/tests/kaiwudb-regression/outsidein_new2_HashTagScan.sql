set hash_scan_mode=1;
SELECT li.pipeline_name,
       t.measure_value,
       o.value
FROM pipec_r.pipeline_info li
JOIN db_pipec.t_point t ON li.pipeline_sn = t.pipeline_sn
JOIN runba.opcdata449600 o ON cast(o.slaveid as int2) = CASE
                                         WHEN t.measure_value > 99.996 THEN t.measure_type
                                         ELSE 3
                                      END
WHERE t.k_timestamp BETWEEN '2024-01-04 14:30:00' AND '2024-01-04 14:30:02'
ORDER BY pipeline_name, measure_value, value
LIMIT 10;