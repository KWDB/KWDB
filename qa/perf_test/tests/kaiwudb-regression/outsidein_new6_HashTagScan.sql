set hash_scan_mode=1;
SELECT li.pipeline_name,
       dp.adr,
       AVG(t.measure_value) AS avg_value
FROM pipec_r.pipeline_info li
JOIN db_pipec.t_point t ON li.pipeline_sn = t.pipeline_sn
JOIN runba_tra.cd_device_point dp ON cast(dp.index_id as int2) = t.measure_type
WHERE t.measure_value > 99.996
GROUP BY li.pipeline_name, dp.adr
ORDER BY li.pipeline_name, avg_value
limit 10;