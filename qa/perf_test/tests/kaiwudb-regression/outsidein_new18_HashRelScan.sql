set hash_scan_mode=3;
SELECT li.pipeline_name,
       dp.adr,
       t.measure_value
FROM (
    SELECT pipeline_sn, measure_value, measure_type
    FROM db_pipec.t_point
    WHERE measure_value > 50
        AND k_timestamp BETWEEN '2024-01-04 14:32:50' AND '2024-01-04 14:33:02'
) t
JOIN pipec_r.pipeline_info li ON li.pipeline_sn = t.pipeline_sn
JOIN runba_tra.cd_device_point dp ON dp.index_id = t.measure_type 
ORDER BY li.pipeline_name, t.measure_value 
limit 10;