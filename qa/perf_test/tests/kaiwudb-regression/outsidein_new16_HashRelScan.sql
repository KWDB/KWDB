set hash_scan_mode=3;
SELECT li.pipeline_name,
       COUNT(t.measure_value) AS value_count,
       AVG(t.measure_value) AS avg_value
FROM db_pipec.t_point t
JOIN pipec_r.pipeline_info li ON li.pipeline_sn = t.pipeline_sn
JOIN pipec_r.station_info si ON si.station_sn = t.station_sn
WHERE si.work_area_sn = 'work_area_sn_11'
GROUP BY li.pipeline_name;