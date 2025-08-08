SELECT
    LOWER(wi.work_area_name) AS work_area_name,  
    CONCAT(si.station_name, ' Station') AS station_name,  
    t.measure_type + 1,
    time_bucket(t.k_timestamp, '10s') as timebucket,
    (EXTRACT(EPOCH FROM t.k_timestamp) / 600)::int * 600 AS timebucket_epoch,
    (CASE 
        WHEN t.measure_value > 100 THEN 'High'
        ELSE 'Low'
    END) AS value_range,
    AVG(t.measure_value) AS avg_value,
    MAX(t.measure_value) AS max_value,
    MIN(t.measure_value) AS min_value,
    COUNT(t.measure_value) AS number_of_values
FROM
    pipec_r.station_info si,
    pipec_r.workarea_info wi,
    pipec_r.pipeline_info li,
    pipec_r.point_info pi,
    db_pipec.t_point t
WHERE
    li.pipeline_sn = pi.pipeline_sn
    AND pi.station_sn = si.station_sn
    AND si.work_area_sn = wi.work_area_sn
    AND t.point_sn = pi.point_sn
    AND li.pipeline_name = 'pipeline_1'
    AND wi.work_area_name IN ('work_area_1', 'work_area_2', 'work_area_3')
    AND t.k_timestamp >= '2023-08-01 01:00:00'
GROUP BY
    LOWER(wi.work_area_name), 
    CONCAT(si.station_name, ' Station'), 
    t.measure_type + 1,
    time_bucket(t.k_timestamp, '10s'),
    (EXTRACT(EPOCH FROM t.k_timestamp) / 600)::int * 600, 
    (CASE 
        WHEN t.measure_value > 100 THEN 'High'
        ELSE 'Low'
    END);
