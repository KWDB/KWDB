set hash_scan_mode=3;
SELECT
    time_bucket(t.k_timestamp, '1h') AS timebucket,
    s.work_area_sn,
    w.work_area_name,
    pinfo.pipeline_name,
    COUNT(t.k_timestamp) AS measurement_count,
    SUM(t.measure_value) AS total_measure_value,
    AVG(t.measure_value) AS avg_measure_value
FROM
    db_pipec.t_point t,        -- 45M
    pipec_r.station_info s,    -- 436
    pipec_r.workarea_info w,   -- 41
    pipec_r.pipeline_info pinfo  -- 26
WHERE
    t.work_area_sn = s.work_area_sn    -- 41, 41
    AND t.pipeline_sn = pinfo.pipeline_sn    -- 21, 41
    AND s.work_area_sn = w.work_area_sn    -- 41, 41
    AND t.k_timestamp BETWEEN '2024-01-04 14:30:51' AND '2024-01-04 14:31:00'    -- 2M/45M
GROUP BY
    timebucket, s.work_area_sn, w.work_area_name, pinfo.pipeline_name
ORDER BY
    timebucket, s.work_area_sn;
