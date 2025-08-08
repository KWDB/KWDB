SELECT si.station_name,
       COUNT(DISTINCT point_sn) AS abnormal_point_count
FROM pipec_r.pipeline_info li,      -- 26
     pipec_r.station_info si,       -- 436
     db_pipec.t_point t             -- 45M
WHERE li.pipeline_sn = t.pipeline_sn             -- 26, 21
    AND t.station_sn = si.station_sn             -- 401, 436
    AND li.pipeline_name = 'pipeline_1'          -- 1/26
    AND t.measure_type = 4                       -- 1/17
    AND t.k_timestamp >= '2023-08-01 00:00:00'
    AND t.k_timestamp <= '2024-08-01 01:00:00'   -- 1/1 (all data passed)
    AND t.measure_value < 0.5 * (
        SELECT AVG(t1.measure_value) 
        FROM db_pipec.t_point t1                 -- 45M
        WHERE t1.pipeline_sn = li.pipeline_sn    -- 21, 26
          AND t1.measure_type = 4)               -- 1/17
GROUP BY
    si.station_name
ORDER BY
    abnormal_point_count DESC;
