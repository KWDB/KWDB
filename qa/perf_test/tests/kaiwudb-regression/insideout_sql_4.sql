SELECT t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.pipeline_info li,         -- 26
     pipec_r.point_info pi,            -- 150K
     db_pipec.t_point t                -- 45M
WHERE li.pipeline_sn = pi.pipeline_sn  -- 26, 22
  AND pi.station_sn in (
      SELECT si.station_sn    -- 436, 436
      FROM pipec_r.station_info si,          -- 436
           pipec_r.workarea_info wi         -- 41
      WHERE si.work_area_sn = wi.work_area_sn -- 41, 41
        AND wi.work_area_name like '%3'  -- 3/41
      )
  AND t.point_sn = pi.point_sn         -- 150k, 150k
  AND t.point_sn in ('point_sn_1', 'point_sn_2', 'point_sn_100401', 'point_sn_112501')
  AND li.pipeline_name = 'pipeline_17'  -- 1/26
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY t.measure_type, timebucket
ORDER BY t.measure_type, timebucket;