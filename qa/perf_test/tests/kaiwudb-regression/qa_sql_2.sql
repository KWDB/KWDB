SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.station_info si,          -- 436
     pipec_r.workarea_info wi,         -- 41
     pipec_r.pipeline_info li,         -- 26
     pipec_r.point_info pi,            -- 150K
     db_pipec.t_point t                -- 45M
WHERE li.pipeline_sn = pi.pipeline_sn  -- 26, 22
  AND pi.station_sn = si.station_sn    -- 436, 436
  AND si.work_area_sn = wi.work_area_sn -- 41, 41
  AND li.pipeline_name = 'pipeline_1'  -- 1/26
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')  -- 3/41
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
  AND t.point_sn = 'point_sn_1'
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;