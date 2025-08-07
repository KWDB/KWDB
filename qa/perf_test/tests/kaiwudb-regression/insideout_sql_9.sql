SELECT tempR.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM db_pipec.t_point t,                -- 45M
     (SELECT pi.point_sn,    -- 436, 436
           si.station_name
      FROM pipec_r.pipeline_info li,         -- 26
           pipec_r.point_info pi,            -- 150K
           pipec_r.station_info si,          -- 436
           pipec_r.workarea_info wi         -- 41
      WHERE si.work_area_sn = wi.work_area_sn -- 41, 41
        AND pi.station_sn = si.station_sn
        AND pi.pipeline_sn = li.pipeline_sn  -- 26, 22
        AND wi.work_area_name like '%3'  -- 3/41
        AND li.pipeline_name like '%7'  -- 1/26
      ) as tempR
WHERE t.point_sn = tempR.point_sn    -- 436, 436
  AND t.point_sn in ('point_sn_1', 'point_sn_2', 'point_sn_100401', 'point_sn_112501')
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY tempR.station_name,
         t.measure_type,
         timebucket
ORDER BY tempR.station_name,
         t.measure_type,
         timebucket;