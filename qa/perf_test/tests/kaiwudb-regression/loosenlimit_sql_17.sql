SELECT wi.work_area_name,
       si.station_name, 
       t.measure_type,
       time_bucket(t.k_timestamp, '5s') as timebucket,
       STDDEV(t.measure_value)
FROM pipec_r.point_info pi,           -- 41 
     pipec_r.pipeline_info li,        -- 41 
     pipec_r.workarea_info wi,        -- 41
     pipec_r.station_info si,         -- 436
     db_pipec.t_point t               -- 45M
WHERE pi.pipeline_sn = li.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn -- 41, 41
  AND pi.point_sn = t.point_sn      -- 436, 401
  AND li.pipeline_name = 'pipeline_1'    -- 1/26
  AND wi.work_area_name = 'work_area_1' -- 1/41
GROUP BY wi.work_area_name, si.station_name, t.measure_type, timebucket
order BY wi.work_area_name, si.station_name, t.measure_type, timebucket;