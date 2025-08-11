SELECT wi.work_area_name,  -- pipe_test:q5 
       t.measure_type,
       COUNT(DISTINCT t.point_sn) AS measure_point_count
FROM pipec_r.pipeline_info li,          -- 26
     pipec_r.station_info si,           -- 436
     pipec_r.workarea_info wi,          -- 41
     db_pipec.t_point t                 -- 45M
WHERE li.pipeline_sn = t.pipeline_sn    -- 26, 21
  AND si.work_area_sn = wi.work_area_sn -- 41, 41
  AND si.work_area_sn = t.work_area_sn  -- 41, 41
  AND li.pipeline_name = 'pipeline_1'   -- 1/26
GROUP BY
    wi.work_area_name, t.measure_type
ORDER BY 
    wi.work_area_name, t.measure_type, measure_point_count;
