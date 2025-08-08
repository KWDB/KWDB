SELECT si.station_name,
       COUNT(t.measure_value),
       ROUND(AVG(t.measure_value), 2) as avg
FROM pipec_r.station_info si,              -- 436
     pipec_r.workarea_info wi,             -- 41
     db_pipec.t_point t                    -- 45M
WHERE wi.work_area_name = 'work_area_1'    -- 1/41
  AND wi.work_area_sn = si.work_area_sn    -- 41, 41
  AND si.station_sn = t.station_sn         -- 436, 401
  AND t.measure_type = 5                   -- 1/17
  AND t.point_sn = 'point_sn_1858'
  AND t.measure_value > 80                 -- est 1/3, act 8995243/45M = 0.19989
GROUP BY si.station_name
HAVING COUNT(t.measure_value) > 10
ORDER BY si.station_name;
