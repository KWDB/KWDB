SELECT si.station_name,  -- pipe_test:q1 
       COUNT(t.measure_value) as count,
       ROUND(AVG(t.measure_value), 3) as avg
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t 
WHERE wi.work_area_name = 'work_area_1' 
  AND wi.work_area_sn = si.work_area_sn 
  AND si.station_sn = t.station_sn 
  AND t.measure_type = 5 
  AND t.measure_value > 80 
GROUP BY si.station_name 
HAVING COUNT(t.measure_value) > 10 
ORDER BY si.station_name;
