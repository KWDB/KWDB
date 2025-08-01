SELECT si.station_name,
       COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si 
    join pipec_r.workarea_info wi             -- 41
        on wi.work_area_sn = si.work_area_sn
    join db_pipec.t_point t                    -- 45M
        on si.station_sn = t.station_sn
WHERE wi.work_area_name like '%1'          -- 5/41
  AND t.measure_type = 5                   -- 1/17
  AND t.measure_value > 80                 -- est 1/3, act 8995243/45M = 0.19989
GROUP BY si.station_name
HAVING COUNT(t.measure_value) > 10
ORDER BY si.station_name;