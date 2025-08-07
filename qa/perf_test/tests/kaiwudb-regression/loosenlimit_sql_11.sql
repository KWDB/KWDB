SELECT rt.c5,
       AVG(t.measure_value)
FROM pipec_r.station_info si,              -- 436
     pipec_r.workarea_info wi,             -- 41
     db_pipec.t_point t,                   -- 45M
     test_rel.rel_t1 rt
WHERE wi.work_area_sn = si.work_area_sn    -- 41, 41
  AND si.station_sn = t.station_sn         -- 436, 401
  AND t.measure_type = rt.c6
  AND t.measure_value > 80                 -- est 1/3, act 8995243/45M = 0.19989
GROUP BY rt.c5
order BY AVG(t.measure_value);