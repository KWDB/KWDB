SELECT li.pipeline_name, 
       li.pipe_start, 
       li.pipe_end, 
       station_name, 
       COUNT(t.measure_value)
FROM pipec_r.pipeline_info li,        -- 26
     pipec_r.station_info si,         -- 436
     db_pipec.t_point t               -- 45M
WHERE t.pipeline_sn = li.pipeline_sn  -- 21, 26
  AND t.station_sn = si.station_sn    -- 401, 436
  AND t.measure_value > 2             -- 44101363/45M = 0.98
  AND t.measure_type = 5              -- 1/17
  AND k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY pipeline_name, pipe_start, pipe_end, station_name
HAVING COUNT(t.measure_value) > 20
ORDER BY COUNT(t.measure_value) DESC, pipeline_name, station_name;