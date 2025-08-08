SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.point_info pi        -- 26
     join pipec_r.pipeline_info li         -- 26
         on li.pipeline_sn = pi.pipeline_sn            -- 150K
     left join pipec_r.station_info si           -- 436
         on pi.station_sn = si.station_sn     
     join db_pipec.t_point t 
          on t.point_sn = pi.point_sn
     join pipec_r.workarea_info wi 
         on si.work_area_sn = wi.work_area_sn        -- 41
WHERE li.pipeline_name = 'pipeline_1'  -- 1/26
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')  -- 3/41
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;