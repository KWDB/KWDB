SELECT ci1.sub_company_name,
       wi1.work_area_name,
       si1.station_name,
       t1.measure_type,
       time_bucket(t1.k_timestamp, '10s') as timebucket,
       AVG(t1.measure_value) AS avg_value,
       MAX(t1.measure_value) AS max_value,
       MIN(t1.measure_value) AS min_value,
       COUNT(t1.measure_value) AS number_of_values
FROM pipec_r.station_info si1,          -- 436
     pipec_r.station_info si2,          -- 436
     pipec_r.workarea_info wi1,         -- 41
     pipec_r.workarea_info wi2,         -- 41
     pipec_r.company_info ci1,         -- 8
     pipec_r.company_info ci2,         -- 8
     pipec_r.pipeline_info li1,         -- 26
     pipec_r.pipeline_info li2,         -- 26
     pipec_r.point_info pi1,            -- 150K
     pipec_r.point_info pi2,            -- 150K     
     db_pipec.t_point t1,                -- 45M
     db_pipec.t_point t2                -- 45M
WHERE li1.pipeline_sn = pi1.pipeline_sn  -- 26, 22
  AND pi1.station_sn = si1.station_sn    -- 436, 436
  AND si1.sub_company_sn = ci1.sub_company_sn -- 8, 8
  AND si1.work_area_sn = wi1.work_area_sn -- 41, 41
  AND t1.point_sn = pi1.point_sn         -- 150k, 150k
  AND li1.pipeline_name = 'pipeline_1'  -- 1/26
  AND wi1.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')  -- 3/41
  AND ci1.sub_company_name = 'sub_com_1' -- 1/8
  AND li2.pipeline_sn = pi2.pipeline_sn  -- 26, 22
  AND pi2.station_sn = si2.station_sn    -- 436, 436
  AND si2.sub_company_sn = ci2.sub_company_sn -- 8, 8
  AND si2.work_area_sn = wi2.work_area_sn -- 41, 41
  AND t2.point_sn = pi2.point_sn         -- 150k, 150k  
  AND li1.pipe_start = li2.pipe_start    -- 3, 3
  AND pi1.signal_type = pi2.signal_type  -- 10, 10
  AND pi1.signal_code = pi2.signal_code  -- 10, 10
  AND li2.pipeline_name = 'pipeline_4'  -- 1/26
  AND wi2.work_area_name in ('work_area_7', 'work_area_8', 'work_area_9')  -- 3/41
  AND ci2.sub_company_name = 'sub_com_2'       -- 1/8
  AND t1.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
  AND t1.k_timestamp = t2.k_timestamp      -- 300, 300
  AND t1.measure_type = t2.measure_type    -- 17, 17
  AND t1.measure_value > t2.measure_value  -- 9888966, 9888966
GROUP BY ci1.sub_company_name,
         wi1.work_area_name,
         si1.station_name,
         t1.measure_type,
         timebucket;
