SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.station_info si,          -- 436
     pipec_r.workarea_info wi,         -- 41
     pipec_r.pipeline_info li,         -- 26
     pipec_r.point_info pi,            -- 150K
     db_pipec.t_point t                -- 45M
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;
