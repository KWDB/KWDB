-- Query 10: primary tag as join key with in filter
SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r_small.station_info si,          -- 436
     pipec_r_small.workarea_info wi,         -- 41
     pipec_r_small.pipeline_info li,         -- 26
     pipec_r_small.point_info pi,            -- 150K
     db_pipec_small.t_point t   
where t.point_sn =  pi.point_sn and pi.point_sn in ('a1', 'a2', 'a3')
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
;
