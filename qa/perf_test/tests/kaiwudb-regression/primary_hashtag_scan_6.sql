-- Query 6: primary tag as join key with function and secondary tag as join key
SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r_small.station_info si,
     pipec_r_small.workarea_info wi,
     pipec_r_small.pipeline_info li,
     pipec_r_small.point_info pi,
     db_pipec_small.t_point t   
where t.point_sn = concat(pi.point_sn, '') and t.station_sn = si.station_sn
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
;

