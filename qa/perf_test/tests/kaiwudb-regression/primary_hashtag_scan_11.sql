-- Query 11: secondary tag as join key with primary tag in filter
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
where t.station_sn = si.station_sn and 
      t.point_sn in ('a2', 'a6')
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket	
;
