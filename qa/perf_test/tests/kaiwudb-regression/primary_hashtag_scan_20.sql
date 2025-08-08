-- Query 20: primary tag as join key with join on predicate
SELECT 
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM 
     pipec_r_small.point_info pi join          
     db_pipec_small.t_point t on  pi.point_sn = t.point_sn
GROUP BY 
         t.measure_type,
         timebucket
ORDER BY t.measure_type,
         timebucket
;
