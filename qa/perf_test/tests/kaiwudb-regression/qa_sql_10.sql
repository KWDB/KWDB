SELECT t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.point_info pi,            -- 150K
     db_pipec.t_point t                -- 45M
WHERE (t.point_sn = pi.point_sn
  OR t.point_sn = 'point_sn_21101')
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY t.measure_type,
         timebucket;
