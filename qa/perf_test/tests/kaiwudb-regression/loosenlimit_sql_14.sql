SELECT 
  wi.work_area_name,
  si.station_name,
  t.measure_type,
  time_bucket(t.k_timestamp, '10s') AS timebucket,
  AVG(CASE WHEN t.measure_value > 100 THEN t.measure_value * 1.1 ELSE t.measure_value * 0.9 END) AS adjusted_avg_value, 
  MIN(t.measure_value / 2) AS half_min_value, 
  COUNT(distinct t.measure_value) AS number_of_values,
  AVG(POWER(t.measure_value, 2)),
  SUM(CAST(t.measure_value AS INT)),
  MAX(LENGTH(t.station_sn)) 
FROM 
  pipec_r.station_info si,
  pipec_r.workarea_info wi,
  pipec_r.pipeline_info li,
  pipec_r.point_info pi,
  db_pipec.t_point t
WHERE 
  li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND li.pipeline_name = 'pipeline_1'
  AND wi.work_area_name IN ('work_area_1', 'work_area_2', 'work_area_3')
  AND t.k_timestamp >= '2023-08-01 01:00:00'
GROUP BY 
  wi.work_area_name,
  si.station_name,
  t.measure_type,
  timebucket
order BY 
  wi.work_area_name,
  si.station_name,
  t.measure_type,
  timebucket;