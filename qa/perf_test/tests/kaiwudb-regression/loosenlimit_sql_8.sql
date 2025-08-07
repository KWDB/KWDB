SELECT 
  wi.work_area_name,
  si.station_name,
  t.measure_type,
  POWER(t.measure_value, 2) AS adjusted_value
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
  AND li.pipeline_name like '%7'
  AND wi.work_area_name like '%3'
  AND t.measure_type = 1
  AND t.k_timestamp >= '2023-08-01 01:00:00'
ORDER BY 
  si.station_name,
  t.measure_type,
  adjusted_value;