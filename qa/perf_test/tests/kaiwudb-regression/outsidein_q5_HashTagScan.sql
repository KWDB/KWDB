set hash_scan_mode=1;
SELECT wi.work_area_name,
       t.measure_type,
       COUNT(DISTINCT t.point_sn) AS measure_point_count
FROM pipec_r.pipeline_info li,
     pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE li.pipeline_sn = t.pipeline_sn
  AND si.work_area_sn = wi.work_area_sn
  AND si.work_area_sn = t.work_area_sn
  AND li.pipeline_name = 'pipeline_1'
GROUP BY
    wi.work_area_name, t.measure_type
ORDER BY 
    wi.work_area_name, t.measure_type, measure_point_count;