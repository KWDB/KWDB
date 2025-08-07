SELECT w.work_area_name,
 STDDEV(tp.measure_value) AS stddev_value,
  MAX(tp.measure_value) AS max_value,
  MIN(tp.measure_value) AS min_value,
  (SELECT COUNT(*) 
   FROM pipec_r.station_info AS st2,
        pipec_r.workarea_info AS w2
   WHERE st2.work_area_sn = w2.work_area_sn) AS station_count
FROM db_pipec.t_point AS tp 
JOIN pipec_r.point_info AS pi
  ON tp.point_sn = pi.point_sn
JOIN pipec_r.station_info AS st
  ON pi.station_sn = st.station_sn
JOIN pipec_r.workarea_info AS w
  ON st.work_area_sn = w.work_area_sn
WHERE tp.measure_value > 80
    AND tp.measure_type = 1
    AND pi.signal_type like '%type%'
    AND tp.k_timestamp >= '2024-01-01 00:00:00'
    AND st.station_location LIKE '%00'
    AND EXISTS (
    SELECT 1
    FROM pipec_r.company_info AS co
    WHERE co.sub_company_sn = st.sub_company_sn
      AND co.sub_company_name LIKE '%com%'
  )
GROUP BY w.work_area_name
HAVING COUNT(tp.measure_value) > 10
ORDER BY w.work_area_name;