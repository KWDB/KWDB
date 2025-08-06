SELECT pl.pipeline_name,
  st.station_name,
  AVG(tp.measure_value) AS avg_measure,
  MIN(tp.measure_value) AS min_measure,
  MAX(tp.measure_value) AS max_measure
FROM db_pipec.t_point AS tp
JOIN pipec_r.point_info AS pi
  ON tp.point_sn = pi.point_sn
JOIN pipec_r.pipeline_info AS pl
  ON pi.pipeline_sn = pl.pipeline_sn
JOIN pipec_r.station_info AS st
  ON pi.station_sn = st.station_sn
JOIN pipec_r.workarea_info AS wa
  ON st.work_area_sn = wa.work_area_sn
JOIN pipec_r.company_info AS co
  ON st.sub_company_sn = co.sub_company_sn
WHERE tp.measure_type = 5
  AND tp.measure_value > 90
  AND tp.k_timestamp BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY pl.pipeline_name, st.station_name, wa.work_area_name, co.sub_company_name
HAVING AVG(tp.measure_value) > 95
ORDER BY pl.pipeline_name, st.station_name;