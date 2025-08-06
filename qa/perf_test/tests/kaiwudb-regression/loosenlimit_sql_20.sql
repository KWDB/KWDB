SELECT co.sub_company_name,
  STDDEV(tp.measure_value) AS std_measure,
  MAX(tp.measure_value) AS max_measure,
  MIN(tp.measure_value) AS min_measure
FROM db_pipec.t_point AS tp
JOIN pipec_r.point_info AS pi
  ON tp.point_sn = pi.point_sn
JOIN pipec_r.station_info AS st
  ON pi.station_sn = st.station_sn
JOIN pipec_r.company_info AS co
  ON st.sub_company_sn = co.sub_company_sn
JOIN (
  SELECT sub_company_sn, COUNT(*) AS station_count
  FROM pipec_r.station_info
  GROUP BY sub_company_sn
  HAVING COUNT(*) > 5
) AS sc
  ON co.sub_company_sn = sc.sub_company_sn
WHERE tp.measure_type = 1
  AND tp.measure_value > 50
  AND tp.k_timestamp BETWEEN '2024-01-01' AND '2024-12-31'
  AND EXISTS (
    SELECT 1
    FROM pipec_r.pipeline_info AS pl
    WHERE pl.pipeline_sn = pi.pipeline_sn
      AND pl.pipe_properties LIKE '%1%'
  )
GROUP BY co.sub_company_name
ORDER BY std_measure DESC;