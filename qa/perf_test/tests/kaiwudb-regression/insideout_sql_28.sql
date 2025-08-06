SELECT
  c.area_name,
  d.device_name,
  p.name AS plat_company_name,
  r.level AS risk_level,
  COUNT(*) AS device_count,
  AVG(o.value) AS avg_value_per_device,
  COUNT(DISTINCT s.device_no) AS unique_security_devices,        
  MIN(o.value) AS min_value_per_device,
  MAX(o.value) AS max_value_per_device,
  COUNT(DISTINCT a.area_id) AS unique_risk_areas
FROM
  runba_tra.cd_behavior_area c,
  runba_tra.cd_device_point d,
  runba_tra.plat_device_info p,
  runba_tra.plat_risk_analyse_objects r,
  runba_tra.cd_security_device s,
  runba_tra.plat_risk_area a,
  runba.opcdata449600 o
WHERE o.companyid = c.company_id
  AND c.company_id = d.company_id
  AND d.device_id = p.id
  AND p.company_id = r.company_id
  AND p.id = s.id
  AND r.id = a.risk_id
  AND o.device = '1#罐区可燃气体报警器'
  AND o.datatype = '气体'
  AND o.time >= '2024-06-29 23:00:00'
GROUP BY
  c.area_name,
  d.device_name,
  p.name,
  r.level
HAVING
  AVG(o.value) > 30.0
ORDER BY
  c.area_name,
  d.device_name,
  p.name,
  r.level;
