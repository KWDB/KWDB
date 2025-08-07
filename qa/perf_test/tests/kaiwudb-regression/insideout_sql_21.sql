SELECT
  c.area_name,
  d.device_name,
  p.name AS plat_company_name,
  r.level AS risk_level,
  COUNT(*),
  SUM(o.value) AS avg_value_per_company,
  MAX(o.value) AS max_value_per_area
FROM
  runba.opcdata449600 o
JOIN
  runba_tra.cd_behavior_area c ON o.companyid = c.company_id
JOIN
  runba_tra.cd_device_point d ON o.companyid = d.company_id
JOIN
  runba_tra.plat_device_info p ON d.company_id = p.company_id
JOIN
  runba_tra.plat_risk_analyse_objects r ON p.company_id = r.company_id
WHERE
   c.area_type = 1
  AND o.device = '1#罐区可燃气体报警器'            -- 1/53
  AND o.datatype = '气体'                        -- 1/4
  AND d.index_upper_value >= 1.2
  AND r.risk_status = 1
GROUP BY c.area_name, d.device_name, p.name, r.level
ORDER BY c.area_name, d.device_name, p.name, r.level;