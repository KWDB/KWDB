SELECT 
  a.area_name,
  d.device_name,
  s.device_no,
  t.index_name,
  r.obj_name,
  COUNT(o.value) AS value_count,
  STDDEV(o.value) AS avg_value,
  MAX(o.value) AS max_value,
  MIN(o.value) AS min_value
FROM 
  runba.opcdata449600 o,
  runba_tra.cd_behavior_area a,
  runba_tra.cd_device_point d,
  runba_tra.cd_security_device s,
  runba_tra.plat_tijian_item_index t,
  runba_tra.plat_risk_analyse_objects r
WHERE 
  o.companyid = a.company_id 
  AND o.companyid = d.company_id 
  AND o.companyid = s.company_id 
  AND d.index_id = t.id 
  AND o.companyid = r.company_id  
  AND a.area_type = 1 
  AND d.index_upper_value > 1.0
  AND s.device_name = '1002'
  AND t.index_unit = 'MPA' 
  AND r.risk_status = 1
  AND o.device = '1#罐区可燃气体报警器'
  AND o.datatype = '气体'
  AND o.time >= '2024-06-29 00:00:00'
GROUP BY 
  a.area_name, 
  d.device_name, 
  s.device_no, 
  t.index_name, 
  r.obj_name
HAVING 
  COUNT(o.value) > 1
order BY 
  a.area_name, 
  d.device_name, 
  s.device_no, 
  t.index_name, 
  r.obj_name;
  