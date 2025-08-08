SELECT 
  a.area_name,
  d.device_name,
  s.device_no,
  t.index_name,
  r.obj_name
FROM
  runba_tra.cd_behavior_area a 
JOIN 
  runba_tra.cd_device_point d ON a.company_id = d.company_id
JOIN 
  runba_tra.cd_security_device s ON a.company_id = s.company_id
JOIN 
  runba_tra.plat_tijian_item_index t ON d.index_id = t.id
JOIN 
  runba_tra.plat_risk_analyse_objects r ON a.company_id = r.company_id
JOIN   
  runba.opcdata449600 o ON o.companyid = a.company_id
WHERE a.area_type = 1 
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
