SELECT
  COUNT(*) AS total_records,
  STDDEV(o.value) AS avg_value,
  MAX(o.value) AS max_value,
  MIN(o.value) AS min_value,
  d.device_name as device_name,
  p.name AS plat_company_name
FROM
    runba.opcdata449600 o,
    runba_tra.cd_behavior_area c,
    runba_tra.cd_device_point d,
    runba_tra.plat_device_info p,
    runba_tra.plat_risk_analyse_objects r,
    runba_tra.plat_tijian_item_index t,
    runba_tra.cd_security_device s,
    runba_tra.plat_risk_area a,
    runba_tra.plat_tijian_item_index i,
    runba_tra.plat_tijian_item ti 
  WHERE       o.companyid = c.company_id    
    AND c.company_id = d.company_id
    AND d.company_id = p.company_id
    AND p.company_id = r.company_id  
    AND d.index_id = t.id+1
    AND p.company_id = s.company_id
    AND r.risk_type = s.category
    AND p.area_id = a.risk_id
    AND t.index_name = i.index_name
    AND p.company_id = ti.company_id
    AND o.time BETWEEN '2023-01-01' AND '2024-01-01 01:05:00'
    AND o.tag like 'MAT81117%'  
    AND o.device = '1#罐区可燃气体报警器'            -- 1/53
    AND o.datatype = '气体'                        -- 1/4
    AND p.qianyi_id = 9098
    AND ti.create_user_id = 872
    AND ti.period_id = 710
    AND r.create_user_id = 470462
GROUP BY
  d.device_name,
  plat_company_name
ORDER BY
  d.device_name,
  plat_company_name; 