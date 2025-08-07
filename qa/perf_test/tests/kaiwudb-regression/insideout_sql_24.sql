SELECT
  COUNT(*) AS total_records,
  AVG(o.value) AS avg_value,
  MAX(o.value) AS max_value,
  MIN(o.value) AS min_value,
  d.device_name as device_name,
  p.name AS plat_company_name,
  r.level AS risk_level,
  t.index_name as index_name,
  t.index_unit as index_unit,
  s.device_name AS security_device_name,
  s.status AS security_device_status,
  a.desc AS risk_area_desc,
  i.index_name AS tijian_index_name,
  i.index_unit AS tijian_index_unit,
  ti.item AS tijian_item,
  ti.content AS tijian_content,
  ti.check_type AS tijian_check_type
FROM
    runba_tra.cd_behavior_area c 
  JOIN
    runba_tra.cd_device_point d ON c.company_id = d.company_id
  JOIN
    runba_tra.plat_device_info p ON d.device_id = p.id
  JOIN
    runba_tra.plat_risk_analyse_objects r ON p.company_id = r.company_id
  JOIN
    runba_tra.plat_tijian_item_index t ON d.index_id = t.id + 1
  JOIN
    runba_tra.cd_security_device s ON p.id = s.id
  JOIN
    runba_tra.plat_risk_area a ON r.id = a.risk_id
  JOIN
    runba_tra.plat_tijian_item_index i ON d.index_id = i.id + 1
  JOIN
    runba_tra.plat_tijian_item ti ON i.id + 1 = ti.index_id
  JOIN
    runba.opcdata449600 o ON o.companyid = c.company_id
  WHERE
    o.time BETWEEN '2023-01-01' AND '2024-01-01 01:05:00'
    AND o.tag in ('MAT81117', 'MAT81103', 'MAT81124', 'MAT81120', 'MAT81116')
    AND o.device = '1#罐区可燃气体报警器'            -- 1/53
    AND o.datatype = '气体'                        -- 1/4
    AND ti.create_user_id = 872
    AND c.area_type = 1
GROUP BY
  d.device_name,
  plat_company_name,
  risk_level,
  t.index_name,
  t.index_unit,
  security_device_name,
  security_device_status,
  risk_area_desc,
  tijian_index_name,
  tijian_index_unit,
  tijian_item,
  tijian_content,
  tijian_check_type
ORDER BY
  d.device_name,
  plat_company_name,
  risk_level;