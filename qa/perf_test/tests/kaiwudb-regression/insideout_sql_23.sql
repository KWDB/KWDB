WITH detailed_data AS (
  SELECT
    c.company_id,
    c.area_name,
    d.device_name,
    p.name AS plat_company_name,
    r.level AS risk_level,
    t.index_name,
    t.index_unit,
    s.device_no,
    s.device_name AS security_device_name,
    s.status AS security_device_status,
    a.desc AS risk_area_desc,
    i.index_name AS tijian_index_name,
    i.index_unit AS tijian_index_unit,
    ti.item AS tijian_item,
    ti.check_type AS tijian_check_type
  FROM
    runba_tra.cd_behavior_area c,
    runba_tra.cd_device_point d,
    runba_tra.plat_device_info p,
    runba_tra.plat_risk_analyse_objects r,
    runba_tra.plat_tijian_item_index t,
    runba_tra.cd_security_device s,
    runba_tra.plat_risk_area a,
    runba_tra.plat_tijian_item_index i,
    runba_tra.plat_tijian_item ti
  WHERE
    c.company_id = d.company_id
    AND d.company_id = p.company_id
    AND p.company_id = r.company_id  
    AND d.index_id = t.id+1
    AND p.company_id = s.company_id
    AND r.risk_type = s.category
    AND p.area_id = a.risk_id
    AND t.index_name = i.index_name
    AND p.company_id = ti.company_id
    AND p.qianyi_id = 9098
    AND ti.create_user_id = 872
    AND ti.period_id = 710
    AND r.create_user_id = 470462
)
SELECT
  dd.device_name,
  dd.plat_company_name,
  dd.risk_level,
  dd.index_name,
  dd.index_unit,
  dd.security_device_name,
  dd.security_device_status,
  dd.risk_area_desc,
  dd.tijian_index_name,
  dd.tijian_index_unit,
  dd.tijian_item,
  dd.tijian_check_type,
  COUNT(*) AS total_records,
  AVG(o.value) AS avg_value,
  MAX(o.value) AS max_value,
  MIN(o.value) AS min_value
FROM
  runba.opcdata449600 o,
  detailed_data dd
WHERE o.companyid = dd.company_id
    AND o.tag in ('MAT81117', 'MAT81103', 'MAT81124')   -- 3/303
    AND o.device = '1#罐区可燃气体报警器'            -- 1/53
    AND o.datatype = '气体'                        -- 1/4
GROUP BY
  dd.device_name,
  dd.plat_company_name,
  dd.risk_level,
  dd.index_name,
  dd.index_unit,
  dd.security_device_name,
  dd.security_device_status,
  dd.risk_area_desc,
  dd.tijian_index_name,
  dd.tijian_index_unit,
  dd.tijian_item,
  dd.tijian_check_type
ORDER BY
  dd.device_name,
  dd.plat_company_name,
  dd.risk_level;
