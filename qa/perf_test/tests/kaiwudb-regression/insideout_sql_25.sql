WITH detailed_data AS (
  SELECT
    o.value,
    d.device_name,
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
  WHERE o.companyid = c.company_id
    AND c.company_id = d.company_id
    AND d.device_id = p.id
    AND p.company_id = r.company_id
    AND d.index_id = t.id+1
    AND p.id = s.id
    AND r.id = a.risk_id
    AND d.index_id = i.id+1
    AND i.id+1 = ti.index_id
    AND o.time BETWEEN '2023-01-01' AND '2024-01-01 01:02:00'
    AND o.tag in ('MAT81117', 'MAT81103', 'MAT81124', 'MAT81120', 'MAT81116')
    AND o.device = '1#罐区可燃气体报警器'            -- 1/53
    AND o.datatype = '气体'                        -- 1/4
    AND ti.create_user_id = 872
    AND c.area_type = 1
)
SELECT
  dd.device_name,
  dd.plat_company_name,
  COUNT(*) AS total_records,
  AVG(dd.value) AS avg_value,
  MAX(dd.value) AS max_value,
  MIN(dd.value) AS min_value
FROM
  detailed_data dd
GROUP BY
  dd.device_name,
  dd.plat_company_name
ORDER BY
  dd.device_name,
  dd.plat_company_name;