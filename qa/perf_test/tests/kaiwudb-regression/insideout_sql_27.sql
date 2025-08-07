  SELECT
    c.area_name,
    d.device_name,
    p.name AS plat_company_name,
    r.level AS risk_level,
    COUNT(*) AS device_count,
    AVG(o.value) AS avg_value_per_device,
    MIN(o.value) AS min_value_per_device,
    MAX(o.value) AS max_value_per_device,
    COUNT(DISTINCT s.device_no) AS unique_security_devices
  FROM
    runba.opcdata449600 o,
    runba_tra.cd_behavior_area c,
    runba_tra.cd_device_point d,
    runba_tra.plat_device_info p,
    runba_tra.plat_risk_analyse_objects r,
    runba_tra.cd_security_device s
  WHERE
    o.companyid = c.company_id
    AND c.company_id = d.company_id
    AND d.device_id = p.id
    AND p.company_id = r.company_id
    AND p.id = s.id
    AND o.time BETWEEN '2023-01-01' AND '2024-01-01 01:01:00'
    AND c.area_type = 1
    AND d.index_upper_value > 1.2
    AND r.risk_status = 1
    AND o.value > 20
    AND o.value < 200
  GROUP BY
    c.area_name,
    d.device_name,
    p.name,
    r.level
  HAVING
    AVG(o.value) > 50.0
  ORDER BY
    c.area_name,
    d.device_name,
    p.name,
    r.level;
