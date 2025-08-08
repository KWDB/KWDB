  SELECT
    c.area_name,
    d.device_name,
    STDDEV(o.value) AS stddev_value,
    SUM(CASE WHEN o.value > 100 THEN 1 ELSE 0 END) AS high_value_count,
    COUNT(DISTINCT s.device_no) AS unique_security_devices,
    SUM(CASE WHEN o.value < 10 THEN 1 ELSE 0 END) AS low_value_count,
    COUNT(DISTINCT a.area_id) AS unique_risk_areas
  FROM
    runba.opcdata449600 o,
    runba_tra.cd_behavior_area c,
    runba_tra.cd_device_point d,
    runba_tra.plat_device_info p,
    runba_tra.plat_risk_analyse_objects r,
    runba_tra.cd_security_device s,
    runba_tra.plat_risk_area a
  WHERE
    o.companyid = c.company_id
    AND c.company_id = d.company_id
    AND d.device_id = p.id
    AND p.company_id = r.company_id
    AND p.id = s.id
    AND r.id = a.risk_id
    AND o.time BETWEEN '2023-01-01' AND '2024-01-01 01:05:00'
    AND c.is_select is NULL
    AND d.index_id > 6
    AND r.is_access = 0
    AND o.value > 15
    AND o.value < 180
    AND s.realtime_play = 0
    AND r.category = 1
  GROUP BY
    c.area_name,
    d.device_name
  HAVING
    STDDEV(o.value) > 2.8
  ORDER BY
    c.area_name,
    d.device_name;
