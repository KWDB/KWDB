set hash_scan_mode=1;
WITH device_data AS (
  SELECT
    o.companyid,
    o.time,
    o.value,
    c.area_name,
    d.device_name,
    p.name AS plat_company_name,
    r.level AS risk_level,
    t.index_name,
    t.index_unit
  FROM
    runba.opcdata449600 o
  JOIN
    runba_tra.cd_behavior_area c ON o.companyid = c.company_id
  JOIN
    runba_tra.cd_device_point d ON c.company_id = d.company_id
  JOIN
    runba_tra.plat_device_info p ON d.device_id = p.id
  JOIN
    runba_tra.plat_risk_analyse_objects r ON p.company_id = r.company_id
  JOIN
    runba_tra.plat_tijian_item_index t ON d.index_id = t.id+1
  WHERE
    o.time BETWEEN '2023-01-01' AND '2024-01-01 00:01:00'
    AND c.area_type = 1
    AND d.index_upper_value > 1.0
    AND r.risk_status = 1
)
SELECT
  dd.area_name,
  dd.device_name,
  dd.plat_company_name,
  dd.risk_level,
  cs.total_records,
  cs.avg_value,
  cs.max_value,
  cs.min_value,
  asu.total_devices,
  asu.avg_value_per_area,
  asu.max_value_per_area,
  asu.min_value_per_area,
  rs.total_risks,
  rs.avg_value_per_risk,
  rs.max_value_per_risk,
  rs.min_value_per_risk
FROM
  (
    SELECT
      o.companyid,
      o.time,
      o.value,
      c.area_name,
      d.device_name,
      p.name AS plat_company_name,
      r.level AS risk_level,
      t.index_name,
      t.index_unit
    FROM
      runba.opcdata449600 o
    JOIN
      runba_tra.cd_behavior_area c ON o.companyid = c.company_id
    JOIN
      runba_tra.cd_device_point d ON c.company_id = d.company_id
    JOIN
      runba_tra.plat_device_info p ON d.device_id = p.id
    JOIN
      runba_tra.plat_risk_analyse_objects r ON p.company_id = r.company_id
    JOIN
      runba_tra.plat_tijian_item_index t ON d.index_id = t.id+1
    WHERE
      o.time BETWEEN '2023-01-01' AND '2024-01-01 00:01:00'
      AND c.area_type = 1
      AND d.index_upper_value > 1.0
      AND r.risk_status = 1
  ) dd
JOIN
  (
    SELECT
      companyid,
      COUNT(*) AS total_records,
      AVG(value) AS avg_value,
      MAX(value) AS max_value,
      MIN(value) AS min_value
    FROM
      device_data
    GROUP BY
      companyid
  ) cs ON dd.companyid = cs.companyid
JOIN
  (
    SELECT
      area_name,
      COUNT(*) AS total_devices,
      AVG(value) AS avg_value_per_area,
      MAX(value) AS max_value_per_area,
      MIN(value) AS min_value_per_area
    FROM
      device_data
    GROUP BY
      area_name
  ) asu ON dd.area_name = asu.area_name
JOIN
  (
    SELECT
      risk_level,
      COUNT(*) AS total_risks,
      AVG(value) AS avg_value_per_risk,
      MAX(value) AS max_value_per_risk,
      MIN(value) AS min_value_per_risk
    FROM
      device_data
    GROUP BY
      risk_level
  ) rs ON dd.risk_level = rs.risk_level
ORDER BY
  dd.area_name,
  dd.device_name,
  dd.plat_company_name,
  dd.risk_level;