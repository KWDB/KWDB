set hash_scan_mode=3;
SELECT pra.risk_id,
       sub_query.index_id,
       sub_query.area_name,
       sub_query.avg_value
FROM (
    SELECT dp.index_id,
           cba.area_name,
           AVG(t.measure_value) AS avg_value
    FROM db_pipec.t_point t
    JOIN runba_tra.cd_device_point dp ON cast(dp.index_id as int2) = t.measure_type
    JOIN runba_tra.cd_behavior_area cba ON cba.company_id = dp.company_id
    WHERE dp.point_name LIKE '%PI%'
    GROUP BY dp.index_id, cba.area_name
) sub_query
JOIN runba_tra.plat_risk_area pra ON sub_query.index_id = pra.id;