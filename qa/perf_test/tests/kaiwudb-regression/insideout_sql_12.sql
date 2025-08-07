SELECT
    AVG(o."value"),
    MIN(o."value"),
    MAX(o."value"),
    SUM(o."value")
FROM
    runba.opcdata449600 o,
    runba_tra.plat_zone_member pzm
WHERE pzm.company_id = o.companyid
    AND o."time" > '2023-12-29 10:10:10'
    AND o."time" <= '2024-05-29 16:10:10'
    AND o.tag like 'MAT%' -- 5/303
    AND o.device = '1#罐区可燃气体报警器'            -- 1/53
    AND o.datatype = '气体'                        -- 1/4
HAVING
    AVG(o."value") > 45.0
    AND MAX(pzm.zone_company_id) > 100;