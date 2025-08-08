set hash_scan_mode=1;
SELECT
    time_bucket(opcdata."time", '10s') AS timebucket,
    AVG(opcdata."value"),
    MIN(opcdata."value"),
    MAX(opcdata."value"),
    SUM(opcdata."value")
FROM
    runba.opcdata449600 opcdata,
    (select c.company_id as company_id, p.company_id as c_id  
          from runba_tra.plat_zone_member p 
          left outer join runba_tra.cd_security_device c 
          on p.company_id = c.company_id limit 100
         ) as pzm
WHERE
    pzm.company_id = opcdata.companyid
    AND opcdata."time" > '2023-05-29 10:10:10'
    AND opcdata."time" <= '2024-05-29 11:10:10'
GROUP BY
    timebucket
ORDER BY
    timebucket;