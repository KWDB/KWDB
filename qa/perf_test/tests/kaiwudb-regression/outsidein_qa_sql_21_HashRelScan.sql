set hash_scan_mode=3;
SELECT
    time_bucket(opcdata."time", '10s') AS timebucket,
    ROUND(AVG(opcdata."value"), 2) as avg,
    MIN(opcdata."value"),
    MAX(opcdata."value"),
    SUM(opcdata."value")
FROM
    runba.opcdata449600 opcdata
    JOIN runba_tra.plat_zone_member pzm
      ON pzm."tag" = opcdata."tag"
        AND pzm.company_id = opcdata.companyid
WHERE
    opcdata."time" > '2023-05-29 10:10:10'
    AND opcdata."time" <= '2024-05-29 16:10:10'
    AND pzm.contract_email = 'ningmeng0823@hotmail.com'
GROUP BY
    timebucket
ORDER BY
    timebucket;
