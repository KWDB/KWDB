set hash_scan_mode=3;
SELECT
        time_bucket(opcdata."time", '10s') AS timebucket,
        AVG(opcdata."value"),
        MIN(opcdata."value"),
        MAX(opcdata."value"),
        SUM(opcdata."value")
    FROM
        runba.opcdata449600 opcdata,
        runba_tra.plat_zone_member pzm
    WHERE
        pzm."tag" = opcdata."tag"
        AND pzm.company_id = opcdata.companyid
        AND opcdata."time" > '2023-05-29 10:10:10'
        AND opcdata."time" <= '2024-05-29 16:10:10'
        AND pzm.contract_email = 'ningmeng0823@hotmail.com'
    GROUP BY
        timebucket
    HAVING
        AVG(opcdata."value") > 65.0
        AND MAX(pzm.zone_company_id) > 100
    ORDER BY
        timebucket desc;
