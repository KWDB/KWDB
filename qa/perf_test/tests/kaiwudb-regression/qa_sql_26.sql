SELECT
    COUNT(*),
    AVG(opcdata."value")
FROM
    runba.opcdata449600 opcdata,
    runba_tra.plat_zone_member pzm
WHERE pzm."tag" = opcdata."tag"
    AND pzm.company_id = opcdata.companyid
    AND opcdata."time" > '2023-05-29 10:10:10'
    AND opcdata."time" <= '2024-01-05 16:10:10'
    AND pzm.contract_email = 'ningmeng0823@hotmail.com' 
    AND pzm.pic2_upload_time = opcdata."time";