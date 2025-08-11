select * from ((SELECT
    COUNT(*),
    AVG(opcdata."value")
FROM
    runba.opcdata449600 opcdata
    , runba_tra.plat_zone_member pzm
 where pzm."tag" = opcdata."tag"
    AND pzm.company_id = opcdata.companyid
    AND  opcdata."time" > '2023-05-29 10:10:10'
    AND opcdata."time" <= '2024-05-29 16:10:10'
    AND pzm.contract_email = 'ningmeng0823@hotmail.com') 
  union all 
  (SELECT
    COUNT(*),
    AVG(opcdata."value")
 FROM
    runba.opcdata449600 opcdata
  , runba_tra.plat_zone_member pzm
 where pzm."tag" = opcdata."tag"
    AND pzm.company_id = opcdata.companyid
    and  opcdata."time" > '2023-05-29 10:10:10'
    AND opcdata."time" <= '2024-05-29 16:10:10'
    AND pzm.contract_email = 'ningmeng0823@hotmail.com'));
