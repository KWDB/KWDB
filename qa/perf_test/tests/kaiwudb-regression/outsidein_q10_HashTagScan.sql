set hash_scan_mode=1;
(SELECT pd.qianyi_id as qianyicode,
    time_bucket(o.time, '3600s') as timebucket,
    AVG(o.value) as avg_value
FROM runba.opcdata449600 o,                     -- pk: 303, row: 98,074,131
     runba_tra.cd_security_device csd,          -- 1835
     runba_tra.plat_device_info pd,             -- 19091
     runba_tra.plat_risk_area pra,              -- 57     
     runba_tra.plat_risk_analyse_objects prab   -- 339
WHERE o.companyid = csd.company_id              -- 1/1, 1/2
  AND o.tag = csd.device_no                     -- 1/303, 1/6
  AND csd.device_info_id = pd.id
  AND csd.company_id = pd.company_id    
  AND pd.area_id = pra.id
  AND pd.company_id = prab.company_id           -- 1/4, 1/8
  AND prab.id = pra.risk_id
  AND prab.category = 1                         -- 1/1
  AND prab.danger_level = '四级'                 -- 1/3
  AND csd.category = 5                          -- 1/2
GROUP BY qianyicode, timebucket)
UNION ALL
(SELECT
    pdi.qianyi_id as qianyicode,
    time_bucket(opcdata.time, '3600s') as timebucket,
    AVG(opcdata.value) as avg_value
FROM
    runba.opcdata449600 opcdata,                   -- pk: 303, row: 98,074,131
    runba_tra.plat_device_info pdi                 -- 19,091
WHERE opcdata.companyid = pdi.company_id           -- 1/1, 1/8
  AND opcdata.device = '1#罐区可燃气体报警器'         -- 1/53
  AND opcdata.datatype = '气体'                     -- 1/4
  AND opcdata.time >= '2024-06-29 00:00:00'        -- 1/31
GROUP BY qianyicode, timebucket)
ORDER BY qianyicode, timebucket, avg_value;