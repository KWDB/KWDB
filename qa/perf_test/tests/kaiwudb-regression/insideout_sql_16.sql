SELECT pd.qianyi_id as qianyicode,
    time_bucket(o.time, '86400s') as timebucket,
    COUNT(o.value) as cnt_value
FROM runba_tra.plat_device_info pd,             -- 19091
     runba.opcdata449600 o,                     -- pk: 303, row: 98,074,131     
     runba_tra.plat_risk_analyse_objects prab  -- 339
WHERE o.companyid = pd.company_id               -- 1/1, 1/8
  AND prab.company_id = pd.company_id           -- 1/4, 1/8  
  AND prab.category = 1                         -- 1/1
  AND prab.risk_type > 4                        -- 1/3
  AND o.tag like 'MAT81%'                        -- 5/303
  AND o.device like '1#%'            -- 1/53
  AND o.datatype like '%体'                        -- 1/4
GROUP BY qianyicode, timebucket
ORDER BY qianyicode, timebucket;