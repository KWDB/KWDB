SELECT pd.qianyi_id as qianyicode,
    time_bucket(o.time, '86400s') as timebucket,
    AVG(o.value) as avg_value
FROM runba_tra.plat_device_info pd,             -- 19091
     runba_tra.plat_risk_area pra,              -- 57     
     runba.opcdata449600 o,                     -- pk: 303, row: 98,074,131     
     runba_tra.plat_risk_analyse_objects prab  -- 339
WHERE o.companyid = pd.company_id               -- 1/1, 1/8
  AND prab.id = pra.risk_id                     -- 1/339, 1/1
  AND prab.company_id = pd.company_id           -- 1/4, 1/8  
  AND pd.area_id = pra.id                       -- 1/5, 1/57
  AND prab.category = 1                         -- 1/1
  AND prab.risk_type = 5                        -- 1/3
  AND o.tag in ('MAT81117', 'MAT81103', 'MAT81124', 'MAT81120', 'MAT81116')                          -- 5/303
  AND o.device = '1#罐区可燃气体报警器'            -- 1/53
  AND o.datatype like '%体'                        -- 1/4
GROUP BY qianyicode, timebucket
ORDER BY qianyicode, timebucket;