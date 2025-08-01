SELECT time_bucket(o.time, '10s') as timebucket,
       ROUND(AVG(o.value), 2) AS avg_value
FROM runba.opcdata449600 o,                    -- pk: 303, row: 98,074,131
     runba_tra.cd_security_device csd,            -- 1835
     runba_tra.cd_device_point cdp,               -- 650
     runba_tra.cd_tijian_item_index ctii          -- 8
WHERE o.companyid = csd.company_id      -- 1/1, 1/2
  AND o.tag = csd.device_no             -- 1/303, 1/6
  AND csd.id = cdp.device_id            -- 1/1835, 1/15
  AND ctii.id = cdp.index_id            -- 1/8, 1/2
  AND csd.device_info_id in (           -- 1/1835
      SELECT id
      FROM runba_tra.plat_device_info pd         -- 19091
      WHERE pd.area_id in (             -- 1/5
            SELECT id 
            FROM runba_tra.plat_risk_area pra     -- 57
            WHERE risk_id in (          -- 1/1
                  SELECT id
                  FROM runba_tra.plat_risk_analyse_objects prab    -- 339
                  WHERE prab.company_id = pd.company_id  -- 1/4, 1/8
                    AND prab.category = 1                -- 1/1
                    AND prab.danger_level = '四级'        -- 1/3
                    AND prab.risk_type = 5))             -- 1/3
         AND pd.company_id = csd.company_id)             -- 1/8, 1/2
  AND csd.company_id = 449600                        -- 1/2
  AND csd.category = 5                                -- 1/2
GROUP BY timebucket
ORDER BY timebucket ASC; 
