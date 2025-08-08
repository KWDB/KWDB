set hash_scan_mode=3;
SELECT
    *
FROM
    runba.opcdata449600 opcdata,                     -- pk: 303, row: 98,074,131
    runba_tra.plat_tijiancard_item_relation ptir,    -- 281,024
    runba_tra.plat_tijian_item pti,                  -- 7,529,071
    runba_tra.plat_device_info pdi,                  -- 19,091
    runba_tra.plat_tijian_item_index ptii            -- 7
WHERE
    opcdata.companyid = ptir.company_id          -- 1/8
    AND ptir.obj_id = pdi.id         -- 1/19092, 1/19091
    AND pti.index_id = ptii.id       -- 1/7, 1/7
    AND opcdata.device = pdi.name
    AND opcdata.datatype = ptii.index_name
    AND pti.create_user_id = 9962
    AND pdi.qianyi_id <= 4;