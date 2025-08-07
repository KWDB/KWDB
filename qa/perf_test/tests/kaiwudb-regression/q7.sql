SELECT
    "time",
    "value",
    "device",
    "datatype"
FROM 
    runba.opcdata449600 opcdata,                     -- pk: 303, row: 98,074,131
    runba_tra.plat_tijiancard_item_relation ptir,    -- 281,024
    runba_tra.plat_tijian_item pti,                  -- 7,529,071
    runba_tra.plat_device_info pdi,                  -- 19,091
    runba_tra.plat_tijian_item_index ptii            -- 7
WHERE
    ptir."tijiancard_id" = '6VVkdjOLPOL1brZ8BBXQtvcRuMIfTfLD4uJyFFzbai5U7LywRF' -- 1/281024
    AND opcdata.companyid = ptir.company_id          -- 1/8
    AND (cast(pti.id as string) = ANY string_to_array(ptir.item_ids, ',')) -- 1/7529071, 1/2
    AND ptir.obj_id = pdi.id         -- 1/19092, 1/19091
    AND pti.index_id = ptii.id       -- 1/7, 1/7        
    AND opcdata.device = pdi.name    -- 1/53, 1/2
    AND opcdata.datatype = ptii.index_name    -- 1/4, 1/7
    AND opcdata."time" BETWEEN '2024-06-20 23:59:59' AND '2024-06-29 23:59:59'
    AND pti.create_user_id > 8000
    AND pti.create_user_id < 8500
ORDER BY
    "time" DESC
LIMIT 1;