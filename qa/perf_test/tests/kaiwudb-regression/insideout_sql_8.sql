SELECT
    time_bucket(o."time", '86400s') as timebucket, 
    AVG(o."value") as avgValue
FROM
    runba.opcdata449600 o,
    runba_tra.plat_tijiancard_item_relation ptir,
    runba_tra.plat_tijian_item pti,
    runba_tra.plat_device_info pdi,
    runba_tra.plat_tijian_item_index ptii
WHERE
    o.device = pdi.name
    AND o.companyid = cast(ptir.company_id as decimal)
    AND (cast(pti.id as char) = ANY string_to_array(ptir.item_ids, ','))
    AND ptir.obj_id = pdi.id
    AND pti.index_id = ptii.id
    AND o.tag like 'MAT%' -- 5/303
    AND o.device = '1#罐区可燃气体报警器'            -- 1/53
    AND o.datatype = '气体'                        -- 1/4
    AND ptir."tijiancard_id" = '6VVkdjOLPOL1brZ8BBXQtvcRuMIfTfLD4uJyFFzbai5U7LywRF'
GROUP BY timebucket
ORDER BY timebucket;