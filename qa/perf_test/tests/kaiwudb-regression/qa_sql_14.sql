SELECT
    last("time"), last("value"), last("device"), last("datatype")
FROM
    runba.opcdata449600 opcdata,
    runba_tra.plat_tijiancard_item_relation ptir,
    runba_tra.plat_tijian_item pti,
    runba_tra.plat_device_info pdi,
    runba_tra.plat_tijian_item_index ptii
WHERE
    ptir."tijiancard_id" = '6VVkdjOLPOL1brZ8BBXQtvcRuMIfTfLD4uJyFFzbai5U7LywRF'
    AND opcdata.companyid = cast(ptir.company_id as float)
    AND (cast(pti.id as char) = ANY string_to_array(ptir.item_ids, ','))
    AND ptir.obj_id = pdi.id
    AND pti.index_id = ptii.id
    AND opcdata.device = pdi.name;
