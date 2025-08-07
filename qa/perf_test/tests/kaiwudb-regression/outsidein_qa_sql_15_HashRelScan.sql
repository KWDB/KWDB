set hash_scan_mode=3;
SELECT
    last("time") as time, last("value") as value, last("device") as device, last("datatype") as datatype
FROM
    runba.opcdata449600 opcdata,
    runba_tra.plat_tijiancard_item_relation ptir,
    runba_tra.plat_tijian_item pti,
    runba_tra.plat_device_info pdi,
    runba_tra.plat_tijian_item_index ptii
WHERE
    ptir."tijiancard_id" = '6VVkdjOLPOL1brZ8BBXQtvcRuMIfTfLD4uJyFFzbai5U7LywRF'
    AND opcdata.companyid = ptir.company_id
    AND (cast(pti.id as char) = ANY string_to_array(ptir.item_ids, ','))
    AND ptir.obj_id = pdi.id
    AND pti.index_id = ptii.id
    AND opcdata.device = cast(pdi.name as string);
