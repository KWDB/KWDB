set hash_scan_mode=3;
SELECT
    *
FROM 
    runba.opcdata449600 opcdata,
    runba_tra.plat_tijiancard_item_relation ptir,
    runba_tra.plat_tijian_item pti,
    runba_tra.plat_device_info pdi,
    runba_tra.plat_tijian_item_index ptii
WHERE
    (ptir."tijiancard_id" = '6VVkdjOLPOL1brZ8BBXQtvcRuMIfTfLD4uJyFFzbai5U7LywRF'
      OR ptir."tijiancard_id" = 'BGNbk9KrNuk9MYF4xu0L1aqOXi4OIKsLtk6xNMpuRVN6EiMThS')
    AND opcdata.companyid = ptir.company_id
    AND (cast(pti.id as string) = ANY string_to_array(ptir.item_ids, ','))
    AND ptir.obj_id = pdi.id
    AND pti.index_id = ptii.id
    AND opcdata.device = pdi.name
    AND opcdata.datatype = ptii.index_name
    AND opcdata."time" BETWEEN '2024-06-20 23:59:59' AND '2024-06-29 23:59:59'
    AND pti.create_user_id > 8000
    AND pti.create_user_id < 8500
ORDER BY
    opcdata."time",
    opcdata."value"
LIMIT 100;
