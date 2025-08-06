SELECT
    AVG(opcdata."value") as avg_val,
    MIN(opcdata."value") as min_val,
    MAX(opcdata."value") as max_val,
    COUNT(opcdata."value") as count_val,
    opcdata."device",
    opcdata."datatype",
    cast(ptir.company_id as decimal) as cid
FROM 
    runba.opcdata449600 opcdata,
    runba_tra.plat_tijiancard_item_relation ptir,
    runba_tra.plat_tijian_item pti,
    runba_tra.plat_device_info pdi,
    runba_tra.plat_tijian_item_index ptii
WHERE
    (ptir."tijiancard_id" = '6VVkdjOLPOL1brZ8BBXQtvcRuMIfTfLD4uJyFFzbai5U7LywRF'
      OR ptir."tijiancard_id" = 'BGNbk9KrNuk9MYF4xu0L1aqOXi4OIKsLtk6xNMpuRVN6EiMThS')
    AND opcdata.companyid = cast(ptir.company_id as INT)
    AND (cast(pti.id as string) = ANY string_to_array(ptir.item_ids, ','))
    AND ptir.obj_id = pdi.id
    AND pti.index_id = ptii.id
    AND opcdata.device = pdi.name
    AND opcdata.datatype = ptii.index_name
GROUP BY
    opcdata.device,
    opcdata.datatype,
    cid
ORDER BY
    opcdata.device,
    opcdata.datatype,
    cid;