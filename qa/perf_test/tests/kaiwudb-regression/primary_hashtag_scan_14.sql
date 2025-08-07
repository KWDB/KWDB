-- Query 14: primary tag multiple columns, one join key, one filter key
SELECT 
    "time",
    "value",
    "device",
    "datatype"
FROM 
    runba.opcdata449600 opcdata,
    runba_tra.plat_tijiancard_item_relation ptir,
    runba_tra.plat_tijian_item pti,
    runba_tra.plat_device_info pdi,
    runba_tra.plat_tijian_item_index ptii
WHERE
    ptir."tijiancard_id" = '6VVkdjOLPOL1brZ8BBXQtvcRuMIfTfLD4uJyFFzbai5U7LywRF' -- 1/281024
    AND opcdata.companyid = ptir.company_id   
    AND opcdata.channel =  'CH17'
    AND (cast(pti.id as string) = ANY string_to_array(ptir.item_ids, ',')) -- 1/7529071, 1/2
    AND ptir.obj_id = pdi.id
    AND pti.index_id = ptii.id       
ORDER BY
    "time" DESC
LIMIT 1;
