-- Query 26: secondary tag as join key primary tags with nonequal predicate
-- fallback case, since primary hash index cannot be used
SELECT 
    "time",
    "value",
    "device",
    "datatype"
FROM 
    runba.opcdata449600 opcdata,
    runba_tra.plat_tijiancard_item_relation ptir,
    runba_tra.plat_device_info pdi
WHERE
    ptir."tijiancard_id" = '6VVkdjOLPOL1brZ8BBXQtvcRuMIfTfLD4uJyFFzbai5U7LywRF'
    AND opcdata.companyid >= 449600
    AND opcdata.channel =  concat('CH19','3')
    AND ptir.obj_id = pdi.id
    AND opcdata.device = pdi.name
ORDER BY
    "time" DESC
LIMIT 1;
