-- Query 25: secondary tag as join key primary tags as filters
SELECT 
    "time",
    "value",
    "device",
    "datatype"
FROM 
    runba.opcdata449600 opcdata,
    runba_tra.plat_tijiancard_item_relation ptir,
    runba_tra.plat_device_info pdi,
    runba_tra.plat_tijian_item_index ptii,
    runba_tra.channel_info chi
WHERE
    ptir."tijiancard_id" = '6VVkdjOLPOL1brZ8BBXQtvcRuMIfTfLD4uJyFFzbai5U7LywRF'
    AND opcdata.companyid = 449600
    AND opcdata.channel =  concat('CH19','3')
    AND ptir.obj_id = pdi.id
    AND opcdata.device = pdi.name
    AND opcdata.datatype = ptii.index_name
    AND chi.channel_id = 'CH193'
ORDER BY
    "time" DESC
LIMIT 1;

