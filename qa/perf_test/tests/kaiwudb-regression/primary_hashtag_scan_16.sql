-- Query 16: primary & secondary tags as join key, partial primary tag as filter
SELECT
    opcdata.channel,
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
    AND opcdata.companyid = ptir.company_id
    AND opcdata.channel =  chi.channel_id
    AND ptir.obj_id = pdi.id
    AND chi.channel_id = 'CH193'
    AND opcdata.device = pdi.name
    AND opcdata.datatype = ptii.index_name
ORDER BY
    opcdata.channel, "time" DESC
limit 10
;

