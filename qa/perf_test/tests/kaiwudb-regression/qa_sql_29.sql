SELECT 
    time_bucket(opcdata."time", '200s') AS timebucket,
    SUM(opcdata."value") / COUNT(opcdata."value") + MAX(opcdata."value") - MIN(opcdata."value") as avg_val,
    COUNT("device"),
    "datatype",
    COUNT(distinct ptir.company_id),
    AVG(pdi.check_period),
    MAX(pdi.area_id)
FROM 
    runba.opcdata449600 opcdata,
    runba_tra.plat_tijiancard_item_relation ptir,
    runba_tra.plat_tijian_item pti,
    runba_tra.plat_device_info pdi,
    runba_tra.plat_tijian_item_index ptii
WHERE
    ptir."tijiancard_id" = '6VVkdjOLPOL1brZ8BBXQtvcRuMIfTfLD4uJyFFzbai5U7LywRF'
    AND opcdata.companyid = ptir.company_id
    AND (cast(pti.id as string) = ANY string_to_array(ptir.item_ids, ','))
    AND ptir.obj_id = pdi.id
    AND pti.index_id = ptii.id
    AND opcdata.device = pdi.name
    AND opcdata.datatype = ptii.index_name
    AND opcdata."time" BETWEEN '2024-01-20 23:59:59' AND '2024-01-29 23:59:59'
    AND pti.create_user_id > 8000
GROUP BY
    opcdata.device,
    opcdata.datatype,
    ptir.company_id,
    timebucket
HAVING
    AVG(opcdata."value") > 60.0
ORDER BY
    opcdata.device,
    opcdata.datatype,
    timebucket;