set hash_scan_mode=3;
SELECT
    *
FROM 
    runba.opcdata449600 opcdata,                     -- pk: 303, row: 98,074,131
    runba_tra.plat_tijiancard_item_relation ptir,    -- 281,024
    runba_tra.plat_tijian_item pti,                  -- 7,529,071
    runba_tra.plat_device_info pdi,                  -- 19,091
    runba_tra.plat_tijian_item_index ptii,            -- 7
    runba_tra.cd_security_device csd,
    runba_tra.cd_tijian_item_index ctii,
    runba_tra.plat_risk_analyse_objects prao
WHERE
    ptir."tijiancard_id" = '6VVkdjOLPOL1brZ8BBXQtvcRuMIfTfLD4uJyFFzbai5U7LywRF' -- 1/281024
    AND opcdata.companyid = ptir.company_id          -- 1/8
    AND opcdata.channel = 'CH193'
    AND (cast(pti.id as string) = ANY string_to_array(ptir.item_ids, ',')) -- 1/7529071, 1/2
    AND ptir.obj_id = pdi.id         -- 1/19092, 1/19091
    AND pti.index_id = ptii.id       -- 1/7, 1/7        
    AND opcdata.device = pdi.name    -- 1/53, 1/2
    AND opcdata.datatype = ptii.index_name    -- 1/4, 1/7
    AND opcdata."tag" = concat(substring(csd.device_no, 0, 5), '1101')
    AND opcdata.unit = ctii.index_unit
    AND opcdata.companyid = prao.company_id
    AND opcdata."time" BETWEEN '2024-06-29 23:58:00' AND '2024-06-29 23:59:00'
    AND prao.id > 319
order by
    opcdata.time,
    csd.device_info_id,
    prao.id;