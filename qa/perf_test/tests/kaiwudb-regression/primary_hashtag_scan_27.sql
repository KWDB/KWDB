-- Query 27: join key cast to a different compatible datatype 
SELECT
    "time",
    "value",
    "device",
    "datatype"
FROM
    runba_tra.channel_info chi,
    runba.opcdata449600 opcdata,
    runba_tra.plat_tijiancard_item_relation ptir
WHERE
    opcdata.companyid = chi.companyid
    AND opcdata.channel =  cast (chi.channel_id as char(5))
    AND chi.companyid = ptir.company_id
    AND ptir."tijiancard_id" = '6VVkdjOLPOL1brZ8BBXQtvcRuMIfTfLD4uJyFFzbai5U7LywRF'
    AND chi.channel_id = 'CH193'
ORDER BY
    "time" DESC
LIMIT 1;
