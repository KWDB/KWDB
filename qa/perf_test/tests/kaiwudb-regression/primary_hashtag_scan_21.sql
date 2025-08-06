-- Query 21: primary tags as join key with join on predicates
SELECT 
    "time",
    "value",
    "device",
    "datatype"
FROM 
    runba_tra.channel_info chi join   
    runba.opcdata449600 opcdata
    on chi.companyid = opcdata.companyid
    AND opcdata.channel =  chi.channel_id
    join 
    runba_tra.plat_tijiancard_item_relation ptir
    on chi.companyid = ptir.company_id 
WHERE
    ptir."tijiancard_id" = '6VVkdjOLPOL1brZ8BBXQtvcRuMIfTfLD4uJyFFzbai5U7LywRF'
    AND chi.channel_id = 'CH193'
ORDER BY
    "time" DESC
LIMIT 1;

