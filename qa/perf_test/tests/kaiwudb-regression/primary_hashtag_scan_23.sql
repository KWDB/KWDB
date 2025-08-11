-- Query 23: primary tag as join key with cast string
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
    AND opcdata.channel =  cast (chi.channel_id as varchar(20)) 
    AND chi.companyid = ptir.company_id 
    AND ptir."tijiancard_id" = '6VVkdjOLPOL1brZ8BBXQtvcRuMIfTfLD4uJyFFzbai5U7LywRF'
    AND chi.channel_id = 'CH193'
ORDER BY
    "time" DESC
LIMIT 1;

