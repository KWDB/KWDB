-- Query 24: one primary tag as join key, one as filter with cast
-- no primary hashtag scan, since filter is not part of join key
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
    AND opcdata.channel =  cast ('CH1934' as char(5)) 
    AND chi.companyid = ptir.company_id
    AND chi.channel_id = 'CH193'
    AND ptir."tijiancard_id" = '6VVkdjOLPOL1brZ8BBXQtvcRuMIfTfLD4uJyFFzbai5U7LywRF'
ORDER BY
    "time" DESC
LIMIT 1;
