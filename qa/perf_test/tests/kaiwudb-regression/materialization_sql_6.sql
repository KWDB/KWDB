set hash_scan_mode=3;
select
    concat('"', cdp.adr, '"') as address,
    opcdata."time",
    opcdata.datatype,
    opcdata.lowwarn,
    opcdata.channel,
    count(*)
from 
    runba_tra.cd_device_point cdp
    join runba_tra.plat_risk_analyse_objects prao
    on cdp.company_id = prao.company_id
    join runba_tra.plat_zone_member pzm
    on prao.company_id = pzm.company_id
    join runba_tra.plat_tijiancard_item_relation ptir
    on pzm.company_id = ptir.company_id
    join runba.opcdata449600 opcdata
    on ptir.company_id = opcdata.companyid
where
    opcdata.channel <> 'CH17'
    and opcdata.lowwarn = cast(ptir.obj_id as string)
    and opcdata.datatype = '液位'
    AND opcdata."time" BETWEEN '2024-01-29 23:58:00' AND '2024-01-29 23:59:00'
group by
    cdp.adr,
    opcdata."time",
    opcdata.datatype,
    opcdata.lowwarn,
    opcdata.channel
order by
    opcdata."time",
    opcdata.lowwarn,
    opcdata.channel;