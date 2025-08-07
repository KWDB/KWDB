set hash_scan_mode=3;
select
    max(opcdata.time),
    avg(opcdata.value),
    opcdata.adr,
    opcdata.channel,
    opcdata.companyid,
    opcdata.companyname,
    opcdata.datatype,
    opcdata.device,
    opcdata.deviceid,
    opcdata.highmorewarn,
    opcdata.highwarn,
    opcdata.lowmorewarn,
    opcdata.lowwarn,
    opcdata."name",
    opcdata.region,
    opcdata.slaveid,
    opcdata."tag",
    opcdata.unit,
    pzm.member_id,
    pzm.member_code,
    pzm.member_type,
    pzm.sys_investment_workplace_id,
    pzm.pic1_upload_time,
    pzm.pic2_url,
    pzm.company_type,
    pzm.is_virtual_member,
    pzm.new_taxpayer_month,
    pzm.effective_taxpayer_month,
    pzm.has_return_tax,
    pzm."level",
    pzm.leader_name,
    pzm.leader_tel,
    pzm.trade_type,
    pzm.move_in_type,
    pzm.expense_management_time,
    pzm.introduce_investment_people_id,
    pzm.company_belong,
    pzm.residence_id,
    pzm.passport_id,
    pzm.accountant
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
    pzm.member_id <> 386
    and ptir.company_id = '449600'
    and opcdata.channel <> 'CH248'
    and ptir.obj_id < 5000
    and opcdata.lowwarn = cast(ptir.obj_id as string)
    and opcdata.datatype = '液位'
    AND opcdata."time" BETWEEN '2024-01-1 00:00:00' AND '2024-01-1 01:00:00'
group by
    opcdata.adr,
    opcdata.channel,
    opcdata.companyid,
    opcdata.companyname,
    opcdata.datatype,
    opcdata.device,
    opcdata.deviceid,
    opcdata.highmorewarn,
    opcdata.highwarn,
    opcdata.lowmorewarn,
    opcdata.lowwarn,
    opcdata."name",
    opcdata.region,
    opcdata.slaveid,
    opcdata."tag",
    opcdata.unit,
    pzm.member_id,
    pzm.member_code,
    pzm.member_type,
    pzm.sys_investment_workplace_id,
    pzm.pic1_upload_time,
    pzm.pic2_url,
    pzm.company_type,
    pzm.is_virtual_member,
    pzm.new_taxpayer_month,
    pzm.effective_taxpayer_month,
    pzm.has_return_tax,
    pzm."level",
    pzm.leader_name,
    pzm.leader_tel,
    pzm.trade_type,
    pzm.move_in_type,
    pzm.expense_management_time,
    pzm.introduce_investment_people_id,
    pzm.company_belong,
    pzm.residence_id,
    pzm.passport_id,
    pzm.accountant
order by
    max(opcdata.time),
    pzm.member_id;