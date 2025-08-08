set hash_scan_mode=3;
select
    sum(ts_t."value"),
    count(*),
    rel_t.company_id
from
    ((select
        t3.company_id as "company_id",
        t3.tijiancard_id as "tijiancard_id",
        t3.obj_type as "obj_type",
        t3.obj_id as "obj_id",
        t3.name as "name",
        t3.devicenumber as "devicenumber",
        t4.company_id as "company_id_1",
        t4.tijiancard_id as "tijiancard_id_1",
        t4.obj_type as "obj_type_1",
        t4.obj_id as "obj_id_1"
    from
        (select
            * 
        from
            runba_tra.plat_tijiancard_item_relation t1,
            runba_tra.plat_device_info t2
        where
            t1.obj_id = t2.id
        ) t3,
        (select * from runba_tra.plat_tijiancard_item_relation where company_id % 10 = 1 limit 100) t4
    where
        t3.company_id = t4.company_id)
    union
    (select
        t3.company_id as "company_id",
        t3.tijiancard_id as "tijiancard_id",
        t3.obj_type as "obj_type",
        t3.obj_id as "obj_id",
        t3.name as "name",
        t3.devicenumber as "devicenumber",
        t4.company_id as "company_id_1",
        t4.tijiancard_id as "tijiancard_id_1",
        t4.obj_type as "obj_type_1",
        t4.obj_id as "obj_id_1"
    from
        (select
            * 
        from
            runba_tra.plat_tijiancard_item_relation t1,
            runba_tra.plat_device_info t2
        where
            t1.obj_id = t2.id
        ) t3,
        (select * from runba_tra.plat_tijiancard_item_relation where company_id % 100 = 0 limit 1) t4
    where
        t3.company_id = t4.company_id
    limit 1)
    ) rel_t,
    runba.opcdata449600 ts_t
where
    rel_t.company_id = ts_t.companyid
group by
    rel_t.company_id;