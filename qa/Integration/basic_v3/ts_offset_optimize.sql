create ts database stbl;
use stbl;
CREATE TABLE stbl_raw (
        ts TIMESTAMPTZ NOT NULL,
        data VARCHAR(16374) NULL,
        type VARCHAR(10) NULL,
        parse VARCHAR(10) NULL
) TAGS (
        device VARCHAR(64) NOT NULL,
        iot_hub_name NCHAR(64) NOT NULL ) PRIMARY TAGS(device, iot_hub_name)
        activetime 1d;

-- all device
explain Select * from stbl.stbl_raw where ts>'2024-11-1' and ts<'2024-11-30' order by ts limit 20 offset 7200000;

-- one device
explain Select * from stbl.stbl_raw where ts>'2024-11-1' and ts<'2024-11-30'  and iot_hub_name='kaiwu_mqtt' order by ts desc limit 20 offset 7200000;

-- one device
explain Select * from stbl.stbl_raw where ts>'2024-11-1' and ts<'2024-11-30'  and device='1' and iot_hub_name='kaiwu_mqtt' order by ts limit 20 offset 7200000;


explain Select * from stbl.stbl_raw where ts>'2024-11-1' and ts<'2024-11-30'  and device='1' and iot_hub_name='kaiwu_mqtt' and length(data) < 10 order by ts limit 20 offset 7200000;

explain Select * from stbl.stbl_raw where ts>'2024-11-1' and ts<'2024-11-30'  and device='1' and iot_hub_name='kaiwu_mqtt' order by ts limit 2000 offset 7200000;


use defaultdb;

drop database stbl cascade;
