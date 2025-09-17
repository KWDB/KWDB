create ts database tsdb;
use tsdb;

create table tsdb.t1(ts timestamp not null, a VARCHAR(10) NULL) tags(b int not null) primary tags(b);
insert into tsdb.t1 values(now(),'aaaa',1);

select
    ref_0.a as c0
from
    tsdb.t1 as ref_0
where pg_catalog.timeofday() LIKE cast(coalesce(
        pg_catalog.experimental_strftime(pg_catalog.transaction_timestamp(), cast(null as text))
    ,
        pg_catalog.version()) as text)
    limit 73;

drop database tsdb cascade;