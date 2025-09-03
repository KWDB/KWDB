drop database if exists tsdb cascade;
create ts database tsdb;
use tsdb;
create table nt(ts timestamp not null, name char(20), age int not null, height float) tags(ptag int not null) primary tags(ptag);

prepare p1 as insert into tsdb.nt values($1,$2,$3,$4,$5),('2023-08-16 08:51:14.623', $6,12,$7,$8),($9,'nick',29,$10,$11);
execute p1('2023-08-16 08:51:14.623','a',1,1.85,1, 'b',1.73,1, '2023-08-16 08:51:14.623',1.69,1);
execute p1(1692147075000,'a',1,1.85,1, 'b',1.73,1, 1692147075000,1.69,1);
execute p1('2023-08-16 08:51:14.623','a',1,1.85,1, 'b',1.73,1, '2023-08-16 08:51:14.623',1.69,1,'error');
select ts from nt order by ts;

prepare p2 as insert into nt values ($1,$2,$3,$4,$5),($6,$7,$8,$9,$10);
execute p2('2023-08-16 08:51:14.623','a',1,1.1,2, '2023-08-16 08:51:14.623','b',2,2.2,2);
select ts from nt order by ts;

prepare p3 as insert into nt(height, age, ts, name, ptag) values ($1,$2,'2023-08-16 08:51:14.623',$3,$4);
execute p3(1.71, 25, 'xiaohong',3);
select ts from nt order by ts;

prepare p4 as insert into nt values ($1,$2,$3,$4,$5);
execute p4('2023-08-16 08:51:14.623', 'a', null, 1.85, 4);
select ts from nt order by ts;

use defaultdb;
drop database if exists tsdb cascade;
create ts database tsdb;
use tsdb;
-- create table tsdb.nst(ts timestamp not null, e1 int2, e2 float4, e3 bool, e4 int8, e5 char(5), e6 nchar(5), e7 varchar(5), e8 nvarchar(5), e9 varbytes(5), e10 varbytes(5)) attributes (ptag int not null) primary tags(ptag);
-- prepare p5 as insert into tsdb.nst values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12);
--
-- --failed
-- execute p5 ('9023-10-27 12:13:14',111111111,1.1,true,111,'char','nchar','varchar','nvarchar','bytes','varbytes',1);
-- execute p5 ('2023-10-27 12:13:14',111111111,1.1,true,111,'char','nchar','varchar','nvarchar','bytes','varbytes',1);
-- execute p5 ('2023-10-27 12:13:14',1, 1e+40, true, 111,'charchar','nchar','varchar','nvarchar','bytes','varbytes',1);
-- execute p5 ('2023-10-27 12:13:14',1, 1.1, true, 111,'charchar','nchar','varchar','nvarchar','bytes','varbytes',1);
-- execute p5 ('2023-10-27 12:13:14',1, 1.1, true, 111,'char','ncharnchar','varchar','nvarchar','bytes','varbytes',1);
-- execute p5 ('2023-10-27 12:13:14',1, 1.1, true, 111,'char','nchar','varchar','nvarchar','bytes','varbytes',1);
-- execute p5 ('2023-10-27 12:13:14',1, 1.1, true, 111,'char','nchar','varch','nvarchar','bytes','varbytes',1);
-- execute p5 ('2023-10-27 12:13:14',1, 1.1, true, 111,'char','nchar','varch','nvarc','bytesbytes','varbytes',1);
-- execute p5 ('2023-10-27 12:13:14',1, 1.1, true, 111,'char','nchar','varch','nvarc','bytes','varbytes',1);
--
-- --succeed
-- execute p5 ('2023-10-27 12:13:14',1, 1.1, true, 111,'char','nchar','varch','nvarc','bytes','varby',1);

create table tsdb.tt(ts timestamp not null, f float4) tags(ptag int not null) primary tags(ptag);
prepare p6 as insert into tsdb.tt values($1,$2,1);
execute p6 ('2023-08-16 08:51:14.623', -2000.2021);

create table tsdb.test(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag);
prepare p7 as insert into test values($1,$2,1);
execute p7('2023-10-27 12:13:14', NULL);

create table tsdb.insertBool(ts timestamp not null, a bool) tags(ptag int not null) primary tags(ptag);
insert into tsdb.insertBool values('2023-10-27 12:13:14', 11111, 1);
prepare p8 as insert into tsdb.insertBool values($1,$2,1);
execute p8('2023-10-27 12:13:14', 11111);

use defaultdb;
drop database tsdb cascade;

--ZDP-36191 insert timestampTZ
drop database if exists test_insert2 cascade;
create ts database test_insert2;
create table test_insert2.t1(ts timestamptz not null,e1 timestamp,e2 int2)tags (tag1 int not null)primary tags(tag1);
prepare p9 as insert into test_insert2.t1 values($1,$2,$3,$4);
prepare p10 as delete from test_insert2.t1 where ts = $1;

-- time zone 0
execute p9('2022-02-02 11:11:11+00','2024-02-02 11:11:11+00', 1, 111);
execute p9('2022-02-02 11:11:12+00','2024-02-02 11:11:11+00', 2, 111);
execute p9('2022-02-02 11:11:13+00','2024-02-02 11:11:11+00', 3, 111);
-- expect 3 rows
select * from test_insert2.t1 order by ts;
-- time zone 8
set time zone 8;
execute p10('2022-02-02 11:11:11+00');
-- expect 2 rows
select * from test_insert2.t1 order by ts;
execute p9('2022-02-02 11:11:14+00','2024-02-02 12:11:11+00', 4, 222);
execute p9('2022-02-02 19:11:15','2024-02-02 12:11:11+00', 5, 222);
execute p9('2022-02-02 19:11:16','2024-02-02 12:11:11+00', 6, 222);
-- expect 5 rows
select * from test_insert2.t1 order by ts;
-- time zone 8
set time zone 1;
-- expect 5 rows
select * from test_insert2.t1 order by ts;
execute p10('2022-02-02 11:11:14+00');
execute p10('2022-02-02 12:11:12');
execute p10('2022-02-02 12:11:15');
-- expect 2 rows
select * from test_insert2.t1 order by ts;
delete from test_insert2.t1 where ts < '2022-02-02 12:11:14';
delete from test_insert2.t1 where ts > '2022-02-02 11:11:15+00';
-- expect 0 rows
select * from test_insert2.t1 order by ts;
drop table test_insert2.t1;
drop database if exists test_insert2 cascade;