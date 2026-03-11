create ts database d1;
use d1;

--- delete time-series table tag
create table t4(ts timestamp not null ,a int) tags( b int not null, c char(2) not null, d bool not null, e int) primary tags(b, c, d);
create table t5(ts timestamp not null ,a int) tags( b int not null, c char(2) not null, d bool not null, e int) primary tags(b, c, d);

insert into t4 values(1672531200000,1,1,'k', false, 1), (1672532200000,1,1,'k', false, 2), (1672533200000,1,1,'k', false, 3), (1672534200000,1,1,'k', false, 4), (1672535200000,1,1,'k', false, 5);
insert into t4 values(1673531300000,1,2,'k', false, 1), (1673532300000,1,2,'k', false, 2), (1673533300000,1,2,'k', false, 3), (1673534300000,1,2,'k', false, 4), (1673535300000,1,2,'k', false, 5);
insert into t4 values(1674531400000,1,1,'m', false, 1), (1674532400000,1,1,'m', false, 2), (1674533400000,1,1,'m', false, 3), (1674534400000,1,1,'m', false, 4), (1674535400000,1,1,'m', false, 5);
insert into t4 values(1675531500000,1,1,'k', true, 1),  (1675532500000,1,1,'k', true, 2),  (1675533500000,1,1,'k', true, 3),  (1675534500000,1,1,'k', true, 4),  (1675535500000,1,1,'k', true, 5);
insert into t4 values(1676531600000,1,1,'1', false, 1), (1676532600000,1,1,'1', false, 2), (1676533600000,1,1,'1', false, 3), (1676534600000,1,1,'1', false, 4), (1676535600000,1,1,'1', false, 5);

insert into t5 values(1672531200000,1,1,'k', false, 1), (1672532200000,1,1,'k', false, 2), (1672533200000,1,1,'k', false, 3), (1672534200000,1,1,'k', false, 4), (1672535200000,1,1,'k', false, 5);
insert into t5 values(1673531300000,1,2,'k', false, 1), (1673532300000,1,2,'k', false, 2), (1673533300000,1,2,'k', false, 3), (1673534300000,1,2,'k', false, 4), (1673535300000,1,2,'k', false, 5);
insert into t5 values(1674531400000,1,1,'m', false, 1), (1674532400000,1,1,'m', false, 2), (1674533400000,1,1,'m', false, 3), (1674534400000,1,1,'m', false, 4), (1674535400000,1,1,'m', false, 5);
insert into t5 values(1675531500000,1,1,'k', true, 1),  (1675532500000,1,1,'k', true, 2),  (1675533500000,1,1,'k', true, 3),  (1675534500000,1,1,'k', true, 4),  (1675535500000,1,1,'k', true, 5);
insert into t5 values(1676531600000,1,1,'1', false, 1), (1676532600000,1,1,'1', false, 2), (1676533600000,1,1,'1', false, 3), (1676534600000,1,1,'1', false, 4), (1676535600000,1,1,'1', false, 5);

select * from t4 order by ts;
delete from t4 where b = '2';
select * from t4 order by ts;
delete from t4 where c ='m';
select * from t4 order by ts;
delete from t4 where d = true;
select * from t4 order by ts;
delete from t4 where b = 1 and c ='1' or b = 1 and c ='1' ;
select * from t4 order by ts;
delete from t4 where (b = 1 and c ='1') or (b = 1 and c ='1');
select * from t4 order by ts;
delete from t4 where (b = 1 and c ='k') and (b = 1 and c ='k');

insert into t5 values(1672531200000,1,2,'k', false, 1),
select * from t5 where b = 2 and false order by ts;
delete from t5 where b = 2 and false;
select * from t5 where b = 2 and false order by ts;

select * from t5 where b = 1 and c ='k' and ts < '2023-01-01 08:00:00' order by ts;
delete from t5 where b = 1 and c ='k' and ts < '2023-01-01 08:00:00';
select * from t5 where b = 1 and c ='k' and ts < '2023-01-01 08:00:00' order by ts;

insert into t5 values('2023-01-01 08:00:00',1,1,'k', false, 1);
select * from t5 where b = 1 and c ='k' and ts = '2023-01-01 08:00:00' order by ts;
delete from t5 where b = 1 and c ='k' and ts = '2023-01-01 08:00:00';
select * from t5 where b = 1 and c ='k' and ts = '2023-01-01 08:00:00' order by ts;

insert into t5 values('2023-01-01 08:00:00',1,1,'k', false, 1);
select * from t5 where b = 1 and c ='k' and ts <= '2023-01-01 08:16:40' order by ts;
delete from t5 where b = 1 and c ='k' and ts <= '2023-01-01 08:16:40';
select * from t5 where b = 1 and c ='k' and ts <= '2023-01-01 08:16:40' order by ts;

insert into t5 values('2023-10-01 08:00:00',1,1,'1', false, 1);
select * from t5 where b = 1 and c ='1' and ts > '2023-02-16 16:20:00' order by ts;
delete from t5 where b = 1 and c ='1' and ts > '2023-02-16 16:20:00';
select * from t5 where b = 1 and c ='1' and ts > '2023-02-16 16:20:00' order by ts;

insert into t5 values('2023-10-01 08:00:00',1,1,'1', false, 1);
select * from t5 where b = 1 and c ='1' and ts >= '2023-02-16 16:20:00' order by ts;
delete from t5 where b = 1 and c ='1' and ts >= '2023-02-16 16:20:00';
select * from t5 where b = 1 and c ='1' and ts >= '2023-02-16 16:20:00' order by ts;

insert into t5 values('2023-01-01 09:06:40',1,1,'k', false, 1),('2023-01-01 05:06:40',1,1,'k', false, 1);
select * from t5 where b = 1 and c ='k' and ts < '2023-01-01 08:00:00' or b = 1 and c ='k' and ts <= '2023-01-01 09:06:40' order by ts;
delete from t5 where b = 1 and c ='k' and ts < '2023-01-01 08:00:00' or b = 1 and c ='k' and ts <= '2023-01-01 09:06:40';
select * from t5 where b = 1 and c ='k' and ts < '2023-01-01 08:00:00' or b = 1 and c ='k' and ts <= '2023-01-01 09:06:40' order by ts;

insert into t5 values('2023-05-01 09:06:40',1,1,'k', false, 1),('2023-02-16 16:20:00',1,1,'k', false, 1);
select * from t5 where b = 1 and c ='k' and ts > '2023-02-16 16:20:00' or b = 1 and c ='k' and ts >= '2023-02-16 16:20:00' order by ts;
delete from t5 where b = 1 and c ='k' and ts > '2023-02-16 16:20:00' or b = 1 and c ='k' and ts >= '2023-02-16 16:20:00';
select * from t5 where b = 1 and c ='k' and ts > '2023-02-16 16:20:00' or b = 1 and c ='k' and ts >= '2023-02-16 16:20:00' order by ts;

insert into t5 values('2023-01-12 21:48:20',1,2,'1', false, 1),('2023-01-12 22:55:00',1,2,'1', false, 1);
select * from t5 where (b = 2 and c ='1' and ts >= '2023-01-12 21:48:20') and (b = 2 and c ='1' and ts <= '2023-01-12 22:55:00') order by ts;
delete from t5 where (b = 2 and c ='1' and ts >= '2023-01-12 21:48:20') and (b = 2 and c ='1' and ts <= '2023-01-12 22:55:00');
select * from t5 where (b = 2 and c ='1' and ts >= '2023-01-12 21:48:20') and (b = 2 and c ='1' and ts <= '2023-01-12 22:55:00') order by ts;

select * from t5 where d = true and ts != '2023-02-16 16:20:00' order by ts;
delete from t5 where d = true and ts != '2023-02-16 16:20:00';
select * from t5 where d = true and ts != '2023-02-16 16:20:00' order by ts;

create table t6(ts timestamp not null, a int) tags(b int not null, c varchar(20) not null) primary tags(b , c);

insert into t6 values(1672576500000, 1, 1, 'abc');
insert into t6 values(1672876500000, 1, 1, 'abc');
insert into t6 values(1672876600000, 1, 1, 'abc');
insert into t6 values(1672876700000, 1, 1, 'abc');
insert into t6 values(1672876800000, 0, 1, 'abc');---2023-01-05 8:00:00
insert into t6 values(1673308500000, 2, 1, 'abc');
insert into t6 values(1673308600000, 2, 1, 'abc');
insert into t6 values(1673308700000, 2, 1, 'abc');
insert into t6 values(1673308800000, 0, 1, 'abc');---2023-01-10 8:00:00
insert into t6 values(1673408800000, 3, 1, 'abc');
insert into t6 values(1673508800000, 3, 1, 'abc');
insert into t6 values(1673608800000, 3, 1, 'abc');
insert into t6 values(1674000000000, 0, 1, 'abc');---2023-01-18 8:00:00
insert into t6 values(1674152800000, 4, 1, 'abc');
insert into t6 values(1674162800000, 4, 1, 'abc');
insert into t6 values(1674171800000, 4, 1, 'abc');
insert into t6 values(1674172800000, 0, 1, 'abc');---2023-01-20 8:00:00
insert into t6 values(1674272800000, 0, 1, 'abc');

select * from t6 where ts < '2023-01-05 7:55:00' or b = 1 order by ts;
delete from t6 where ts < '2023-01-05 7:55:00' or b = 1;
delete from t6 where ts < '2023-01-05 7:55:00' and b = 1;
select * from t6 order by ts;
delete from t6 where ts <= '2023-01-05 7:55:00' and b = 1;
select * from t6 order by ts;
delete from t6 where ts > '2023-01-21 11:46:40' and b = 1;
select * from t6 order by ts;
delete from t6 where ts >= '2023-01-21 11:46:40'and b = 1;
select * from t6 order by ts;

delete from t6 where ts < '2023-01-21 11:46:40' and ts < '2023-01-05 7:55:00' and b = 1;
select * from t6 order by ts;
delete from t6 where ts < '2023-01-21 11:46:40' and b = 1 and ts <= '2023-01-05 7:55:00' and b = 1;
select * from t6 order by ts;
delete from t6 where ts > '2023-01-21 11:46:40' and b = 1 and ts > '2023-01-05 7:55:00' and b = 1;
select * from t6 order by ts;
delete from t6 where ts > '2023-01-21 11:46:40' and ts >= '2023-01-05 7:55:00' and b = 1;
select * from t6 order by ts;
delete from t6 where ts > '2023-01-21 11:46:40' and ts < '2023-01-05 7:55:00' and b = 1;
select * from t6 order by ts;
delete from t6 where ts >= '2023-01-21 11:46:40' and b = 1 and ts <= '2023-01-05 7:55:00';
select * from t6 order by ts;
delete from t6 where b = 1 and ts < '2023-01-10 08:00:00' and ts > '2023-01-10 07:56:40';
select * from t6 order by ts;
delete from t6 where ts <= '2023-01-10 08:00:00' and ts >= '2023-01-10 07:56:40' and b = 1;
select * from t6 order by ts;

delete from t6 where ts < '2023-01-06 11:46:40' or ts < '2023-01-05 7:55:00' and b = 1;
select * from t6 order by ts;
delete from t6 where ts < '2023-01-06 11:46:40' and b = 1 or ts <= '2023-01-05 7:55:00' and b = 1;
select * from t6 order by ts;
delete from t6 where ts > '2023-01-21 11:46:40' or ts > '2023-01-20 7:55:00' and b = 1;
select * from t6 order by ts;
delete from t6 where ts > '2023-01-21 11:46:40' and b = 1 or ts >= '2023-01-20 7:55:00' and b = 1;
select * from t6 order by ts;
delete from t6 where ts > '2023-01-21 11:46:40' and b = 12 or ts < '2023-01-10 7:55:00' and b = 1;
select * from t6 order by ts;
delete from t6 where ts >= '2023-01-21 11:46:40' and b = 1 or ts <= '2023-01-05 7:55:00' and b = 1;
select * from t6 order by ts;
delete from t6 where ts < '2023-01-10 08:00:00' and b = 1 or ts > '2023-01-10 07:56:40' and b = 1;
select * from t6 order by ts;
delete from t6 where ts <= '2023-01-10 08:00:00' and b = 1 or ts >= '2023-01-10 07:56:40' and b = 1;
select * from t6 order by ts;
insert into t6 values(1672876800000, 0, 1, 'abc');
delete from t6 where ts = '2023-01-05 08:00:00' and b = 1;
delete from t6 where false;
select * from t6 order by ts;
delete from t6 where (ts <= '2023-01-10 08:00:00' and ts >= '2023-01-09 07:56:40' and b = 1) or
    (ts <= '2023-01-05 08:00:00' and ts >= '2023-01-04 07:56:40' and b = 1) or
    (ts >= '2023-01-20 07:56:40' and b = 1);
select * from t6 order by ts;
delete from t6 where (ts <= '2023-01-09 08:00:00' or ts >= '2023-01-10 08:56:40' and b = 1) and
    (ts <= '2023-01-04 08:00:00' or ts >= '2023-01-05 08:56:40') and
    (ts <= '2023-01-18 08:00:00');
select * from t6 order by ts;
delete from t6 where ts != '2023-01-05 08:00:00' and b = 1;
select * from t6 order by ts;
delete from t6 where true;
select * from t6 order by ts;

--- add operator in, not in
insert into t6 values(1672576500000, 1, 1, 'abc');
insert into t6 values(1672576600000, 1, 1, 'abc');
insert into t6 values(1672576700000, 1, 1, 'abc');
insert into t6 values(1672576800000, 1, 1, 'abc');
insert into t6 values(1672576900000, 1, 1, 'abc');
insert into t6 values(1672577000000, 1, 1, 'abc');
insert into t6 values(1672577100000, 1, 1, 'abc');
delete from t6 where ts in ('2023-01-05 12:35:00') or b = 1;
delete from t6 where ts in ('ABC') or b = 1;
delete from t6 where ts >'2023-01-01 12:45:00' and b in (1, 2);
select ts from t6 order by ts;
delete from t6 where ts in ('2023-01-05 12:55:00', '2023-01-01 12:40:00', '2023-01-01 12:45:00') and b = 1;
select ts from t6 order by ts;
delete from t6 where ts not in ('2023-01-05 12:35:00', '2023-01-01 12:40:00', '2023-01-01 12:45:00') and b = 1;
select ts from t6 order by ts;
insert into t6 values(1672576500000, 1, 1, 'abc');
insert into t6 values(1672576600000, 1, 1, 'abc');
insert into t6 values(1672576700000, 1, 1, 'abc');
insert into t6 values(1672576800000, 1, 1, 'abc');
insert into t6 values(1672576900000, 1, 1, 'abc');
insert into t6 values(1672577000000, 1, 1, 'abc');
insert into t6 values(1672577100000, 1, 1, 'abc');
delete from t6 where ts not in ('2023-01-05 12:35:00', '2023-01-01 12:40:00', '2023-01-01 12:45:00') and b = 1;
select ts from t6 order by ts;
delete from t6 where ts in ('2023-01-05 12:55:00', '2023-01-01 12:40:00', '2023-01-01 12:45:00') and b = 1;
select ts from t6 order by ts;
insert into t6 values(1672577100000, 1, 1, 'abc');
delete from t6 where ts not in ('2023-01-05 12:55:00', null, '2023-01-01 12:40:00', '2023-01-01 12:45:00') and b = 1;
delete from t6 where ts in ('2023-01-05 12:55:00', null, '2023-01-01 12:40:00', '2023-01-01 12:45:00') and b = 1;
insert into t6 values(1672577100000, 1, 1, 'abc');
prepare p28 as delete from t6 where ts not in ($1, $2) and b = 1;
execute p28('2023-01-05 12:35:00', '2023-01-01 12:40:00');

create table t7(ts timestamp not null, a int) tags(b int2 not null, c int2 not null) primary tags(b, c);
create table t8(ts timestamp not null, a int) tags(b int4 not null, c int4 not null) primary tags(b, c);
create table t9(ts timestamp not null, a int) tags(b int8 not null, c int8 not null) primary tags(b, c);
insert into t7 values(0, 0, 0, 0),(0, 1, 32767, 32767),(0, 2, -32768, -32768);
insert into t8 values(0, 0, 0, 0),(0, 1, 2147483647, 2147483647),(0, 2, -2147483648, -2147483648);
insert into t9 values(0, 0, 0, 0),(0, 1, 9223372036854775807, 9223372036854775807),(0, 2, -9223372036854775808, -9223372036854775808);
delete from t7 where b = 32768;
delete from t7 where b = -32769;
delete from t7 where b = 32767;
delete from t7 where b = -32768;
delete from t8 where b = 2147483648;
delete from t8 where b = -2147483649;
delete from t8 where b = 2147483647;
delete from t8 where b = -2147483648;
delete from t9 where b = 9223372036854775808;
delete from t9 where b = -922337203685477580;
delete from t9 where b = 9223372036854775807;
delete from t9 where b = -9223372036854775808;

create table t11(ts timestamp not null ,a int) tags( b int2 not null, c int2 not null) primary tags(b, c);
insert into t11 values('2024-07-03 00:00:00', 1, 1, 1);
alter table t11 add column d int;
delete from t11 where b = 1;
insert into t11 values('2024-07-03 00:00:00', 1, 1, 1, 1);
alter table t11 drop column a;
delete from t11 where b = 1;

create table t10(ts timestamp not null ,a int) tags( b int2 not null, c int2 not null) primary tags(b, c);
insert into t10 values('0000-01-01 00:00:00', 1, 1, 1);
insert into t10 values('0001-01-01 00:00:00', 1, 1, 1);
insert into t10 values('1000-01-01 00:00:00', 1, 1, 1);
insert into t10 values('1001-01-01 00:00:00', 1, 1, 1);
insert into t10 values('1969-12-31 23:59:59.999', 1, 1, 1);
insert into t10 values('1970-01-01 00:00:00', 1, 1, 1);
insert into t10 values('2000-01-01 00:00:00', 1, 1, 1);
insert into t10 values('2024-04-24 00:00:00', 1, 1, 1);
insert into t10 values('2970-01-01 00:00:00', 1, 1, 1);

select ts from t10 order by ts;
delete from t10 where b = 1 and ts in ('-0001-01-01 00:00:00');
delete from t10 where b = 1 and ts < '0000-01-01 00:00:00';
delete from t10 where b = 1 and ts <= '0001-01-01 00:00:00';
delete from t10 where b = 1 and (ts > '0000-01-01 00:00:00' and ts < '1000-01-01 00:00:00');
delete from t10 where b = 1 and ts = '1001-01-01 00:00:00';
delete from t10 where b = 1 and ('1969-12-31 23:59:59.999' <= ts and '1970-01-01 00:00:00' >= ts);
delete from t10 where b = 1 and (ts < '-0001-01-01 00:00:00' or ts > '2971-01-01 00:00:00');
delete from t10 where b = 1 and (ts > '-0001-01-01 00:00:00' or ts < '2971-01-01 00:00:00');

insert into t10 values('0000-01-01 00:00:00', 1, 1, 1);
insert into t10 values('0001-01-01 00:00:00', 1, 1, 1);
insert into t10 values('1000-01-01 00:00:00', 1, 1, 1);
insert into t10 values('1001-01-01 00:00:00', 1, 1, 1);
insert into t10 values('1969-12-31 23:59:59.999', 1, 1, 1);
insert into t10 values('1970-01-01 00:00:00', 1, 1, 1);
insert into t10 values('2000-01-01 00:00:00', 1, 1, 1);
insert into t10 values('2024-04-24 00:00:00', 1, 1, 1);
insert into t10 values('2970-01-01 00:00:00', 1, 1, 1);
select ts from t10 order by ts;
delete from t10 where b = 1 and ts in ('0000-01-01 00:00:00', '1969-12-31 23:59:59.999', '2024-04-24 00:00:00');
delete from t10 where b = 1 and ts not in ('1000-01-01 00:00:00', '1970-01-01 00:00:00', '2970-01-01 00:00:00');
delete from t10 where b = 1 and ts not in ('-1000-01-01 00:00:00', '3070-01-01 00:00:00');

insert into t10 values('0000-01-01 00:00:00', 1, 1, 1);
insert into t10 values('0001-01-01 00:00:00', 1, 1, 1);
insert into t10 values('1000-01-01 00:00:00', 1, 1, 1);
insert into t10 values('1001-01-01 00:00:00', 1, 1, 1);
insert into t10 values('1969-12-31 23:59:59.999', 1, 1, 1);
insert into t10 values('1970-01-01 00:00:00', 1, 1, 1);
insert into t10 values('2000-01-01 00:00:00', 1, 1, 1);
insert into t10 values('2024-04-24 00:00:00', 1, 1, 1);
insert into t10 values('2970-01-01 00:00:00', 1, 1, 1);
select ts from t10 order by ts;
prepare pre_1 as delete from t10 where b = 1 and ts < $1;
prepare pre_2 as delete from t10 where b = 1 and ts > $1 and ts < $2;
prepare pre_3 as delete from t10 where b = 1 and ts in ($1, $2);
prepare pre_4 as delete from t10 where b = 1 and ts not in ($1, $2);
execute pre_1('1960-1-1 08:00:14');
execute pre_2('1960-1-1 08:00:14', '2024-1-1 08:00:16');
execute pre_3('1960-1-1 08:00:14', '2024-04-24 00:00:00');
execute pre_4('-0001-01-01 00:00:00', '2971-01-01 00:00:00');

drop database d1 cascade;


--- test prepare delete
create ts database d1;
use d1;

--- delete time-series table tag
create table t4(ts timestamp not null ,a int) tags( b int not null, c char(2) not null, d bool not null, e int) primary tags(b, c, d);
create table t5(ts timestamp not null ,a int) tags( b int not null, c char(2) not null, d bool not null, e int) primary tags(b, c, d);

insert into t4 values(1672531200000,1,1,'k', false, 1), (1672532200000,1,1,'k', false, 2), (1672533200000,1,1,'k', false, 3), (1672534200000,1,1,'k', false, 4), (1672535200000,1,1,'k', false, 5);
insert into t4 values(1673531300000,1,2,'k', false, 1), (1673532300000,1,2,'k', false, 2), (1673533300000,1,2,'k', false, 3), (1673534300000,1,2,'k', false, 4), (1673535300000,1,2,'k', false, 5);
insert into t4 values(1674531400000,1,1,'m', false, 1), (1674532400000,1,1,'m', false, 2), (1674533400000,1,1,'m', false, 3), (1674534400000,1,1,'m', false, 4), (1674535400000,1,1,'m', false, 5);
insert into t4 values(1675531500000,1,1,'k', true, 1),  (1675532500000,1,1,'k', true, 2),  (1675533500000,1,1,'k', true, 3),  (1675534500000,1,1,'k', true, 4),  (1675535500000,1,1,'k', true, 5);
insert into t4 values(1676531600000,1,1,'1', false, 1), (1676532600000,1,1,'1', false, 2), (1676533600000,1,1,'1', false, 3), (1676534600000,1,1,'1', false, 4), (1676535600000,1,1,'1', false, 5);

insert into t5 values(1672531200000,1,1,'k', false, 1), (1672532200000,1,1,'k', false, 2), (1672533200000,1,1,'k', false, 3), (1672534200000,1,1,'k', false, 4), (1672535200000,1,1,'k', false, 5);
insert into t5 values(1673531300000,1,2,'k', false, 1), (1673532300000,1,2,'k', false, 2), (1673533300000,1,2,'k', false, 3), (1673534300000,1,2,'k', false, 4), (1673535300000,1,2,'k', false, 5);
insert into t5 values(1674531400000,1,1,'m', false, 1), (1674532400000,1,1,'m', false, 2), (1674533400000,1,1,'m', false, 3), (1674534400000,1,1,'m', false, 4), (1674535400000,1,1,'m', false, 5);
insert into t5 values(1675531500000,1,1,'k', true, 1),  (1675532500000,1,1,'k', true, 2),  (1675533500000,1,1,'k', true, 3),  (1675534500000,1,1,'k', true, 4),  (1675535500000,1,1,'k', true, 5);
insert into t5 values(1676531600000,1,1,'1', false, 1), (1676532600000,1,1,'1', false, 2), (1676533600000,1,1,'1', false, 3), (1676534600000,1,1,'1', false, 4), (1676535600000,1,1,'1', false, 5);

select * from t4 order by ts;
prepare p3 as delete from t4 where b = $1 and c =$2;
execute p3(1,'m');
select * from t4 order by ts;
execute p3('2','k');
select * from t4 order by ts;
execute p3(1,'k');
select * from t4 order by ts;
prepare p4 as delete from t4 where b = $1 and c =$2 or b = $4 and c = $5;


prepare p5 as delete from t5 where b = $1 and c =$2 and false;
execute p5(1,'k');
prepare p6 as delete from t5 where b = $1 and c = $2 and ts < $3;
execute p6(1,'k','2023-01-01 08:00:00');
prepare p7 as delete from t5 where b = $1 and c = $2 and ts = $3;
execute p7(1,'k','2023-01-01 08:00:00');
prepare p8 as delete from t5 where b = $1 and c = $2 and ts <= $3;
execute p8(1,'k','2023-01-01 08:16:40');
prepare p9 as delete from t5 where b = $1 and c = $2 and ts > $3;
execute p9(1,'1','2023-02-16 16:20:00');
prepare p10 as delete from t5 where b = $1 and c = $2 and ts >= $3;
execute p10(1,'1','2023-02-16 16:20:00');
prepare p11 as delete from t5 where b = $1 and c = $2 and ts != $3;
execute p11(1,'k','2023-02-16 16:20:00');

create table t7(ts timestamp not null, a int) tags(b int2 not null, c int2 not null) primary tags(b, c);
create table t8(ts timestamp not null, a int) tags(b int4 not null, c int4 not null) primary tags(b, c);
create table t9(ts timestamp not null, a int) tags(b int8 not null, c int8 not null) primary tags(b, c);
insert into t7 values(0, 0, 0, 0),(0, 1, 32767, 32767),(0, 2, -32768, -32768);
insert into t8 values(0, 0, 0, 0),(0, 1, 2147483647, 2147483647),(0, 2, -2147483648, -2147483648);
insert into t9 values(0, 0, 0, 0),(0, 1, 9223372036854775807, 9223372036854775807),(0, 2, -9223372036854775808, -9223372036854775808);
prepare p23 as delete from t7 where b = $1;
prepare p24 as delete from t8 where b = $1;
prepare p25 as delete from t9 where b = $1;

execute p23(32768);
execute p23(-32769);
execute p23(32767);
execute p23(-32768);
execute p24(2147483648);
execute p24(-2147483649);
execute p24(2147483647);
execute p24(-2147483648);
execute p25(9223372036854775808);
execute p25(-922337203685477580);
execute p25(9223372036854775807);
execute p25(-9223372036854775808);
drop database d1 cascade;

create ts database test_delete1;
create table test_delete1.t1(ts timestamptz not null, e1 nchar, e2 nchar(64))  tags(tag1 nchar not null, tag2 nchar(64) not null) primary tags(tag1, tag2);
prepare p26 as delete from test_delete1.t1 where ts=$1 and tag1 ='a';
prepare p27 as insert into test_delete1.t1 values($1,'a','a','a','a');
execute p27(1);
execute p26(1);
drop database test_delete1;

set sql_safe_updates = false;

DROP DATABASE IF EXISTS tsdba CASCADE;
create ts database tsdba;

create table tsdba.t2 (k_timestamp timestamp not null,e1 int2  not null,e2 int,e3 int8 not null,e4 float4,e5 float8 not null,e6 bool,e7 double not null) TAGS (code1 int2 not null, code2 varchar(128) not null)primary tags(code1, code2);

insert into tsdba.t2 values('2021-03-01 15:00:00',1,2,3,1.1,1.2,true,3.33333,1,'《test1中文YingYu@!!!》');
insert into tsdba.t2 values('2021-03-02 15:00:00',1,2,3,1.1,1.2,true,3.33333,1,'《test1中文YingYu@!!!》');
insert into tsdba.t2 values('2021-03-03 15:00:00',1,2,3,1.1,1.2,true,3.33333,1,'《test1中文YingYu@!!!》');
insert into tsdba.t2 values('2021-03-04 15:00:00',1,2,3,1.1,1.2,true,3.33333,1,'《test1中文YingYu@!!!》');

insert into tsdba.t2 values('2021-03-02 15:00:00',1,2,3,1.1,1.2,true,3.33333,2,'《test1中文YingYu@!!!》');
insert into tsdba.t2 values('2021-03-03 15:00:00',1,2,3,1.1,1.2,true,3.33333,2,'《test1中文YingYu@!!!》');
insert into tsdba.t2 values('2021-03-04 15:00:00',1,2,3,1.1,1.2,true,3.33333,2,'《test1中文YingYu@!!!》');
insert into tsdba.t2 values('2021-03-05 15:00:00',1,2,3,1.1,1.2,true,3.33333,2,'《test1中文YingYu@!!!》');

insert into tsdba.t2 values('2021-03-03 15:00:00',1,2,3,1.1,1.2,true,3.33333,3,'《test1中文YingYu@!!!》');
insert into tsdba.t2 values('2021-03-04 15:00:00',1,2,3,1.1,1.2,true,3.33333,3,'《test1中文YingYu@!!!》');
insert into tsdba.t2 values('2021-03-05 15:00:00',1,2,3,1.1,1.2,true,3.33333,3,'《test1中文YingYu@!!!》');
insert into tsdba.t2 values('2021-03-06 15:00:00',1,2,3,1.1,1.2,true,3.33333,3,'《test1中文YingYu@!!!》');

select count(*) from tsdba.t2;
delete from tsdba.t2 where code1 = 1;
select count(*) from tsdba.t2;

create table tsdba.t3 (k_timestamp timestamp not null,e1 int2  not null,e2 int,e3 int8 not null,e4 float4,e5 float8 not null,e6 bool,e7 double not null) TAGS (code1 int2 not null, code2 varchar(128) not null)primary tags(code1, code2);

insert into tsdba.t3 values('2021-03-01 15:00:00',1,2,3,1.1,1.2,true,3.33333,1,'《test1中文YingYu@!!!》');
insert into tsdba.t3 values('2021-03-02 15:00:00',1,2,3,1.1,1.2,true,3.33333,1,'《test1中文YingYu@!!!》');
insert into tsdba.t3 values('2021-03-03 15:00:00',1,2,3,1.1,1.2,true,3.33333,1,'《test1中文YingYu@!!!》');
insert into tsdba.t3 values('2021-03-04 15:00:00',1,2,3,1.1,1.2,true,3.33333,1,'《test1中文YingYu@!!!》');

insert into tsdba.t3 values('2021-03-02 15:00:00',1,2,3,1.1,1.2,true,3.33333,2,'《test1中文YingYu@!!!》');
insert into tsdba.t3 values('2021-03-03 15:00:00',1,2,3,1.1,1.2,true,3.33333,2,'《test1中文YingYu@!!!》');
insert into tsdba.t3 values('2021-03-04 15:00:00',1,2,3,1.1,1.2,true,3.33333,2,'《test1中文YingYu@!!!》');
insert into tsdba.t3 values('2021-03-05 15:00:00',1,2,3,1.1,1.2,true,3.33333,2,'《test1中文YingYu@!!!》');

insert into tsdba.t3 values('2021-03-03 15:00:00',1,2,3,1.1,1.2,true,3.33333,3,'《test1中文YingYu@!!!》');
insert into tsdba.t3 values('2021-03-04 15:00:00',1,2,3,1.1,1.2,true,3.33333,3,'《test1中文YingYu@!!!》');
insert into tsdba.t3 values('2021-03-05 15:00:00',1,2,3,1.1,1.2,true,3.33333,3,'《test1中文YingYu@!!!》');
insert into tsdba.t3 values('2021-03-06 15:00:00',1,2,3,1.1,1.2,true,3.33333,3,'《test1中文YingYu@!!!》');

select * from tsdba.t3 order by k_timestamp,code1;
delete from tsdba.t3 where k_timestamp <= '2021-03-03 15:00:00' and code1 = 1;
select * from tsdba.t3 order by k_timestamp,code1;
USE defaultdb;DROP DATABASE IF EXISTS tsdba CASCADE;

create ts database d2;
use d2;
create table t1(
                   start_time TIMESTAMPTZ(3) NOT NULL,
                   end_time TIMESTAMPTZ(3) NOT NULL,
                   seller_load FLOAT8 NOT NULL,
                   seller_price FLOAT8 NOT NULL,
                   buyer VARCHAR(200) NOT NULL,
                   seller VARCHAR(200) NOT NULL
) TAGS (
                        site_id VARCHAR(50) NOT NULL,
                        code VARCHAR(20) NOT NULL,
                        interval_code VARCHAR(20) NOT NULL,
                        id VARCHAR(50) NOT NULL
                ) PRIMARY TAGS(site_id, code, interval_code, id);
insert into t1 (
    start_time, end_time, seller_load, seller_price, buyer, seller,
    site_id, code, interval_code, id
) VALUES
    ('2024-02-01 00:00:00+08', '2024-02-01 01:00:00+08', 2000.0, 0.5,
     '国家电网公司', '测试电力公司',
     'GS1002T', '2', '00:00-01:00', 'test-id-001');
PREPARE delete_stmt AS
DELETE FROM t1 WHERE site_id = $1 AND code = $2 AND interval_code = $3;
set sql_safe_updates = false;
ALTER TABLE t1 DROP COLUMN seller_price;

EXECUTE delete_stmt('GS1002T', '2', '00:00-01:00');

USE defaultdb;DROP DATABASE IF EXISTS d2 CASCADE;