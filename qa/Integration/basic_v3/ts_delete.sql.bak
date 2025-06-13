create ts database d1;
use d1;

--- delete time-series table tag
create table t2(ts timestamp not null ,a int) tags( b int2 not null) primary tags(b);
create table t3(ts timestamp not null ,a int) tags( b varchar(20) not null, c varchar(10) not null) primary tags(b, c);
create table t4(ts timestamp not null ,a int) tags( b int not null, c char(2) not null, d bool not null, e int) primary tags(b, c, d);
create table t5(ts timestamp not null ,a int) tags( b int not null, c char(2) not null, d bool not null, e int) primary tags(b, c, d);

insert into t2 values(1672531200000,1,1);
delete from t2 where b = 32768;

insert into t3 values(1672531200000,1,'abc','abc');
delete from t3 where b = char'abc' and c = 'abc';
delete from t3 where b = 'abc'::bytes and c = 'abc'::bytes;
delete from t3 where b = b and c = c;
delete from t3 where b is not null and c is not null;
delete from t3 where b = 'abc' and c = 'abc';

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

delete from t4;
set sql_safe_updates = false;
delete from t4;
delete from t4 where true;
delete from t4 where false;
delete from t4 where c like '1';
delete from t4 where b = 1;
delete from t4 where b >= 1;
delete from t4 where b != 1;
delete from t4 where b between 1 and 10;
delete from t4 where b = 's';
delete from t4 where b = 1 and c ='k' and d = false and e =1;
delete from t4 where b = 1 and c ='k' and d = false or e =1;
delete from t4 where b = 1 and c ='k' and d = false and 1 = 1;

select * from t4 order by ts;
delete from t4 where b = 1 and c ='k' and d = false;
select * from t4 order by ts;
delete from t4 where b = '2' and c ='k' and d = false;
select * from t4 order by ts;
delete from t4 where b = 1 and c ='k' and d = true;
select * from t4 order by ts;
delete from t4 where b = 1 and c ='k' and d = false or b = 1 and c ='k' and d = false;
select * from t4 order by ts;
delete from t4 where (b = 1 and c ='1' and d = false) or (b = 1 and c ='1' and d = false);
select * from t4 order by ts;
delete from t4 where (b = 1 and c ='k' and d = true) and (b = 1 and c ='k' and d = true);

delete from t5 where b = 1 and c ='k' and d = false and false;
delete from t5 where b = 1 and c ='k' and d = false and ts < '2023-01-01 08:00:00';
delete from t5 where b = 1 and c ='k' and d = false and ts = '2023-01-01 08:00:00';
delete from t5 where b = 1 and c ='k' and d = false and ts <= '2023-01-01 08:16:40';
delete from t5 where b = 1 and c ='1' and d = false and ts > '2023-02-16 16:20:00';
delete from t5 where b = 1 and c ='1' and d = false and ts >= '2023-02-16 16:20:00';
delete from t5 where b = 1 and c ='k' and d = false and ts < '2023-01-01 08:00:00' or b = 1 and c ='k' and d = false and ts <= '2023-01-01 09:06:40';
delete from t5 where b = 1 and c ='k' and d = false and ts > '2023-02-16 16:20:00' or b = 1 and c ='k' and d = false and ts >= '2023-02-16 16:20:00';
delete from t5 where (b = 2 and c ='1' and d = false and ts >= '2023-01-12 21:48:20') and (b = 2 and c ='1' and d = false and ts <= '2023-01-12 22:55:00');
delete from t5 where b = 1 and c ='k' and d = true and ts != '2023-02-16 16:20:00';

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

delete from t6 where ts < '2023-01-05 7:55:00' or b = 1 and c = 'abc';
select * from t6 order by ts;
delete from t6 where ts <= '2023-01-05 7:55:00' and b = 1 and c = 'abc';
select * from t6 order by ts;
delete from t6 where ts > '2023-01-21 11:46:40' and b = 1 and c = 'abc';
select * from t6 order by ts;
delete from t6 where ts >= '2023-01-21 11:46:40'and b = 1 and c = 'abc';
select * from t6 order by ts;

delete from t6 where ts < '2023-01-21 11:46:40' and ts < '2023-01-05 7:55:00' and b = 1 and c = 'abc';
select * from t6 order by ts;
delete from t6 where ts < '2023-01-21 11:46:40' and b = 1 and c = 'abc' and ts <= '2023-01-05 7:55:00' and b = 1 and c = 'abc';
select * from t6 order by ts;
delete from t6 where ts > '2023-01-21 11:46:40' and b = 1 and c = 'abc' and ts > '2023-01-05 7:55:00' and b = 1 and c = 'abc';
select * from t6 order by ts;
delete from t6 where ts > '2023-01-21 11:46:40' and ts >= '2023-01-05 7:55:00' and b = 1 and c = 'abc';
select * from t6 order by ts;
delete from t6 where ts > '2023-01-21 11:46:40' and ts < '2023-01-05 7:55:00' and b = 1 and c = 'abc';
select * from t6 order by ts;
delete from t6 where ts >= '2023-01-21 11:46:40' and b = 1 and c = 'abc' and ts <= '2023-01-05 7:55:00';
select * from t6 order by ts;
delete from t6 where b = 1 and c = 'abc' and ts <  '2023-01-10 08:00:00' and ts > '2023-01-10 07:56:40';
select * from t6 order by ts;
delete from t6 where ts <= '2023-01-10 08:00:00' and ts >= '2023-01-10 07:56:40' and b = 1 and c = 'abc';
select * from t6 order by ts;

delete from t6 where ts < '2023-01-06 11:46:40' or ts < '2023-01-05 7:55:00' and b = 1 and c = 'abc';
select * from t6 order by ts;
delete from t6 where ts < '2023-01-06 11:46:40' and b = 1 and c = 'abc' or ts <= '2023-01-05 7:55:00' and b = 1 and c = 'abc';
select * from t6 order by ts;
delete from t6 where ts > '2023-01-21 11:46:40' or ts > '2023-01-20 7:55:00' and b = 1 and c = 'abc';
select * from t6 order by ts;
delete from t6 where ts > '2023-01-21 11:46:40' and b = 1 and c = 'abc' or ts >= '2023-01-20 7:55:00' and b = 1 and c = 'abc';
select * from t6 order by ts;
delete from t6 where ts > '2023-01-21 11:46:40' and b = 12 and c = 'abc' or ts < '2023-01-10 7:55:00' and b = 1 and c = 'abc';
select * from t6 order by ts;
delete from t6 where ts >= '2023-01-21 11:46:40' and b = 1 and c = 'abc' or ts <= '2023-01-05 7:55:00' and b = 1 and c = 'abc';
select * from t6 order by ts;
delete from t6 where ts < '2023-01-10 08:00:00' and b = 1 and c = 'abc' or ts > '2023-01-10 07:56:40' and b = 1 and c = 'abc';
select * from t6 order by ts;
delete from t6 where ts <= '2023-01-10 08:00:00' and b = 1 and c = 'abc' or ts >= '2023-01-10 07:56:40' and b = 1 and c = 'abc';
select * from t6 order by ts;
insert into t6 values(1672876800000, 0, 1, 'abc');
delete from t6 where ts = '2023-01-05 08:00:00' and b = 1 and c = 'abc';
delete from t6 where false;
select * from t6 order by ts;
delete from t6 where (ts <= '2023-01-10 08:00:00' and ts >= '2023-01-09 07:56:40' and b = 1 and c = 'abc') or
                     (ts <= '2023-01-05 08:00:00' and ts >= '2023-01-04 07:56:40' and b = 1 and c = 'abc') or
                     (ts >= '2023-01-20 07:56:40' and b = 1 and c = 'abc');
select * from t6 order by ts;
delete from t6 where (ts <= '2023-01-09 08:00:00' or ts >= '2023-01-10 08:56:40' and b = 1 and c = 'abc') and
                     (ts <= '2023-01-04 08:00:00' or ts >= '2023-01-05 08:56:40') and
                     (ts <= '2023-01-18 08:00:00');
select * from t6 order by ts;
delete from t6 where ts != '2023-01-05 08:00:00' and b = 1 and c = 'abc';
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
delete from t6 where ts in ('2023-01-05 12:35:00') or b = 1 and c = 'abc';
delete from t6 where ts in ('ABC') or b = 1 and c = 'abc';
delete from t6 where ts >'2023-01-01 12:45:00' and b in (1, 2) and c = 'abc';
select ts from t6 order by ts;
delete from t6 where ts in ('2023-01-05 12:55:00', '2023-01-01 12:40:00', '2023-01-01 12:45:00') and b = 1 and c = 'abc';
select ts from t6 order by ts;
delete from t6 where ts not in ('2023-01-05 12:35:00', '2023-01-01 12:40:00', '2023-01-01 12:45:00') and b = 1 and c = 'abc';
select ts from t6 order by ts;
insert into t6 values(1672576500000, 1, 1, 'abc');
insert into t6 values(1672576600000, 1, 1, 'abc');
insert into t6 values(1672576700000, 1, 1, 'abc');
insert into t6 values(1672576800000, 1, 1, 'abc');
insert into t6 values(1672576900000, 1, 1, 'abc');
insert into t6 values(1672577000000, 1, 1, 'abc');
insert into t6 values(1672577100000, 1, 1, 'abc');
delete from t6 where ts not in ('2023-01-05 12:35:00', '2023-01-01 12:40:00', '2023-01-01 12:45:00') and b = 1 and c = 'abc';
select ts from t6 order by ts;
delete from t6 where ts in ('2023-01-05 12:55:00', '2023-01-01 12:40:00', '2023-01-01 12:45:00') and b = 1 and c = 'abc';
select ts from t6 order by ts;
insert into t6 values(1672577100000, 1, 1, 'abc');
delete from t6 where ts not in ('2023-01-05 12:55:00', null, '2023-01-01 12:40:00', '2023-01-01 12:45:00') and b = 1 and c = 'abc';
delete from t6 where ts in ('2023-01-05 12:55:00', null, '2023-01-01 12:40:00', '2023-01-01 12:45:00') and b = 1 and c = 'abc';
insert into t6 values(1672577100000, 1, 1, 'abc');
prepare p28 as delete from t6 where ts not in ($1, $2) and b = 1 and c = 'abc';
execute p28('2023-01-05 12:35:00', '2023-01-01 12:40:00');

--- fix-bug-31539
create table t7(ts timestamp not null, a int) tags(b int2 not null) primary tags(b);
create table t8(ts timestamp not null, a int) tags(b int4 not null) primary tags(b);
create table t9(ts timestamp not null, a int) tags(b int8 not null) primary tags(b);
insert into t7 values(0, 0, 0),(0, 1, 32767),(0, 2, -32768);
insert into t8 values(0, 0, 0),(0, 1, 2147483647),(0, 2, -2147483648);
insert into t9 values(0, 0, 0),(0, 1, 9223372036854775807),(0, 2, -9223372036854775808);
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

--- fix-bug-39137
create table t11(ts timestamp not null ,a int) tags( b int2 not null) primary tags(b);
insert into t11 values('2024-07-03 00:00:00', 1, 1);
alter table t11 add column c int;
delete from t11 where b = 1;
insert into t11 values('2024-07-03 00:00:00', 1, 1, 1);
alter table t11 drop column a;
delete from t11 where b = 1;

--- timestamp modification case
create table t10(ts timestamp not null ,a int) tags( b int2 not null) primary tags(b);
insert into t10 values('0000-01-01 00:00:00', 1, 1);
insert into t10 values('0001-01-01 00:00:00', 1, 1);
insert into t10 values('1000-01-01 00:00:00', 1, 1);
insert into t10 values('1001-01-01 00:00:00', 1, 1);
insert into t10 values('1969-12-31 23:59:59.999', 1, 1);
insert into t10 values('1970-01-01 00:00:00', 1, 1);
insert into t10 values('2000-01-01 00:00:00', 1, 1);
insert into t10 values('2024-04-24 00:00:00', 1, 1);
insert into t10 values('2970-01-01 00:00:00', 1, 1);

select ts from t10 order by ts;
delete from t10 where b = 1 and ts in ('-0001-01-01 00:00:00');
delete from t10 where b = 1 and ts < '0000-01-01 00:00:00';
delete from t10 where b = 1 and ts <= '0001-01-01 00:00:00';
delete from t10 where b = 1 and (ts > '0000-01-01 00:00:00' and ts < '1000-01-01 00:00:00');
delete from t10 where b = 1 and ts = '1001-01-01 00:00:00';
delete from t10 where b = 1 and ('1969-12-31 23:59:59.999' <= ts and '1970-01-01 00:00:00' >= ts);
delete from t10 where b = 1 and (ts < '-0001-01-01 00:00:00' or ts > '2971-01-01 00:00:00');
delete from t10 where b = 1 and (ts > '-0001-01-01 00:00:00' or ts < '2971-01-01 00:00:00');

insert into t10 values('0000-01-01 00:00:00', 1, 1);
insert into t10 values('0001-01-01 00:00:00', 1, 1);
insert into t10 values('1000-01-01 00:00:00', 1, 1);
insert into t10 values('1001-01-01 00:00:00', 1, 1);
insert into t10 values('1969-12-31 23:59:59.999', 1, 1);
insert into t10 values('1970-01-01 00:00:00', 1, 1);
insert into t10 values('2000-01-01 00:00:00', 1, 1);
insert into t10 values('2024-04-24 00:00:00', 1, 1);
insert into t10 values('2970-01-01 00:00:00', 1, 1);
select ts from t10 order by ts;
delete from t10 where b = 1 and ts in ('0000-01-01 00:00:00', '1969-12-31 23:59:59.999', '2024-04-24 00:00:00');
delete from t10 where b = 1 and ts not in ('1000-01-01 00:00:00', '1970-01-01 00:00:00', '2970-01-01 00:00:00');
delete from t10 where b = 1 and ts not in ('-1000-01-01 00:00:00', '3070-01-01 00:00:00');

insert into t10 values('0000-01-01 00:00:00', 1, 1);
insert into t10 values('0001-01-01 00:00:00', 1, 1);
insert into t10 values('1000-01-01 00:00:00', 1, 1);
insert into t10 values('1001-01-01 00:00:00', 1, 1);
insert into t10 values('1969-12-31 23:59:59.999', 1, 1);
insert into t10 values('1970-01-01 00:00:00', 1, 1);
insert into t10 values('2000-01-01 00:00:00', 1, 1);
insert into t10 values('2024-04-24 00:00:00', 1, 1);
insert into t10 values('2970-01-01 00:00:00', 1, 1);
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
create table t2(ts timestamp not null ,a int) tags( b int2 not null) primary tags(b);
create table t3(ts timestamp not null ,a int) tags( b varchar(20) not null, c varchar(10) not null) primary tags(b, c);
create table t4(ts timestamp not null ,a int) tags( b int not null, c char(2) not null, d bool not null, e int) primary tags(b, c, d);
create table t5(ts timestamp not null ,a int) tags( b int not null, c char(2) not null, d bool not null, e int) primary tags(b, c, d);

insert into t2 values(1672531200000,1,1);
prepare p1 as delete from t2 where b = $1;
execute p1(32768);

insert into t3 values(1672531200000,1,'abc','abc');
prepare p2 as delete from t3 where b = $1 and c = $2;
execute p2(char'abc', 'abc');
execute p2('abc'::bytes, 'abc'::bytes);
execute p2(b, c);
execute p2('abc', 'abc');

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

--prepare p3 as delete from t4;
set sql_safe_updates = false;
--prepare p3 as delete from t4;
--prepare p3 as delete from t4 where true;
--prepare p3 as delete from t4 where false;
prepare p3 as delete from t4 where c like '1';
prepare p3 as delete from t4 where b = $1;
prepare p3 as delete from t4 where b >= $1;
prepare p3 as delete from t4 where b != $1;
prepare p3 as delete from t4 where b between $1 and $2;
prepare p3 as delete from t4 where b = $1;
prepare p3 as delete from t4 where b = $1 and c = $2 and d = $3 and e = $4;
prepare p3 as delete from t4 where b = $1 and c = $2 and d = $3 or e = $4;
prepare p3 as delete from t4 where b = $1 and c = $2 and d = $3 and 1 = 1;

select * from t4 order by ts;
prepare p3 as delete from t4 where b = $1 and c =$2 and d = $3;
execute p3(1,'k',false);
select * from t4 order by ts;
execute p3('2','k',false);
select * from t4 order by ts;
execute p3(1,'k',true);
select * from t4 order by ts;
prepare p4 as delete from t4 where b = $1 and c =$2 and d = $3 or b = $4 and c = $5 and d = $6;


prepare p5 as delete from t5 where b = $1 and c =$2 and d = $3 and false;
execute p5(1,'k',false);
prepare p6 as delete from t5 where b = $1 and c = $2 and d = $3 and ts < $4;
execute p6(1,'k',false,'2023-01-01 08:00:00');
prepare p7 as delete from t5 where b = $1 and c = $2 and d = $3 and ts = $4;
execute p7(1,'k',false,'2023-01-01 08:00:00');
prepare p8 as delete from t5 where b = $1 and c = $2 and d = $3 and ts <= $4;
execute p8(1,'k',false,'2023-01-01 08:16:40');
prepare p9 as delete from t5 where b = $1 and c = $2 and d = $3 and ts > $4;
execute p9(1,'1',false,'2023-02-16 16:20:00');
prepare p10 as delete from t5 where b = $1 and c = $2 and d = $3 and ts >= $4;
execute p10(1,'1',false,'2023-02-16 16:20:00');
prepare p11 as delete from t5 where b = $1 and c = $2 and d = $3 and ts != $4;
execute p11(1,'k',true,'2023-02-16 16:20:00');

--- fix-bug-31539
create table t7(ts timestamp not null, a int) tags(b int2 not null) primary tags(b);
create table t8(ts timestamp not null, a int) tags(b int4 not null) primary tags(b);
create table t9(ts timestamp not null, a int) tags(b int8 not null) primary tags(b);
insert into t7 values(0, 0, 0),(0, 1, 32767),(0, 2, -32768);
insert into t8 values(0, 0, 0),(0, 1, 2147483647),(0, 2, -2147483648);
insert into t9 values(0, 0, 0),(0, 1, 9223372036854775807),(0, 2, -9223372036854775808);
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
create table test_delete1.t1(ts timestamptz not null, e1 nchar, e2 nchar(64))  tags(tag1 nchar not null, tag2 nchar(64)) primary tags(tag1);
prepare p26 as delete from test_delete1.t1 where ts=$1 and tag1 ='a';
prepare p27 as insert into test_delete1.t1 values($1,'a','a','a','a');
execute p27(1);
execute p26(1);
drop database test_delete1;

set sql_safe_updates = false;

USE defaultdb;DROP DATABASE IF EXISTS test_delete CASCADE;
CREATE TS DATABASE test_delete;
CREATE TABLE test_delete.tb1(
k_timestamp TIMESTAMPTZ NOT NULL,
e1 INT2,
e2 INT,
e3 INT8,
e4 FLOAT4,
e5 FLOAT,
e6 BOOL,
e7 TIMESTAMP,
e8 CHAR(1023),
e9 VARCHAR,
e10 VARCHAR(4096),
e11 NCHAR(255),
e12 VARBYTES(6))
TAGS (code1 INT4 NOT NULL, code2 VARCHAR(128) NOT NULL, code3 CHAR(200) NOT NULL, code4 INT8 NOT NULL,
code5 INT4 NOT NULL, code6 VARCHAR(128) NOT NULL, code7 CHAR(200) NOT NULL, code8 INT8 NOT NULL)
PRIMARY TAGS (code1,code2);
-- expected failed
delete from test_delete.tb1 where k_timestamp >0;
delete from test_delete.tb1 where k_timestamp =0;
delete from test_delete.tb1 where k_timestamp <0;
delete from test_delete.tb1 where k_timestamp >=0;
delete from test_delete.tb1 where k_timestamp <=0;
delete from test_delete.tb1 where k_timestamp >0 or k_timestamp =0;
delete from test_delete.tb1 where k_timestamp >0 and k_timestamp =0;
delete from test_delete.tb1 where k_timestamp in(123);
delete from test_delete.tb1 where k_timestamp not in(123);
delete from test_delete.tb1 where k_timestamp in('test');
delete from test_delete.tb1 where k_timestamp in('123');
delete from test_delete.tb1 where k_timestamp >'2024-1-1 08:00:00' and code1=0;

-- expected successed
-- case001
USE defaultdb;DROP DATABASE IF EXISTS test_delete CASCADE;
CREATE TS DATABASE test_delete;
CREATE TABLE test_delete.tb2(
k_timestamp TIMESTAMPTZ NOT NULL,
e1 INT2,
e2 INT,
e3 INT8,
e4 FLOAT4,
e5 FLOAT,
e6 BOOL,
e7 TIMESTAMP,
e8 CHAR(1023),
e9 VARCHAR,
e10 VARCHAR(4096),
e11 NCHAR(255),
e12 VARBYTES(6))
TAGS (code1 INT4 NOT NULL, code2 VARCHAR(128) NOT NULL, code3 CHAR(200) NOT NULL, code4 INT8 NOT NULL,
code5 INT4 NOT NULL, code6 VARCHAR(128) NOT NULL, code7 CHAR(200) NOT NULL, code8 INT8 NOT NULL)
PRIMARY TAGS (code1,code2);
INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:01',10001,1000001,100000001,110011.110011,110011.110011,true,'2010-10-10 10:10:11.101','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',b'\x61',1001,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182,9182,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182);
INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:02',10001,1000001,100000001,110011.110011,110011.110011,true,'2010-10-10 10:10:11.101','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',b'\x61',1001,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182,9182,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182);

INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:03',20002,2000002,200000002,220022.220022,220022.220022,false,'2020-12-12 00:10:59.222','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',b'\x80',1002,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',-9223372036854775808,2147483647,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',-9223372036854775808);
INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:04',20002,2000002,200000002,220022.220022,220022.220022,false,'2020-12-12 00:10:59.222','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',b'\x80',1002,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',-9223372036854775808,2147483647,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',-9223372036854775808);

INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:05',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL, 1003,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182,9182,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182);
INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:06',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL, 1003,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182,9182,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182);

INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:07',30003,3000003,300000003,330033.330033,330033.330033,true,'2230-3-30 10:10:00.3','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',b'\xcc',1004,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',9223372036854775807,-2147483648,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',9223372036854775807);
INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:08',30003,3000003,300000003,330033.330033,330033.330033,true,'2230-3-30 10:10:00.3','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',b'\xcc',1004,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',9223372036854775807,-2147483648,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',9223372036854775807);

INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:09',30004,4000004,400000004,440044.440044,440044.440044,false,'2010-10-10 10:10:11.101','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',b'\x61',1005,'\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',0,0,'\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',0);
INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:10',30004,4000004,400000004,440044.440044,440044.440044,false,'2010-10-10 10:10:11.101','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',b'\x61',1005,'\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',0,0,'\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',0);

INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:11',30005,5000005,500000005,550055.550055,550055.550055,true,'2230-3-30 10:10:00.3','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',b'\xcc',1006,'《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',0,0,'《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',0);
INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:12',30005,5000005,500000005,550055.550055,550055.550055,true,'2230-3-30 10:10:00.3','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',b'\xcc',1006,'《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',0,0,'《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',0);

INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:13',30006,6000006,600000006,660066.660066,660066.660066,true,'2230-3-30 10:10:00.3','','','','','',1007,'','',0,0,'','',0);
INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:14',30006,6000006,600000006,660066.660066,660066.660066,true,'2230-3-30 10:10:00.3','','','','','',1007,'','',0,0,'','',0);

INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:15',30007,7000007,700000007,770077.770077,770077.770077,true,'2230-3-30 10:10:00.3','     ','     ','     ','     ',' ',1008,'     ','     ',0,0,'     ','     ',0);
INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:16',30007,7000007,700000007,770077.770077,770077.770077,true,'2230-3-30 10:10:00.3','     ','     ','     ','     ',' ',1008,'     ','     ',0,0,'     ','     ',0);

INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:17',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','','','','','0',1009,'','',0,0,'','',0);
INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:18',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','','','','','0',1009,'','',0,0,'','',0);

INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:19',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','\','\','\','\','\\',1010,'\','\',0,0,'\','\',0);
INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:20',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','\','\','\','\','\\',1010,'\','\',0,0,'\','\',0);

INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:21',30009,9000009,900000009,90099.990099,990099.990099,true,'2230-3-30 10:10:00.3',e'\\',e'\\',e'\\',e'\\',e'\\\\',1011,e'\\',e'\\',0,0,e'\\',e'\\',0);
INSERT INTO test_delete.tb2 VALUES('2024-1-1 08:00:22',30009,9000009,900000009,90099.990099,990099.990099,true,'2230-3-30 10:10:00.3',e'\\',e'\\',e'\\',e'\\',e'\\\\',1011,e'\\',e'\\',0,0,e'\\',e'\\',0);


select count(*) from test_delete.tb2;
delete from test_delete.tb2 where k_timestamp ='2024-1-1 08:00:01';
delete from test_delete.tb2 where k_timestamp ='2024-1-1 08:00:02' or k_timestamp ='2024-1-1 08:00:03';
delete from test_delete.tb2 where k_timestamp ='2024-1-1 08:00:02' and k_timestamp ='2024-1-1 08:00:03';
select k_timestamp from test_delete.tb2 order by k_timestamp;
delete from test_delete.tb2 where k_timestamp <'2024-1-1 08:00:05';
delete from test_delete.tb2 where k_timestamp >'2024-1-1 08:00:21';
select k_timestamp from test_delete.tb2 order by k_timestamp;
delete from test_delete.tb2 where k_timestamp >='2024-1-1 08:00:10' and k_timestamp <='2024-1-1 08:00:12';
select k_timestamp from test_delete.tb2 order by k_timestamp;
delete from test_delete.tb2 where k_timestamp <='2024-1-1 08:00:06' or k_timestamp >'2024-1-1 08:00:20';
select k_timestamp from test_delete.tb2 order by k_timestamp;
delete from test_delete.tb2 where k_timestamp in('2024-1-1 08:00:07','2024-1-1 08:00:20');
select k_timestamp from test_delete.tb2 order by k_timestamp;
delete from test_delete.tb2 where k_timestamp not in('2024-1-1 08:00:15','2024-1-1 08:00:09','2024-1-1 08:00:18');
select k_timestamp from test_delete.tb2 order by k_timestamp;
delete from test_delete.tb2;
select k_timestamp from test_delete.tb2 order by k_timestamp;

-- case002
USE defaultdb;DROP DATABASE IF EXISTS test_delete CASCADE;
CREATE TS DATABASE test_delete;
CREATE TABLE test_delete.tb3(
k_timestamp TIMESTAMPTZ NOT NULL,
e1 INT2,
e2 INT,
e3 INT8,
e4 FLOAT4,
e5 FLOAT,
e6 BOOL,
e7 TIMESTAMP,
e8 CHAR(1023),
e9 VARCHAR,
e10 VARCHAR(4096),
e11 NCHAR(255),
e12 VARBYTES(6))
TAGS (code1 INT4 NOT NULL, code2 VARCHAR(128) NOT NULL, code3 CHAR(200) NOT NULL, code4 INT8 NOT NULL,
code5 INT4 NOT NULL, code6 VARCHAR(128) NOT NULL, code7 CHAR(200) NOT NULL, code8 INT8 NOT NULL)
PRIMARY TAGS (code1,code2,code7,code8);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:01',10001,1000001,100000001,110011.110011,110011.110011,true,'2010-10-10 10:10:11.101','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',b'\x61',1001,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182,9182,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:02',10001,1000001,100000001,110011.110011,110011.110011,true,'2010-10-10 10:10:11.101','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',b'\x61',1001,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182,9182,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:03',20002,2000002,200000002,220022.220022,220022.220022,false,'2020-12-12 00:10:59.222','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',b'\x80',1002,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',-9223372036854775808,2147483647,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',-9223372036854775808);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:04',20002,2000002,200000002,220022.220022,220022.220022,false,'2020-12-12 00:10:59.222','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',b'\x80',1002,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',-9223372036854775808,2147483647,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',-9223372036854775808);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:05',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL, 1003,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182,9182,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:06',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL, 1003,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182,9182,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:07',30003,3000003,300000003,330033.330033,330033.330033,true,'2230-3-30 10:10:00.3','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',b'\xcc',1004,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',9223372036854775807,-2147483648,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',9223372036854775807);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:08',30003,3000003,300000003,330033.330033,330033.330033,true,'2230-3-30 10:10:00.3','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',b'\xcc',1004,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',9223372036854775807,-2147483648,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',9223372036854775807);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:09',30004,4000004,400000004,440044.440044,440044.440044,false,'2010-10-10 10:10:11.101','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',b'\x61',1005,'\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',0,0,'\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',0);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:10',30004,4000004,400000004,440044.440044,440044.440044,false,'2010-10-10 10:10:11.101','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',b'\x61',1005,'\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',0,0,'\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',0);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:11',30005,5000005,500000005,550055.550055,550055.550055,true,'2230-3-30 10:10:00.3','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',b'\xcc',1006,'《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',0,0,'《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',0);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:12',30005,5000005,500000005,550055.550055,550055.550055,true,'2230-3-30 10:10:00.3','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',b'\xcc',1006,'《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',0,0,'《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',0);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:13',30006,6000006,600000006,660066.660066,660066.660066,true,'2230-3-30 10:10:00.3','','','','','',1007,'','',0,0,'','',0);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:14',30006,6000006,600000006,660066.660066,660066.660066,true,'2230-3-30 10:10:00.3','','','','','',1007,'','',0,0,'','',0);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:15',30007,7000007,700000007,770077.770077,770077.770077,true,'2230-3-30 10:10:00.3','     ','     ','     ','     ',' ',1008,'     ','     ',0,0,'     ','     ',0);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:16',30007,7000007,700000007,770077.770077,770077.770077,true,'2230-3-30 10:10:00.3','     ','     ','     ','     ',' ',1008,'     ','     ',0,0,'     ','     ',0);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:17',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','','','','','0',1009,'','',0,0,'','',0);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:18',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','','','','','0',1009,'','',0,0,'','',0);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:19',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','\','\','\','\','\\',1010,'\','\',0,0,'\','\',0);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:20',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','\','\','\','\','\\',1010,'\','\',0,0,'\','\',0);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:21',30009,9000009,900000009,90099.990099,990099.990099,true,'2230-3-30 10:10:00.3',e'\\',e'\\',e'\\',e'\\',e'\\\\',1011,e'\\',e'\\',0,0,e'\\',e'\\',0);
INSERT INTO test_delete.tb3 VALUES('2024-1-1 08:00:22',30009,9000009,900000009,90099.990099,990099.990099,true,'2230-3-30 10:10:00.3',e'\\',e'\\',e'\\',e'\\',e'\\\\',1011,e'\\',e'\\',0,0,e'\\',e'\\',0);

delete from test_delete.tb3;
select k_timestamp from test_delete.tb3 order by k_timestamp;

-- case003
USE defaultdb;DROP DATABASE IF EXISTS test_delete CASCADE;
CREATE TS DATABASE test_delete;
CREATE TABLE test_delete.tb4(
                                k_timestamp TIMESTAMPTZ NOT NULL,
                                e1 INT2,
                                e2 INT,
                                e3 INT8,
                                e4 FLOAT4,
                                e5 FLOAT,
                                e6 BOOL,
                                e7 TIMESTAMP,
                                e8 CHAR(1023),
                                e9 VARCHAR,
                                e10 VARCHAR(4096),
                                e11 NCHAR(255),
                                e12 VARBYTES(6))
    TAGS (code1 INT4 NOT NULL, code2 VARCHAR(128) NOT NULL, code3 CHAR(200) NOT NULL, code4 INT8 NOT NULL,
code5 INT4 NOT NULL, code6 VARCHAR(128) NOT NULL, code7 CHAR(200) NOT NULL, code8 INT8 NOT NULL)
PRIMARY TAGS (code1,code2,code7,code8);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:01',10001,1000001,100000001,110011.110011,110011.110011,true,'2010-10-10 10:10:11.101','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',b'\x61',1001,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182,9182,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:02',10001,1000001,100000001,110011.110011,110011.110011,true,'2010-10-10 10:10:11.101','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',b'\x61',1001,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182,9182,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:03',20002,2000002,200000002,220022.220022,220022.220022,false,'2020-12-12 00:10:59.222','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',b'\x80',1002,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',-9223372036854775808,2147483647,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',-9223372036854775808);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:04',20002,2000002,200000002,220022.220022,220022.220022,false,'2020-12-12 00:10:59.222','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',b'\x80',1002,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',-9223372036854775808,2147483647,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',-9223372036854775808);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:05',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL, 1003,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182,9182,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:06',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL, 1003,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182,9182,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:07',30003,3000003,300000003,330033.330033,330033.330033,true,'2230-3-30 10:10:00.3','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',b'\xcc',1004,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',9223372036854775807,-2147483648,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',9223372036854775807);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:08',30003,3000003,300000003,330033.330033,330033.330033,true,'2230-3-30 10:10:00.3','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',b'\xcc',1004,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',9223372036854775807,-2147483648,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',9223372036854775807);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:09',30004,4000004,400000004,440044.440044,440044.440044,false,'2010-10-10 10:10:11.101','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',b'\x61',1005,'\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',0,0,'\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:10',30004,4000004,400000004,440044.440044,440044.440044,false,'2010-10-10 10:10:11.101','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',b'\x61',1005,'\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',0,0,'\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:11',30005,5000005,500000005,550055.550055,550055.550055,true,'2230-3-30 10:10:00.3','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',b'\xcc',1006,'《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',0,0,'《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:12',30005,5000005,500000005,550055.550055,550055.550055,true,'2230-3-30 10:10:00.3','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',b'\xcc',1006,'《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',0,0,'《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:13',30006,6000006,600000006,660066.660066,660066.660066,true,'2230-3-30 10:10:00.3','','','','','',1007,'','',0,0,'','',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:14',30006,6000006,600000006,660066.660066,660066.660066,true,'2230-3-30 10:10:00.3','','','','','',1007,'','',0,0,'','',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:15',30007,7000007,700000007,770077.770077,770077.770077,true,'2230-3-30 10:10:00.3','     ','     ','     ','     ',' ',1008,'     ','     ',0,0,'     ','     ',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:16',30007,7000007,700000007,770077.770077,770077.770077,true,'2230-3-30 10:10:00.3','     ','     ','     ','     ',' ',1008,'     ','     ',0,0,'     ','     ',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:17',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','','','','','0',1009,'','',0,0,'','',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:18',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','','','','','0',1009,'','',0,0,'','',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:19',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','\','\','\','\','\\',1010,'\','\',0,0,'\','\',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:20',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','\','\','\','\','\\',1010,'\','\',0,0,'\','\',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:21',30009,9000009,900000009,90099.990099,990099.990099,true,'2230-3-30 10:10:00.3',e'\\',e'\\',e'\\',e'\\',e'\\\\',1011,e'\\',e'\\',0,0,e'\\',e'\\',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:22',30009,9000009,900000009,90099.990099,990099.990099,true,'2230-3-30 10:10:00.3',e'\\',e'\\',e'\\',e'\\',e'\\\\',1011,e'\\',e'\\',0,0,e'\\',e'\\',0);

select k_timestamp from test_delete.tb4 order by k_timestamp;
delete from test_delete.tb4 where k_timestamp < '1960-1-1 08:00:14';
delete from test_delete.tb4 where k_timestamp <= '1960-1-1 08:00:14' and k_timestamp > '2024-1-1 08:00:21';
delete from test_delete.tb4 where k_timestamp = '1960-1-1 08:00:14' or k_timestamp = '2024-1-1 08:00:21';
delete from test_delete.tb4 where k_timestamp > '1960-1-1 08:00:14' and k_timestamp < '2024-1-1 08:00:10';
delete from test_delete.tb4 where k_timestamp >= '1960-1-1 08:00:14' or k_timestamp < '2024-1-1 08:00:10';

INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:01',10001,1000001,100000001,110011.110011,110011.110011,true,'2010-10-10 10:10:11.101','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',b'\x61',1001,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182,9182,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:02',10001,1000001,100000001,110011.110011,110011.110011,true,'2010-10-10 10:10:11.101','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',b'\x61',1001,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182,9182,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:03',20002,2000002,200000002,220022.220022,220022.220022,false,'2020-12-12 00:10:59.222','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',b'\x80',1002,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',-9223372036854775808,2147483647,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',-9223372036854775808);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:04',20002,2000002,200000002,220022.220022,220022.220022,false,'2020-12-12 00:10:59.222','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',b'\x80',1002,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',-9223372036854775808,2147483647,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',-9223372036854775808);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:05',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL, 1003,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182,9182,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:06',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL, 1003,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182,9182,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',-9182);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:07',30003,3000003,300000003,330033.330033,330033.330033,true,'2230-3-30 10:10:00.3','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',b'\xcc',1004,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',9223372036854775807,-2147483648,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',9223372036854775807);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:08',30003,3000003,300000003,330033.330033,330033.330033,true,'2230-3-30 10:10:00.3','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',b'\xcc',1004,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',9223372036854775807,-2147483648,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',9223372036854775807);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:09',30004,4000004,400000004,440044.440044,440044.440044,false,'2010-10-10 10:10:11.101','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',b'\x61',1005,'\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',0,0,'\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:10',30004,4000004,400000004,440044.440044,440044.440044,false,'2010-10-10 10:10:11.101','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',b'\x61',1005,'\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',0,0,'\0《test2中文YingYu@!!!》\0\0\0','\0《test2中文YingYu@!!!》\0\0\0',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:11',30005,5000005,500000005,550055.550055,550055.550055,true,'2230-3-30 10:10:00.3','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',b'\xcc',1006,'《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',0,0,'《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:12',30005,5000005,500000005,550055.550055,550055.550055,true,'2230-3-30 10:10:00.3','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',b'\xcc',1006,'《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',0,0,'《test4中文YingYu@!!!》','《test4中文YingYu@!!!》',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:13',30006,6000006,600000006,660066.660066,660066.660066,true,'2230-3-30 10:10:00.3','','','','','',1007,'','',0,0,'','',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:14',30006,6000006,600000006,660066.660066,660066.660066,true,'2230-3-30 10:10:00.3','','','','','',1007,'','',0,0,'','',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:15',30007,7000007,700000007,770077.770077,770077.770077,true,'2230-3-30 10:10:00.3','     ','     ','     ','     ',' ',1008,'     ','     ',0,0,'     ','     ',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:16',30007,7000007,700000007,770077.770077,770077.770077,true,'2230-3-30 10:10:00.3','     ','     ','     ','     ',' ',1008,'     ','     ',0,0,'     ','     ',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:17',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','','','','','0',1009,'','',0,0,'','',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:18',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','','','','','0',1009,'','',0,0,'','',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:19',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','\','\','\','\','\\',1010,'\','\',0,0,'\','\',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:20',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','\','\','\','\','\\',1010,'\','\',0,0,'\','\',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:21',30009,9000009,900000009,90099.990099,990099.990099,true,'2230-3-30 10:10:00.3',e'\\',e'\\',e'\\',e'\\',e'\\\\',1011,e'\\',e'\\',0,0,e'\\',e'\\',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:22',30009,9000009,900000009,90099.990099,990099.990099,true,'2230-3-30 10:10:00.3',e'\\',e'\\',e'\\',e'\\',e'\\\\',1011,e'\\',e'\\',0,0,e'\\',e'\\',0);

select k_timestamp from test_delete.tb4 order by k_timestamp;
delete from test_delete.tb4 where k_timestamp in ('1960-1-1 08:00:14');
delete from test_delete.tb4 where k_timestamp in ('1960-1-1 08:00:14', '2024-1-1 08:00:21');
delete from test_delete.tb4 where k_timestamp not in ('1960-1-1 08:00:14', '2024-1-1 08:00:21', '2024-1-1 08:00:22');

INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:15',30007,7000007,700000007,770077.770077,770077.770077,true,'2230-3-30 10:10:00.3','     ','     ','     ','     ',' ',1008,'     ','     ',0,0,'     ','     ',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:16',30007,7000007,700000007,770077.770077,770077.770077,true,'2230-3-30 10:10:00.3','     ','     ','     ','     ',' ',1008,'     ','     ',0,0,'     ','     ',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:17',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','','','','','0',1009,'','',0,0,'','',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:18',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','','','','','0',1009,'','',0,0,'','',0);
INSERT INTO test_delete.tb4 VALUES('2024-1-1 08:00:19',30008,8000008,800000008,880088.880088,880088.880088,true,'2230-3-30 10:10:00.3','\','\','\','\','\\',1010,'\','\',0,0,'\','\',0);
select k_timestamp from test_delete.tb4 order by k_timestamp;
prepare test_pre1 as delete from test_delete.tb4 where k_timestamp < $1;
prepare test_pre2 as delete from test_delete.tb4 where k_timestamp > $1 and k_timestamp < $2;
prepare test_pre3 as delete from test_delete.tb4 where k_timestamp in ($1, $2);
prepare test_pre4 as delete from test_delete.tb4;
execute test_pre1('1960-1-1 08:00:14');
execute test_pre2('1960-1-1 08:00:14', '2024-1-1 08:00:16');
execute test_pre3('1960-1-1 08:00:14', '2024-1-1 08:00:22');
execute test_pre4;

USE defaultdb;DROP DATABASE IF EXISTS test_delete CASCADE;


-- bug ZDP-39156
-- case 1
set sql_safe_updates = false;
DROP DATABASE IF EXISTS tsdb2 CASCADE;
create ts database tsdb2;
create table tsdb2.t1(ts timestamp not null, c1 int, ac10 VARCHAR(50) NULL) tags(c2 int not null) primary tags(c2);

insert into tsdb2.t1 values('2024-06-01 01:00:01.000', 1, '1ac10', 1);
insert into tsdb2.t1 values('2024-06-01 01:00:02.000', 1, '3ac10', 1);
insert into tsdb2.t1 values('2024-06-01 01:00:03.000', 1, NULL, 1);
select count(ac10), max(ac10), min(ac10), first(ac10), last(ac10), first_row(ac10), last_row(ac10) from tsdb2.t1 where ts<='2024-06-01 01:00:03.000';

delete from tsdb2.t1 where ts<'2024-06-01 01:00:03.000';
delete from tsdb2.t1 where ts='2024-06-01 01:00:03.000';
select count(ac10), max(ac10), min(ac10), first(ac10), last(ac10), first_row(ac10), last_row(ac10) from tsdb2.t1 where ts<='2024-06-01 01:00:03.000';

insert into tsdb2.t1 values('2024-06-01 01:00:01.000', 1, '1ac10', 1);
insert into tsdb2.t1 values('2024-06-01 01:00:02.000', 1, '3ac10', 1);
insert into tsdb2.t1 values('2024-06-01 01:00:03.000', 1, NULL, 1);
select count(ac10), max(ac10), min(ac10), first(ac10), last(ac10), first_row(ac10), last_row(ac10) from tsdb2.t1 where ts<='2024-06-01 01:00:03.000';

-- case 2
CREATE TABLE tsdb2.t2(k_timestamp timestamptz NOT NULL, e1 int2, e2 int, e3 int8, e4 float4, e5 float8, e6 bool, e7 timestamptz, e8 char(1023), e9 nchar(255), e10 char(810), e11 char, e12 char(812), e13 nchar, e14 char(814), e15 nchar(215), e16 char(816), e17 nchar(217), e18 char(418), e19 varchar(256), e20 char(420), e21 char(221) , e22 char(422), ac10 VARCHAR(50) NULL) ATTRIBUTES (code1 int NOT NULL,flag int NOT NULL,color nchar(200) NOT NULL,t1 smallint,t2 int,t3 bigint,t4 float,t5 double) primary tags(code1,flag,color);

INSERT INTO tsdb2.t2(k_timestamp,ac10,code1,flag,color) values('2024-06-01 01:00:00.000',NULL,1,1,'a');
INSERT INTO tsdb2.t2(k_timestamp,ac10,code1,flag,color) values('2024-06-01 01:00:01.000','1ac10',1,1,'a');
INSERT INTO tsdb2.t2(k_timestamp,ac10,code1,flag,color) values('2024-06-01 01:00:02.000','3ac10',1,1,'a');
INSERT INTO tsdb2.t2(k_timestamp,ac10,code1,flag,color) values('2024-06-01 01:00:03.000',NULL,1,1,'a');

select ac10 from tsdb2.t2 where k_timestamp<='2024-06-01 01:00:03.000';

select count(ac10),max(ac10),min(ac10),first(ac10),last(ac10),first_row(ac10),last_row(ac10) from tsdb2.t2 where k_timestamp<='2024-06-01 01:00:03.000';

delete from tsdb2.t2 where k_timestamp<='2024-06-01 01:00:03.000';

INSERT INTO tsdb2.t2(k_timestamp,ac10,code1,flag,color) values('2024-06-01 01:00:00.000',NULL,1,1,'a');
INSERT INTO tsdb2.t2(k_timestamp,ac10,code1,flag,color) values('2024-06-01 01:00:01.000','1ac10',1,1,'a');
INSERT INTO tsdb2.t2(k_timestamp,ac10,code1,flag,color) values('2024-06-01 01:00:02.000','3ac10',1,1,'a');
INSERT INTO tsdb2.t2(k_timestamp,ac10,code1,flag,color) values('2024-06-01 01:00:03.000',NULL,1,1,'a');

select ac10 from tsdb2.t2 where k_timestamp<='2024-06-01 01:00:03.000';

select count(ac10),max(ac10),min(ac10),first(ac10),last(ac10),first_row(ac10),last_row(ac10) from tsdb2.t2 where k_timestamp<='2024-06-01 01:00:03.000';

-- case 3
CREATE TABLE tsdb2.t3(k_timestamp timestamptz NOT NULL, e1 int2, e2 int, e3 int8, e4 float4, e5 float8, e6 bool, e7 timestamptz, e8 char(1023), e9 nchar(255), e10 char(810), e11 char, e12 char(812), e13 nchar, e14 char(814), e15 nchar(215), e16 char(816), e17 nchar(217), e18 char(418), e19 varchar(256), e20 char(420), e21 char(221) , e22 char(422), ac10 VARCHAR(50) NULL) ATTRIBUTES (code1 int NOT NULL,flag int NOT NULL,color nchar(200) NOT NULL,t1 smallint,t2 int,t3 bigint,t4 float,t5 double) primary tags(code1,flag,color);

INSERT INTO tsdb2.t3(k_timestamp,ac10,code1,flag,color) values('2024-06-01 01:00:00.000',NULL,1,1,'a');
INSERT INTO tsdb2.t3(k_timestamp,ac10,code1,flag,color) values('2024-06-01 01:00:01.000','1ac10',1,1,'a');
INSERT INTO tsdb2.t3(k_timestamp,ac10,code1,flag,color) values('2024-06-01 01:00:02.000','3ac10',1,1,'a');
INSERT INTO tsdb2.t3(k_timestamp,ac10,code1,flag,color) values('2024-06-01 01:00:03.000',NULL,1,1,'a');

select ac10 from tsdb2.t3 where k_timestamp<='2024-06-01 01:00:03.000';

select count(ac10),max(ac10),min(ac10),first(ac10),last(ac10),first_row(ac10),last_row(ac10) from tsdb2.t3 where k_timestamp<='2024-06-01 01:00:03.000';

delete from tsdb2.t3 where k_timestamp<='2024-06-01 01:00:03.000';

INSERT INTO tsdb2.t3(k_timestamp,ac10,code1,flag,color) values('2024-06-01 01:00:00.000',NULL,1,1,'a');
INSERT INTO tsdb2.t3(k_timestamp,ac10,code1,flag,color) values('2024-06-01 01:00:01.000',NULL,1,1,'a');
INSERT INTO tsdb2.t3(k_timestamp,ac10,code1,flag,color) values('2024-06-01 01:00:02.000',NULL,1,1,'a');
INSERT INTO tsdb2.t3(k_timestamp,ac10,code1,flag,color) values('2024-06-01 01:00:03.000',NULL,1,1,'a');

select ac10 from tsdb2.t3 where k_timestamp<='2024-06-01 01:00:03.000';

select count(ac10),max(ac10),min(ac10),first(ac10),last(ac10),first_row(ac10),last_row(ac10) from tsdb2.t3 where k_timestamp<='2024-06-01 01:00:03.000';

USE defaultdb;DROP DATABASE IF EXISTS tsdb2 CASCADE;

-- bug ZDP-42366
-- case 1
DROP DATABASE IF EXISTS tsdba CASCADE;
create ts database tsdba;
create table tsdba.t1 (k_timestamp timestamp not null,e1 int2  not null,e2 int,e3 int8 not null,e4 float4,e5 float8 not null,e6 bool,e7 double not null) TAGS (code1 int2 not null)primary tags(code1);

insert into tsdba.t1 values('2021-03-01 15:00:00',1,2,3,1.1,1.2,true,3.33333,1);
select count(*) from tsdba.t1;
delete from tsdba.t1 where 1=1;
select count(*) from tsdba.t1;

--case 2
create table tsdba.t2 (k_timestamp timestamp not null,e1 int2  not null,e2 int,e3 int8 not null,e4 float4,e5 float8 not null,e6 bool,e7 double not null) TAGS (code1 int2 not null)primary tags(code1);

insert into tsdba.t2 values('2021-03-01 15:00:00',1,2,3,1.1,1.2,true,3.33333,1);
insert into tsdba.t2 values('2021-03-02 15:00:00',1,2,3,1.1,1.2,true,3.33333,1);
insert into tsdba.t2 values('2021-03-03 15:00:00',1,2,3,1.1,1.2,true,3.33333,1);
insert into tsdba.t2 values('2021-03-04 15:00:00',1,2,3,1.1,1.2,true,3.33333,1);

insert into tsdba.t2 values('2021-03-02 15:00:00',1,2,3,1.1,1.2,true,3.33333,2);
insert into tsdba.t2 values('2021-03-03 15:00:00',1,2,3,1.1,1.2,true,3.33333,2);
insert into tsdba.t2 values('2021-03-04 15:00:00',1,2,3,1.1,1.2,true,3.33333,2);
insert into tsdba.t2 values('2021-03-05 15:00:00',1,2,3,1.1,1.2,true,3.33333,2);

insert into tsdba.t2 values('2021-03-03 15:00:00',1,2,3,1.1,1.2,true,3.33333,3);
insert into tsdba.t2 values('2021-03-04 15:00:00',1,2,3,1.1,1.2,true,3.33333,3);
insert into tsdba.t2 values('2021-03-05 15:00:00',1,2,3,1.1,1.2,true,3.33333,3);
insert into tsdba.t2 values('2021-03-06 15:00:00',1,2,3,1.1,1.2,true,3.33333,3);

select count(*) from tsdba.t2;
delete from tsdba.t2 where code1 = 1;
select count(*) from tsdba.t2;

--case 3
create table tsdba.t3 (k_timestamp timestamp not null,e1 int2  not null,e2 int,e3 int8 not null,e4 float4,e5 float8 not null,e6 bool,e7 double not null) TAGS (code1 int2 not null)primary tags(code1);

insert into tsdba.t3 values('2021-03-01 15:00:00',1,2,3,1.1,1.2,true,3.33333,1);
insert into tsdba.t3 values('2021-03-02 15:00:00',1,2,3,1.1,1.2,true,3.33333,1);
insert into tsdba.t3 values('2021-03-03 15:00:00',1,2,3,1.1,1.2,true,3.33333,1);
insert into tsdba.t3 values('2021-03-04 15:00:00',1,2,3,1.1,1.2,true,3.33333,1);

insert into tsdba.t3 values('2021-03-02 15:00:00',1,2,3,1.1,1.2,true,3.33333,2);
insert into tsdba.t3 values('2021-03-03 15:00:00',1,2,3,1.1,1.2,true,3.33333,2);
insert into tsdba.t3 values('2021-03-04 15:00:00',1,2,3,1.1,1.2,true,3.33333,2);
insert into tsdba.t3 values('2021-03-05 15:00:00',1,2,3,1.1,1.2,true,3.33333,2);

insert into tsdba.t3 values('2021-03-03 15:00:00',1,2,3,1.1,1.2,true,3.33333,3);
insert into tsdba.t3 values('2021-03-04 15:00:00',1,2,3,1.1,1.2,true,3.33333,3);
insert into tsdba.t3 values('2021-03-05 15:00:00',1,2,3,1.1,1.2,true,3.33333,3);
insert into tsdba.t3 values('2021-03-06 15:00:00',1,2,3,1.1,1.2,true,3.33333,3);

select count(*) from tsdba.t3;
delete from tsdba.t3 where k_timestamp <= '2021-03-03 15:00:00';
select count(*) from tsdba.t3;

USE defaultdb;DROP DATABASE IF EXISTS tsdba CASCADE;