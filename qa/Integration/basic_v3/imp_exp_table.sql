--test_case001 basic export and import;
create ts database test;
use test;
create table test.tb1(k_timestamp timestamptz not null, e1 int2, e2 int4, e3 int8, e4 float4, e5 float8, e6 bool, e7 char(20), e8 nchar(20), e9 varbytes(20), e10 double) tags (tag1 int2 not null, tag2 int4 not null, tag3 int8 not null, tag4 float4, tag5 float8, tag6 bool, tag7 char(20), tag8 nchar(20), tag9 varbytes(20), tag10 double) primary tags(tag1, tag2, tag3);
insert into test.tb1 values('2024-01-01 00:00:01+00:00',663,620,901,7463.861328125,-1551.4947464030101,true,'x','o','\x30',225.31828421061618,820,139,851,3052.771728515625,-3061.167301514549,true,'w','Z','\x38',1632.308420147181);
insert into test.tb1 values('2024-01-01 00:00:01+00:00',663,620,901,7463.861328125,-1551.4947464030101,true,'x','o','\\test',225.31828421061618,495,736,420,3052.771728515625,-3061.167301514549,true,'w','Z','\x38',1632.308420147181);
insert into test.tb1 values('2024-01-01 00:00:02+00:00',500,324,821,-3514.2734375,2907.959323289191,false,'g','R','\x39',1942.0105699072847,865,577,987,-6812.10791015625,5215.895202662417,true,'U','i','\x45',-6363.044280492493);
insert into test.tb1 values('2024-01-01 00:00:02+00:00',500,324,821,-3514.2734375,2907.959323289191,false,'g','R','测试',1942.0105699072847,820,139,851,-6812.10791015625,5215.895202662417,true,'U','i','\x45',-6363.044280492493);
insert into test.tb1 values('2024-01-01 00:00:04+00:00',666,119,807,9944.78125,-7359.134805999276,true,'A','H','\x42',-238.10581074656693,495,736,420,659.4307861328125,-349.5548293794309,false,'m','o','\x36',3778.0368072157435);
insert into test.tb1 values('2024-01-01 00:00:04+00:00',666,119,807,9944.78125,-7359.134805999276,true,'A','H','byte',-238.10581074656693,865,577,987,659.4307861328125,-349.5548293794309,false,'m','o','\x36',3778.0368072157435);
insert into test.tb1 values('2024-01-01 00:00:05+00:00',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,865,577,987,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
export into csv "nodelocal://1/tbtest1/tb1/" from table test.tb1;
drop table test.tb1;
import table create using 'nodelocal://1/tbtest1/tb1/meta.sql' csv data ('nodelocal://1/tbtest1/tb1');
select * from test.tb1 order by tag1, k_timestamp;
use defaultdb;
drop database test cascade;

--test_case002 export with data_only and meta_only, then import;
create ts database test;
use test;
create table test.tb1(k_timestamp timestamptz not null, e1 int2, e2 int4, e3 int8, e4 float4, e5 float8, e6 bool, e7 char(20), e8 nchar(20), e9 varbytes(20), e10 double) tags (tag1 int2 not null, tag2 int4 not null, tag3 int8 not null, tag4 float4, tag5 float8, tag6 bool, tag7 char(20), tag8 nchar(20), tag9 varbytes(20), tag10 double) primary tags(tag1, tag2, tag3);
insert into test.tb1 values('2024-01-01 00:00:01+00:00',663,620,901,7463.861328125,-1551.4947464030101,true,'x','o','\x30',225.31828421061618,820,139,851,3052.771728515625,-3061.167301514549,true,'w','Z','\x38',1632.308420147181);
insert into test.tb1 values('2024-01-01 00:00:01+00:00',663,620,901,7463.861328125,-1551.4947464030101,true,'x','o','\\test',225.31828421061618,495,736,420,3052.771728515625,-3061.167301514549,true,'w','Z','\x38',1632.308420147181);
insert into test.tb1 values('2024-01-01 00:00:02+00:00',500,324,821,-3514.2734375,2907.959323289191,false,'g','R','\x39',1942.0105699072847,865,577,987,-6812.10791015625,5215.895202662417,true,'U','i','\x45',-6363.044280492493);
insert into test.tb1 values('2024-01-01 00:00:02+00:00',500,324,821,-3514.2734375,2907.959323289191,false,'g','R','测试',1942.0105699072847,820,139,851,-6812.10791015625,5215.895202662417,true,'U','i','\x45',-6363.044280492493);
insert into test.tb1 values('2024-01-01 00:00:04+00:00',666,119,807,9944.78125,-7359.134805999276,true,'A','H','\x42',-238.10581074656693,495,736,420,659.4307861328125,-349.5548293794309,false,'m','o','\x36',3778.0368072157435);
insert into test.tb1 values('2024-01-01 00:00:04+00:00',666,119,807,9944.78125,-7359.134805999276,true,'A','H','byte',-238.10581074656693,865,577,987,659.4307861328125,-349.5548293794309,false,'m','o','\x36',3778.0368072157435);
insert into test.tb1 values('2024-01-01 00:00:05+00:00',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,865,577,987,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
export into csv "nodelocal://1/tbtest2/tb1data" from table test.tb1 with data_only;
export into csv "nodelocal://1/tbtest2/tb1meta" from table test.tb1 with meta_only;
drop table test.tb1;
import table create using 'nodelocal://1/tbtest2/tb1meta/meta.sql' csv data ('nodelocal://1/tbtest2/tb1data');
select * from test.tb1 order by tag1, k_timestamp;
use defaultdb;
drop database test cascade;

--test_case003 basic export and import into new table;
create ts database test;
use test;
create table test.tb1(k_timestamp timestamptz not null, e1 int2, e2 int4, e3 int8, e4 float4, e5 float8, e6 bool, e7 char(20), e8 nchar(20), e9 varbytes(20), e10 double) tags (tag1 int2 not null, tag2 int4 not null, tag3 int8 not null, tag4 float4, tag5 float8, tag6 bool, tag7 char(20), tag8 nchar(20), tag9 varbytes(20), tag10 double) primary tags(tag1, tag2, tag3);
insert into test.tb1 values('2024-01-01 00:00:01+00:00',663,620,901,7463.861328125,-1551.4947464030101,true,'x','o','\x30',225.31828421061618,820,139,851,3052.771728515625,-3061.167301514549,true,'w','Z','\x38',1632.308420147181);
insert into test.tb1 values('2024-01-01 00:00:01+00:00',663,620,901,7463.861328125,-1551.4947464030101,true,'x','o','\\test',225.31828421061618,495,736,420,3052.771728515625,-3061.167301514549,true,'w','Z','\x38',1632.308420147181);
insert into test.tb1 values('2024-01-01 00:00:02+00:00',500,324,821,-3514.2734375,2907.959323289191,false,'g','R','\x39',1942.0105699072847,865,577,987,-6812.10791015625,5215.895202662417,true,'U','i','\x45',-6363.044280492493);
insert into test.tb1 values('2024-01-01 00:00:02+00:00',500,324,821,-3514.2734375,2907.959323289191,false,'g','R','测试',1942.0105699072847,820,139,851,-6812.10791015625,5215.895202662417,true,'U','i','\x45',-6363.044280492493);
insert into test.tb1 values('2024-01-01 00:00:04+00:00',666,119,807,9944.78125,-7359.134805999276,true,'A','H','\x42',-238.10581074656693,495,736,420,659.4307861328125,-349.5548293794309,false,'m','o','\x36',3778.0368072157435);
insert into test.tb1 values('2024-01-01 00:00:04+00:00',666,119,807,9944.78125,-7359.134805999276,true,'A','H','byte',-238.10581074656693,865,577,987,659.4307861328125,-349.5548293794309,false,'m','o','\x36',3778.0368072157435);
insert into test.tb1 values('2024-01-01 00:00:05+00:00',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,865,577,987,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
export into csv "nodelocal://1/tbtest3/tb1" from table test.tb1;
create table test.tb2(k_timestamp timestamptz not null, e1 int2, e2 int4, e3 int8, e4 float4, e5 float8, e6 bool, e7 char(20), e8 nchar(20), e9 varbytes(20), e10 double) tags (tag1 int2 not null, tag2 int4 not null, tag3 int8 not null, tag4 float4, tag5 float8, tag6 bool, tag7 char(20), tag8 nchar(20), tag9 varbytes(20), tag10 double) primary tags(tag1, tag2, tag3);
import into test.tb2 csv data ('nodelocal://1/tbtest3/tb1');
select * from test.tb2 order by tag1, k_timestamp;
use defaultdb;
drop database test cascade;

--test_case004 export and import table with delimiter;
create ts database test;
use test;
create table test.tb1(k_timestamp timestamptz not null, e1 int2, e2 int4, e3 int8, e4 float4, e5 float8, e6 bool, e7 char(20), e8 nchar(20), e9 varbytes(20), e10 double) tags (tag1 int2 not null, tag2 int4 not null, tag3 int8 not null, tag4 float4, tag5 float8, tag6 bool, tag7 char(20), tag8 nchar(20), tag9 varbytes(20), tag10 double) primary tags(tag1, tag2, tag3);
insert into test.tb1 values('2024-01-01 00:00:01+00:00',663,620,901,7463.861328125,-1551.4947464030101,true,'x','o','\x30',225.31828421061618,820,139,851,3052.771728515625,-3061.167301514549,true,'w','Z','\x38',1632.308420147181);
insert into test.tb1 values('2024-01-01 00:00:01+00:00',663,620,901,7463.861328125,-1551.4947464030101,true,'x','o','\\test',225.31828421061618,495,736,420,3052.771728515625,-3061.167301514549,true,'w','Z','\x38',1632.308420147181);
insert into test.tb1 values('2024-01-01 00:00:02+00:00',500,324,821,-3514.2734375,2907.959323289191,false,'g','R','\x39',1942.0105699072847,865,577,987,-6812.10791015625,5215.895202662417,true,'U','i','\x45',-6363.044280492493);
insert into test.tb1 values('2024-01-01 00:00:02+00:00',500,324,821,-3514.2734375,2907.959323289191,false,'g','R','测试',1942.0105699072847,820,139,851,-6812.10791015625,5215.895202662417,true,'U','i','\x45',-6363.044280492493);
insert into test.tb1 values('2024-01-01 00:00:04+00:00',666,119,807,9944.78125,-7359.134805999276,true,'A','H','\x42',-238.10581074656693,495,736,420,659.4307861328125,-349.5548293794309,false,'m','o','\x36',3778.0368072157435);
insert into test.tb1 values('2024-01-01 00:00:04+00:00',666,119,807,9944.78125,-7359.134805999276,true,'A','H','byte',-238.10581074656693,865,577,987,659.4307861328125,-349.5548293794309,false,'m','o','\x36',3778.0368072157435);
insert into test.tb1 values('2024-01-01 00:00:05+00:00',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,865,577,987,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
export into csv "nodelocal://1/tbtest4/tb1" from table test.tb1 with delimiter = '.';
drop table test.tb1;
import table create using 'nodelocal://1/tbtest4/tb1/meta.sql' csv data ('nodelocal://1/tbtest4/tb1') with delimiter = '.';
select * from test.tb1 order by tag1, k_timestamp;
use defaultdb;
drop database test cascade;

--test_case005 export select;
create ts database test;
use test;
CREATE TABLE cpu (
         k_timestamp TIMESTAMPTZ NOT NULL,
         usage_user INT8,
         usage_system CHAR(20),
         usage_idle INT8,
         usage_nice INT8,
         usage_iowait INT8,
         usage_irq INT8,
         usage_softirq INT8,
         usage_steal INT8,
         usage_guest INT8,
         usage_guest_nice INT8
) TAGS (
        hostname CHAR(30) NOT NULL,
        region CHAR(30),
        datacenter CHAR(30),
        rack CHAR(30),
        os CHAR(30),
        arch CHAR(30),
        team CHAR(30),
        service CHAR(30),
        service_version CHAR(30),
        service_environment CHAR(30) ) PRIMARY TAGS(hostname);
INSERT INTO cpu VALUES('2016-01-01 00:00:00+00:00', 0, '1', 2, 3, 4, 5, 6, 7, 8, 9, 'host_5', 'us-west-1', 'us-west-a', '0', 'Ubuntu15.10', 'x86_a',  'A', '1', '2', 'ENV');
INSERT INTO cpu VALUES('2020-01-01 00:00:00+00:00', 1, '2', 3, 4, 5, 6, 7, 8, 9, 0, 'host_555', 'us-west-2', 'us-west-b', '1', 'Ubuntu16.10', 'x86_b',  'B', '2', '3', 'ENV2');
INSERT INTO cpu VALUES('2024-01-01 00:00:00+00:00', NULL, NULL, NULL, NULL, NULL, NULL,  NULL, NULL, NULL, NULL, 'host_55555',  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO cpu VALUES('2028-01-01 00:00:00+00:00', NULL, NULL, NULL, NULL, NULL, NULL,  NULL, NULL, NULL, NULL, 'host_55556',  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

EXPORT INTO CSV "nodelocal://1/tbtest5/csv1" FROM SELECT k_timestamp, usage_user, hostname, os FROM cpu WHERE hostname > 'host_4' AND k_timestamp > '1997-01-01' AND os > 'Ubuntu15' AND usage_user > -1;
EXPORT INTO CSV "nodelocal://1/tbtest5/csv2" FROM SELECT k_timestamp, usage_user, hostname, os FROM cpu WHERE hostname > 'host_4' AND k_timestamp < '2020-01-01' AND NOT os > 'Ubuntu15.10' OR usage_user > 0;
EXPORT INTO CSV "nodelocal://1/tbtest5/csv3" FROM SELECT k_timestamp, usage_user, hostname, os FROM cpu WHERE hostname IS NOT NULL AND k_timestamp IS NOT NULL AND os IS NULL AND usage_user IS NULL;
EXPORT INTO CSV "nodelocal://1/tbtest5/csv4" FROM SELECT k_timestamp, usage_user, hostname, os FROM cpu WHERE hostname LIKE '%5' AND os LIKE 'Ubuntu%' AND usage_system LIKE '_';
EXPORT INTO CSV "nodelocal://1/tbtest5/csv5" FROM SELECT k_timestamp, usage_user, hostname, os FROM cpu WHERE hostname IN ('host_5', 'host_555') AND k_timestamp IN ('2016-01-01', '2020-01-01', '2024-01-01') AND os IN ('Ubuntu15.10', 'Ubuntu16.10') AND usage_user IN (0, 1, 2);
EXPORT INTO CSV "nodelocal://1/tbtest5/csv6" FROM SELECT k_timestamp, usage_user, hostname, os FROM cpu WHERE k_timestamp BETWEEN '2007-01-01' AND '2027-01-01' AND hostname BETWEEN 'host_0' AND 'host_666' AND os BETWEEN 'Ubuntu15' AND 'Ubuntu17' AND usage_user BETWEEN '0' AND '1';

CREATE TABLE cpu1 (k_timestamp TIMESTAMPTZ NOT NULL, usage_user INT8) TAGS (hostname CHAR(30) NOT NULL, os CHAR(30)) PRIMARY TAGS(hostname);
CREATE TABLE cpu2 (k_timestamp TIMESTAMPTZ NOT NULL, usage_user INT8) TAGS (hostname CHAR(30) NOT NULL, os CHAR(30)) PRIMARY TAGS(hostname);
CREATE TABLE cpu3 (k_timestamp TIMESTAMPTZ NOT NULL, usage_user INT8) TAGS (hostname CHAR(30) NOT NULL, os CHAR(30)) PRIMARY TAGS(hostname);
CREATE TABLE cpu4 (k_timestamp TIMESTAMPTZ NOT NULL, usage_user INT8) TAGS (hostname CHAR(30) NOT NULL, os CHAR(30)) PRIMARY TAGS(hostname);
CREATE TABLE cpu5 (k_timestamp TIMESTAMPTZ NOT NULL, usage_user INT8) TAGS (hostname CHAR(30) NOT NULL, os CHAR(30)) PRIMARY TAGS(hostname);
CREATE TABLE cpu6 (k_timestamp TIMESTAMPTZ NOT NULL, usage_user INT8) TAGS (hostname CHAR(30) NOT NULL, os CHAR(30)) PRIMARY TAGS(hostname);

IMPORT INTO cpu1 CSV DATA ('nodelocal://1/tbtest5/csv1');
IMPORT INTO cpu2 CSV DATA ('nodelocal://1/tbtest5/csv2');
IMPORT INTO cpu3 CSV DATA ('nodelocal://1/tbtest5/csv3');
IMPORT INTO cpu4 CSV DATA ('nodelocal://1/tbtest5/csv4');
IMPORT INTO cpu5 CSV DATA ('nodelocal://1/tbtest5/csv5');
IMPORT INTO cpu6 CSV DATA ('nodelocal://1/tbtest5/csv6');

SELECT * FROM cpu1 order by hostname;
SELECT * FROM cpu2 order by hostname;
SELECT * FROM cpu3 order by hostname;
SELECT * FROM cpu4 order by hostname;
SELECT * FROM cpu5 order by hostname;
SELECT * FROM cpu6 order by hostname;

use defaultdb;
drop database test cascade;

--test_case006 insert timestampTZ
create ts database test;
use test;
create table test.tb3(k_timestamp timestamptz not null, e1 int2, e2 int4, e3 int8, e4 float4, e5 float8, e6 bool, e7 char(20), e8 nchar(20), e9 varbytes(20), e10 double) tags (tag1 int2 not null, tag2 int4 not null, tag3 int8 not null, tag4 float4, tag5 float8, tag6 bool, tag7 char(20), tag8 nchar(20), tag9 varbytes(20), tag10 double) primary tags(tag1, tag2, tag3);

insert into test.tb3 values('2024-01-01 00:00:01+00:00',663,620,901,7463.861328125,-1551.4947464030101,true,'x','o','\x30',225.31828421061618,820,139,851,3052.771728515625,-3061.167301514549,true,'w','Z','\x38',1632.308420147181);
insert into test.tb3 values('2024-01-01 00:00:02+00:00',663,620,901,7463.861328125,-1551.4947464030101,true,'x','o','\\test',225.31828421061618,495,736,420,3052.771728515625,-3061.167301514549,true,'w','Z','\x38',1632.308420147181);
set time zone 8;
insert into test.tb3 values('2024-01-01 00:00:03+00:00',500,324,821,-3514.2734375,2907.959323289191,false,'g','R','\x39',1942.0105699072847,865,577,987,-6812.10791015625,5215.895202662417,true,'U','i','\x45',-6363.044280492493);
insert into test.tb3 values('2024-01-01 00:00:04+00:00',500,324,821,-3514.2734375,2907.959323289191,false,'g','R','测试',1942.0105699072847,820,139,851,-6812.10791015625,5215.895202662417,true,'U','i','\x45',-6363.044280492493);
insert into test.tb3 values('2024-01-01 08:00:05',666,119,807,9944.78125,-7359.134805999276,true,'A','H','\x42',-238.10581074656693,495,736,420,659.4307861328125,-349.5548293794309,false,'m','o','\x36',3778.0368072157435);
insert into test.tb3 values('2024-01-01 08:00:06',666,119,807,9944.78125,-7359.134805999276,true,'A','H','byte',-238.10581074656693,865,577,987,659.4307861328125,-349.5548293794309,false,'m','o','\x36',3778.0368072157435);
set time zone 1;
insert into test.tb3 values('2024-01-01 08:00:07+08:00',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,865,577,987,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
select k_timestamp from test.tb3 order by k_timestamp;
export into csv "nodelocal://1/tbtest6/tb3/" from table test.tb3;
drop table test.tb3;
set time zone 8;
import table create using 'nodelocal://1/tbtest6/tb3/meta.sql' csv data ('nodelocal://1/tbtest6/tb3');
select k_timestamp from test.tb3 order by k_timestamp;
use defaultdb;
drop database test cascade;
