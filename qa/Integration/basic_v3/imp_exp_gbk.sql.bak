--test_case001 table with charset=GBK;
--testmode 1n 5c
create ts database gbk_impexp;
use gbk_impexp;
CREATE TABLE gbk_impexp.t1(
 k_timestamp TIMESTAMPTZ NOT NULL,
 id INT NOT NULL,
 e1 INT2,
 e2 INT,
 e3 INT8,
 e4 FLOAT4,
 e5 FLOAT8,
 e6 BOOL,
 e7 TIMESTAMPTZ,
 e8 CHAR(1023),
 e9 NCHAR(255),
 e10 VARCHAR(4096),
 e11 CHAR,
 e12 CHAR(255),
 e13 NCHAR,
 e14 NVARCHAR(4096),
 e15 VARCHAR(1023),
 e16 NVARCHAR(200),
 e17 NCHAR(255),
 e18 CHAR(200),
 e19 VARBYTES,
 e20 VARBYTES(60),
 e21 VARCHAR,
 e22 NVARCHAR)
ATTRIBUTES (
 code1 INT2 NOT NULL,
 code2 INT,
 code3 INT8,
 code4 FLOAT4 ,
 code5 FLOAT8,
 code6 BOOL,
 code7 VARCHAR,
 code8 VARCHAR(128) NOT NULL,
 code9 VARBYTES,
 code10 VARBYTES(60),
 code11 VARCHAR,
 code12 VARCHAR(60),
 code13 CHAR(2),
 code14 CHAR(1023) NOT NULL,
 code15 NCHAR,
 code16 NCHAR(254) NOT NULL)
PRIMARY TAGS(code1) partition interval 30d;
INSERT INTO gbk_impexp.t1 values('2024-04-01 00:01:10',1,-22802,NULL,1482075815,40489.984375,7193410.151417,NULL,'1998-06-18 07:27:21.641','中中000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000中000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST0000000中Test000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000Testaaa中0Testaaa000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000aaa000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000Test中TestaaaTest0中000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000Test中0aaa中aaaaaaaaa0中aaa','test数据库语法查询测试！！！@TEST000---',NULL,'H','test数据库语法查询测试！！！@TEST111---Testtest数据库语法查询测试！！！@TEST888---test数据库语法查询测试！！！@TEST000---','a','0Testtest数据库语法查询测试！！！@TEST777---','UU文2e303TUH学学2',NULL,'test数据库语法查询测试！！！@TEST111---Testaaa',NULL,')（c112）-(Tabc中）-(H测试1！abc中b1298*Aabc中)（',NULL,'test数据库语法查询测试！！！@TEST000---中','0test数据库语法查询测试！！！@TEST999---中',-11336,NULL,702359162,34053.347656,8838531.997967,true,'test数据库语法查询测试！！！@TEST1-1','test数据库语法查询测试！！！@TEST1-1','d00ba1ca3U0))bA3bU1A',NULL,'aT3dcaHT*dHd*)3TU)eU',NULL,'e','test数据库语法查询测试！！！@TEST2-2','3','abc中');
INSERT INTO gbk_impexp.t1 values('2024-04-01 00:01:11',2,-5234,NULL,489607587,7684.782227,1156451.029822,false,'1983-12-16 00:21:42.902','中Test中00000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000中aaa中Test000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000中000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000TestTest000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000aaaaaa000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000Test中','test数据库语法查询测试！！！@TEST888---',NULL,'A',NULL,')',NULL,'中c中c)c中中1THa','0aaatest数据库语法查询测试！！！@TEST555---test数据库语法查询测试！！！@TEST111---test数据库语法查询测试！！！@TEST555---','test数据库语法查询测试！！！@TEST666---Test','abAed文eU2HHd(文中bUcb1c文13学0(b1)中d*de*','TT测试1！9090c112TT9090测试1！9090eww）-(Habc中*','3Hb1298*eww）-(U2aaaT3）-()（3AAb1298*','test数据库语法查询测试！！！@TEST777---','Testtest数据库语法查询测试！！！@TEST777---',-11336,292838291,1928554091,NULL,3299510.615697,NULL,'test数据库语法查询测试！！！@TEST3-3','test数据库语法查询测试！！！@TEST1-1','T中*U2*0T(学dd0AU*',NULL,')a1dHA323U3*)Hd*3ae)','文AT文ed(aebd1Aa1','c','test数据库语法查询测试！！！@TEST1-1','U','测试1！');
INSERT INTO gbk_impexp.t1 values('2024-04-01 00:01:12',3,32316,null,1342399661,null,1196369.621120,true,'1993-06-09 08:27:31.451',null,'test数据库语法查询测试！！！@TEST666---',null,'e',null,'*','test数据库语法查询测试！！！@TEST555---Test','中eba0ae*HaH11A001b','test数据库语法查询测试！！！@TEST666---test数据库语法查询测试！！！@TEST777---test数据库语法查询测试！！！@TEST444---','test数据库语法查询测试！！！@TEST777---','2ad学2e(文((0T30dHA(1文a文2**U22a中)文1daAe(','2aaa2aaaTAewwUUc1122aaa）-(U*eww）-(90902aaaTc112T*）-(UT',null,'test数据库语法查询测试！！！@TEST999---',null,-11336,null,616590989,null,441412.964213,null,'test数据库语法查询测试！！！@TEST2-2','test数据库语法查询测试！！！@TEST1-1','T3(cT学学00a(中','3bc)T*1AAbebTccAb0AA(*bA0bUA)3030A2bTAH1','b22d0bebH)*AbTdce)cA','*e0)bb文(A(TT中11e','b','test数据库语法查询测试！！！@TEST3-3','1','T');
INSERT INTO gbk_impexp.t1 values('2024-06-16 00:00:00.30',4,15740,1833527213,1537333752,null,4699419.463739,true,'1997-03-26 03:02:31.151','000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST0000000000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000中000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000中中0中中0aaaaaa中中中aaaTest中000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST00000000000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST0000000中Test000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000Test中aaa','test数据库语法查询测试！！！@TEST999---aaa',null,'*','中中test数据库语法查询测试！！！@TEST555---test数据库语法查询测试！！！@TEST555---test数据库语法查询测试！！！@TEST000---aaa','U',null,'文A*)33Ubeb2HeTa22U',null,'test数据库语法查询测试！！！@TEST333---',null,'dlll》dlll》Ab1298**3)（Tdlll》9090abc中Hc112c112U*2aaa',null,'test数据库语法查询测试！！！@TEST555---','test数据库语法查询测试！！！@TEST444---',2576,null,636730443,5869.634766,2960915.590755,true,'test数据库语法查询测试！！！@TEST2-2','test数据库语法查询测试！！！@TEST3-3','T2U02(3)2(e)d学d**2','3a3dT(*02(caUdTa3A33)abb)dc)UbcbbUeU3dc(','e(A32A*bc1ac2cbTead1','*Hcac*学A学ced11','*','test数据库语法查询测试！！！@TEST2-2','2','U');
INSERT INTO gbk_impexp.t1 values('2024-06-16 00:00:00.31',4,15740,1833527213,1537333752,null,4699419.463739,true,'1997-03-26 03:02:31.151','000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST0000000000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000中000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000中中0中中0aaaaaa中中中aaaTest中000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST00000000000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST0000000中Test000000test数据库语法------查询测试！！！@TEST000000000000test数据库语法------查询测试！！！@TEST000000Test中aaa','test数据库语法查询测试！！！@TEST999---aaa',null,'*','中中test数据库语法查询测试！！！@TEST555---test数据库语法查询测试！！！@TEST555---test数据库语法查询测试！！！@TEST000---aaa','U',null,'文A*)33Ubeb2HeTa22U',null,'test数据库语法查询测试！！！@TEST333---',null,'dlll》dlll》Ab1298**3)（Tdlll》9090abc中Hc112c112U*2aaa',null,'test数据库语法查询测试！！！@TEST555---','test数据库语法查询测试！！！@TEST444---',2576,null,636730443,5869.634766,2960915.590755,true,'test数据库语法查询测试！！！@TEST2-2','test数据库语法查询测试！！！@TEST3-3','T2U02(3)2(e)d学d**2','3a3dT(*02(caUdTa3A33)abb)dc)UbcbbUeU3dc(','e(A32A*bc1ac2cbTead1','*Hcac*学A学ced11','*','test数据库语法查询测试！！！@TEST2-2','2','U');
select * from gbk_impexp.t1 order by k_timestamp;
export into csv "nodelocal://1/geometry/GBK/" from table gbk_impexp.t1 with charset="GBK";
drop table gbk_impexp.t1;
import table create using "nodelocal://1/geometry/GBK/meta.sql" csv data ("nodelocal://1/geometry/GBK/") with charset="GBK";
select * from gbk_impexp.t1 order by k_timestamp;

--test_case002 table with charset=GB18030;
--testmode 1n 5c
export into csv "nodelocal://1/geometry/GB18030/" from table gbk_impexp.t1 with charset="GB18030";
drop table gbk_impexp.t1;
import table create using "nodelocal://1/geometry/GB18030/meta.sql" csv data ("nodelocal://1/geometry/GB18030/") with charset="GB18030";
select * from gbk_impexp.t1 order by k_timestamp;

--test_case003 table with charset=BIG5;
--testmode 1n 5c
use defaultdb;
create ts database gbk_impexp_big5;
use gbk_impexp_big5;
create table deftb(k_timestamp timestamptz not null, e1 int2, e2 int4, e3 int8, e4 float4, e5 float8, e6 bool, e7 char(20), e8 nchar(20), e9 varbytes(20), e10 double) tags (tag1 int2 not null, tag2 int4 not null, tag3 int8 not null, tag4 float4, tag5 float8, tag6 bool, tag7 char(20), tag8 nchar(20), tag9 varbytes(20), tag10 double) primary tags(tag1, tag2, tag3);
insert into deftb values('2024-01-01 00:00:02+00:00',500,324,821,-3514.2734375,2907.959323289191,false,'g','R','\x39',1942.0105699072847,865,577,987,-6812.10791015625,5215.895202662417,true,'U','i','\x45',-6363.044280492493);
insert into deftb values('2024-01-01 00:00:03+00:00',666,119,807,9944.78125,-7359.134805999276,true,'A','H','byte',-238.10581074656693,865,577,987,659.4307861328125,-349.5548293794309,false,'m','o','\x36',3778.0368072157435);
insert into deftb values('2024-01-01 00:00:05+00:00',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,865,577,987,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
select * from gbk_impexp_big5.deftb order by k_timestamp;
export into csv "nodelocal://1/geometry/BIG5/" from table gbk_impexp_big5.deftb with charset="BIG5";
drop table gbk_impexp_big5.deftb;
import table create using "nodelocal://1/geometry/BIG5/meta.sql" csv data ("nodelocal://1/geometry/BIG5/") with charset="BIG5";
select * from gbk_impexp_big5.deftb order by k_timestamp;

--test_case004 database with charset=GB18030;
--testmode 1n 5c
EXPORT INTO CSV "nodelocal://1/db/GB18030/" from database gbk_impexp with charset = "GB18030";
use defaultdb;
drop database gbk_impexp cascade;
IMPORT DATABASE CSV data ("nodelocal://1/db/GB18030/") with charset = "GB18030";
select * from gbk_impexp.t1 order by k_timestamp;

--test_case005 database with charset=GBK;
--testmode 1n 5c
EXPORT INTO CSV "nodelocal://1/db/GBK/" from database gbk_impexp with charset = "GBK";
use defaultdb;
drop database gbk_impexp cascade;
IMPORT DATABASE CSV data ("nodelocal://1/db/GBK/") with charset = "GBK";
select * from gbk_impexp.t1 order by k_timestamp;
use defaultdb;
drop database gbk_impexp cascade;
--test_case006 database with charset=BIG5;
--testmode 1n 5c
use gbk_impexp_big5;
select * from gbk_impexp_big5.deftb order by k_timestamp;
EXPORT INTO CSV "nodelocal://1/db/BIG5/" from database gbk_impexp_big5 with charset = "BIG5";
use defaultdb;
drop database gbk_impexp_big5 cascade;
IMPORT DATABASE CSV data ("nodelocal://1/db/BIG5/") with charset = "BIG5";
select * from gbk_impexp_big5.deftb order by k_timestamp;
use defaultdb;
drop database gbk_impexp_big5 cascade;

--test_case007 bug_37613 Import data after adding columns;
--testmode 1n 5c
CREATE TS DATABASE tsdb1;
CREATE TABLE tsdb1.t1(
                         ts TIMESTAMPTZ NOT NULL,
                         col1 varchar NOT NULL,
                         col2 varchar NOT NULL
)
    ATTRIBUTES (
tag1 INT NOT NULL,
tag2 INT
)
PRIMARY TAGS(tag1);
insert into tsdb1.t1 values('2024-01-01 00:00:01+00:00','1a1中@!','2a1中@!',1,1);
select * from tsdb1.t1;
export into csv "nodelocal://1/addcol" from table tsdb1.t1;

CREATE TABLE tsdb1.t2(
                         ts TIMESTAMPTZ NOT NULL,
                         col1 varchar NOT NULL
)
    ATTRIBUTES (
tag1 INT NOT NULL,
tag2 INT
)
PRIMARY TAGS(tag1);
alter table tsdb1.t2 add col2 varchar(50);
import into tsdb1.t2 csv data ("nodelocal://1/addcol/");
select * from tsdb1.t2;
use defaultdb;
drop database tsdb1 cascade;

--test_case008 bug_37810 Geographical type;
--testmode 1n 5c
create ts database test_geometry;
use test_geometry;
CREATE TABLE test_geometry.tb(
                                 k_timestamp TIMESTAMPTZ NOT NULL,
                                 id INT NOT NULL,
                                 e1 INT2,
                                 e2 INT,
                                 e3 INT8,
                                 e4 FLOAT4,
                                 e5 FLOAT8,
                                 e6 BOOL,
                                 e7 TIMESTAMPTZ,
                                 e8 CHAR(1023),
                                 e9 NCHAR(255),
                                 e10 VARCHAR(4096),
                                 e11 CHAR,
                                 e12 CHAR(255),
                                 e13 NCHAR,
                                 e14 NVARCHAR(4096),
                                 e15 VARCHAR(1023),
                                 e16 NVARCHAR(200),
                                 e17 NCHAR(255),
                                 e18 CHAR(200),
                                 e19 VARBYTES,
                                 e20 varbytes(60),
                                 e21 VARCHAR,
                                 e22 NVARCHAR,
                                 e23 geometry,
                                 e24 geometry,
                                 e25 geometry)
    ATTRIBUTES (
            code1 INT2 NOT NULL,code2 INT,code3 INT8,
            code4 FLOAT4 ,code5 FLOAT8,
            code6 BOOL,
            code7 VARCHAR,code8 VARCHAR(128) NOT NULL,
            code9 VARBYTES,code10 varbytes(60),
            code11 VARCHAR,code12 VARCHAR(60),
            code13 CHAR(2),code14 CHAR(1023) NOT NULL,
            code15 NCHAR,code16 NCHAR(254) NOT NULL)
PRIMARY TAGS(code1,code14,code8,code16);
INSERT INTO test_geometry.tb VALUES('2000-12-31 12:10:10.911',13,23000,23000000,230000000000,-23000088.665120604,-122209810.1131921,true,'2020-12-31 23:59:59.999','测试0','测试1','测试2','T','SSSSSSDDDDDDKKKKKK','B','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','V','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','Point(600000.606066666 9223372036854775806.12345)', 'Linestring(0.0000000 0.0000,1.435243 7.673421)','Polygon((0.0 0.0,1.0 2.0,3.0 4.0,5.0 5.0,6.0 7.0,8.0 9.0,11.0 15.0,45.0 22.0,0.1111 0.0,9.0 0.0,0.0 0.0))',20002,20000002,200000000002,1047200.00312001,1109810.113011921,false,'SSSSSSDDDDDDKKKKKK','O','P','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','WL','SSSSSSDDDDDDKKKKKK','N','SSSSSSDDDDDDKKKKKK');
INSERT INTO test_geometry.tb VALUES('2001-12-9 09:48:12.30',9,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,30000,null,null,null,null,true,null,'test鏁版嵁搴撹娉曟煡璇㈡祴璇曪紒锛侊紒@TESTnull',null,null,null,null,null,'test鏁版嵁搴撹娉曟煡璇㈡祴璇曪紒锛侊紒@TESTnull',null,'test鏁版嵁搴撹娉曟煡璇㈡祴璇曪紒锛侊紒@TESTnull');
INSERT INTO test_geometry.tb VALUES('2004-9-9 00:00:00.9',12,12000,12000000,120000000000,-12000021.003125,-122209810.1131921,true,'2129-3-1 12:00:00.011','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','t','aaaaaabbbbbbcccccc','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','c','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','Point(0.0000000 0.0000)','Linestring(0 0,1 1,2 3,3 3,4 4,5 5,6 6,9 9,111 445,999 999999)','Polygon((0 9223372036854775806.12345,-1.0000000 -1.0000,1.435243 7.673421,0 9223372036854775806.12345))' ,-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'aaaaaabbbbbbcccccc','b','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','ty','aaaaaabbbbbbcccccc','u','aaaaaabbbbbbcccccc');
INSERT INTO test_geometry.tb VALUES('2004-12-31 12:10:10.911',13,23000,23000000,230000000000,-23000088.665120604,-122209810.1131921,true,'2020-12-31 23:59:59.999','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','T','SSSSSSDDDDDDKKKKKK','B','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','V','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','Point(600000.606066666 9223372036854775806.12345)', 'Linestring(0.0000000 0.0000,1.435243 7.673421)','Polygon((0.0 0.0,1.0 2.0,3.0 4.0,5.0 5.0,6.0 7.0,8.0 9.0,11.0 15.0,45.0 22.0,0.1111 0.0,9.0 0.0,0.0 0.0))',20002,20000002,200000000002,1047200.00312001,1109810.113011921,false,'SSSSSSDDDDDDKKKKKK','O','P','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','WL','SSSSSSDDDDDDKKKKKK','N','SSSSSSDDDDDDKKKKKK');
INSERT INTO test_geometry.tb VALUES('2008-2-29 2:10:10.111',14,32767,34000000,340000000000,-43000079.07812032,-122209810.1131921,true,'1975-3-11 00:00:00.0','1234567890987654321','1234567890987654321','1234567890987654321','1','1234567890987654321','2','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','9','1234567890987654321','1234567890987654321','1234567890987654321','Point(-32768 -1)', 'Linestring(-2147483648 -2147483648,0 2147483647)','Polygon((1.0 1.0,2.0 2.0, 1.0 2.0, 1.0 1.0))',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'1234567890987654321','8','7','1234567890987654321','1234567890987654321','1234567890987654321','65','1234567890987654321','4','1234567890987654321');
INSERT INTO test_geometry.tb VALUES(1344618710110,16,11111,-11111111,111111111111,-11111.11111,11111111.11111111,false,'2017-12-11 09:10:00.200',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'','Point(-9223372036854775807 -1)','Linestring(0 9223372036854775807,-9223372036854775807 -1)','Polygon((-32768 -1,1 32768,0.0 0.0,-32768 -1))',0,0,0,0,0,false,e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'');
INSERT INTO test_geometry.tb VALUES(1374618710110,17,-11111,11111111,-111111111111,11111.11111,-11111111.11111111,true,'2036-2-3 10:10:00.089',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ','Point(1.2345678911111 -1)','Linestring(1000.101156789 1000.101156789,0 -2000.2022)','Polygon((0 32767,0 2147483647,-1 -1,0.0 0.0,0 32767))',0,0,0,0,0,false,e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ');
INSERT INTO test_geometry.tb VALUES(1574618710110,18,22222,-22222222,222222222222,-22222.22222,22222222.22222222,false,'2012-1-1 12:12:00.049' ,e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\','Point(-1.0000000 -1.0000)','Linestring(0 -2000.2022,1000.101156789 1000.101156789)','Polygon((-9223372036854775807 -1,0 9223372036854775807,0 0,-9223372036854775807 -1))',0,0,0,0,0,false,e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\');
INSERT INTO test_geometry.tb VALUES(1874618710110,19,-22222,22222222,-222222222222,22222.22222,-22222222.22222222,true,'1980-6-27 19:17:00.123','\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\'  ,'\\\\\\\\' ,'\'  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'Point(0 9223372036854775806.12345)','Linestring(-32767.111156 -1,0 -32767.111156)','Polygon((-1.0000000 -1.0000,0 9223372036854775806.12345,600000.606066666 9223372036854775806.12345,-1.0000000 -1.0000))',0,0,0,0,0,false,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\ ' ,'\\\\\\\\' ,'\'  ,'\\\\\\\\');

select * from test_geometry.tb order by k_timestamp;

EXPORT INTO CSV "nodelocal://1/geometry/table/" from table test_geometry.tb with escaped='"', enclosed='"', delimiter=';';
drop table tb;
IMPORT TABLE CREATE USING 'nodelocal://1/geometry/table/meta.sql' csv data ("nodelocal://1/geometry/table/") with escaped='"', enclosed='"', delimiter=';';
select * from test_geometry.tb order by k_timestamp;
use defaultdb;
drop database test_geometry cascade;