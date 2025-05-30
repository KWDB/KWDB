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
--error
export into sql "nodelocal://1/test_tbtest1/tb1/" from table test.tb1;

create table test.tb2(k_timestamp timestamptz not null, e1 int2, e2 int4, e3 int8, e4 float4, e5 float8, e6 bool, e7 char(20), e8 nchar(20), e10 double) tags (tag1 int2 not null, tag2 int4 not null, tag3 int8 not null, tag4 float4, tag5 float8, tag6 bool, tag7 char(20), tag8 nchar(20), tag10 double) primary tags(tag1, tag2, tag3);
insert into test.tb2 values('2024-01-01 00:00:01+00:00',663,620,901,7463.861328125,-1551.4947464030101,true,'x','o',225.31828421061618,820,139,851,3052.771728515625,-3061.167301514549,true,'w','Z',1632.308420147181);
insert into test.tb2 values('2024-01-01 00:00:01+00:00',663,620,901,7463.861328125,-1551.4947464030101,true,'x','o',225.31828421061618,495,736,420,3052.771728515625,-3061.167301514549,true,'w','Z',1632.308420147181);
insert into test.tb2 values('2024-01-01 00:00:02+00:00',500,324,821,-3514.2734375,2907.959323289191,false,'g','R',1942.0105699072847,865,577,987,-6812.10791015625,5215.895202662417,true,'U','i',-6363.044280492493);
insert into test.tb2 values('2024-01-01 00:00:02+00:00',500,324,821,-3514.2734375,2907.959323289191,false,'g','R',1942.0105699072847,820,139,851,-6812.10791015625,5215.895202662417,true,'U','i',-6363.044280492493);
insert into test.tb2 values('2024-01-01 00:00:04+00:00',666,119,807,9944.78125,-7359.134805999276,true,'A','H',-238.10581074656693,495,736,420,659.4307861328125,-349.5548293794309,false,'m','o',3778.0368072157435);
insert into test.tb2 values('2024-01-01 00:00:04+00:00',666,119,807,9944.78125,-7359.134805999276,true,'A','H',-238.10581074656693,865,577,987,659.4307861328125,-349.5548293794309,false,'m','o',3778.0368072157435);

--success
export into sql "nodelocal://1/test_tbtest1/tb1/" from table test.tb2;

--error(aggregate query)
export INTO sql "nodelocal://1/test_tbtest2/1" from select k_timestamp,e1 from test.tb2 union select k_timestamp,e1 from test.tb2 order by k_timestamp,e1;
use defaultdb;
drop database test cascade;