--test_case001 basic export and import ts before1970;
create ts database test_impexp;
use test_impexp;
create table test_impexp.tb1(k_timestamp timestamptz not null, e1 int2, e2 int4, e3 int8, e4 float4, e5 float8, e6 bool, e7 char(20), e8 nchar(20), e9 varbytes(20), e10 double) tags (tag1 int2 not null, tag2 int4 not null, tag3 int8 not null, tag4 float4, tag5 float8, tag6 bool, tag7 char(20), tag8 nchar(20), tag9 varbytes(20), tag10 double) primary tags(tag1, tag2, tag3);
insert into test_impexp.tb1 values('1969-01-01 00:00:01+00:00',663,620,901,7463.861328125,-1551.4947464030101,true,'x','o','\x30',225.31828421061618,820,139,851,3052.771728515625,-3061.167301514549,true,'w','Z','\x38',1632.308420147181);
export into csv "nodelocal://1/test_impexp/tsbefore1970/" from table test_impexp.tb1;
drop table test_impexp.tb1;
import table create using 'nodelocal://1/test_impexp/tsbefore1970/meta.sql' csv data ('nodelocal://1/test_impexp/tsbefore1970');
select * from test_impexp.tb1 order by tag1, k_timestamp;
use defaultdb;
drop database test_impexp cascade;

