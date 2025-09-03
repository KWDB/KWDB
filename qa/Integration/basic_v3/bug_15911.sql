CREATE ts DATABASE test_query;

create TABLE test_query.d1 (
k_timestamp timestamp not null,
e0 int not null ,
e1 int2 not null ,
e2 int not null ,
e3 bool not null ,
e4 float not null ,
e5 int8 not null ,
e6 float8 not null ,
e7 timestamp not null ,
e8 char(1023) not null ,
e9 nchar(255) not null ,
e10 varchar(4096) not null ,
e11 char not null ,
e12 varchar(255) not null ,
e13 nchar not null ,
e14 varchar not null ,
e15 nvarchar(4096) not null ,
e16 varbytes not null ,
e17 nvarchar(255) not null ,
e18 nvarchar  not null ,
e19 varbytes not null ,
e20 varbytes(1023) not null ,
e21 varbytes(4096) not null ,
e22 varbytes(254) not null )
tags (tag1 int not null) primary tags(tag1) ;

INSERT INTO test_query.d1 VALUES(100,1,10000,1000000,true,1000.123,100000000000,100000.123,111,'test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试', 1);
INSERT INTO test_query.d1 VALUES(20000,2,2000,2000000,true,2000.123,200000000000,200000.123,222,'test时间精度通用查询测试！！！@TEST2','test时间精度通用查询测试！！！@TEST2','test时间精度通用查询测试！！！@TEST2','e','test时间精度通用查询测试！！！@TEST2','文','test时间精度通用查询测试！！！@TEST2','test时间精度通用查询测试！！！@TEST2','test时间精度通用查询测试！TEST2xbb','test时间精度通用查询测试！！！@TEST2','test时间精度通用查询测试！！！@TEST2',b'\xbb','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试', 1);
INSERT INTO test_query.d1 VALUES(3000000,3,3000,3000000,true,3000.123,300000000000,300000.123,333,'test时间精度通用查询测试！！！@TEST3','test时间精度通用查询测试！！！@TEST3','test时间精度通用查询测试！！！@TEST3','e','test时间精度通用查询测试！！！@TEST3','中','test时间精度通用查询测试！！！@TEST3','test时间精度通用查询测试！！！@TEST3','test时间精度通用查询测试！TEST3xcc','test时间精度通用查询测试！！！@TEST3','test时间精度通用查询测试！！！@TEST3',b'\xcc','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试', 1);
INSERT INTO test_query.d1 VALUES(400000000,4,4000,4000000,true,4000.123,400000000000,400000.123,444,'test时间精度通用查询测试！！！@TEST4','test时间精度通用查询测试！！！@TEST4','test时间精度通用查询测试！！！@TEST4','e','test时间精度通用查询测试！！！@TEST4','文','test时间精度通用查询测试！！！@TEST4','test时间精度通用查询测试！！！@TEST4','test时间精度通用查询测试！TEST4xaa','test时间精度通用查询测试！！！@TEST4','test时间精度通用查询测试！！！@TEST4',b'\xdd','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试', 1);
INSERT INTO test_query.d1 VALUES(50000000000,5,5000,5000000,true,5000.123,500000000000,500000.123,555,'test时间精度通用查询测试！！！@TEST5','test时间精度通用查询测试！！！@TEST2','test时间精度通用查询测试！！！@TEST5','e','test时间精度通用查询测试！！！@TEST5','中','test时间精度通用查询测试！！！@TEST5','test时间精度通用查询测试！！！@TEST5','test时间精度通用查询测试！TEST5xee','test时间精度通用查询测试！！！@TEST5','test时间精度通用查询测试！！！@TEST5',b'\xee','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试', 1);
INSERT INTO test_query.d1 VALUES(6000000000000,6,6000,6000000,true,6000.123,600000000000,600000.123,666,'test时间精度通用查询测试！！！@TEST6','test时间精度通用查询测试！！！@TEST6','test时间精度通用查询测试！！！@TEST6','e','test时间精度通用查询测试！！！@TEST6','文','test时间精度通用查询测试！！！@TEST6','test时间精度通用查询测试！！！@TEST6','test时间精度通用查询测试！TEST6xff','test时间精度通用查询测试！！！@TEST6','test时间精度通用查询测试！！！@TEST6',b'\xff','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试', 1);


SELECT e2 > ( select max(e1) from test_query.d1) from test_query.d1;

drop database test_query cascade;
