INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:00', -1, 1);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:01', 0, 2);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:02', 32768, 3);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:03', 'test', 4);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:04', true, 5);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:05', 123.679, 6);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:06', -32768, 7);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:07', 32767, 8);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:08', 32767, 9);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:09', '中文', 10);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:10', 32768, 11);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:11', -32769, 12);

INSERT INTO test_savedata1.d2  VALUES ('2018-10-10 10:00:00', -1, 1);
INSERT INTO test_savedata1.d2  VALUES ('2018-10-10 10:00:01', 0, 2);
INSERT INTO test_savedata1.d2  VALUES ('2018-10-10 10:00:02', 2147483648, 3);
INSERT INTO test_savedata1.d2  VALUES ('2018-10-10 10:00:03', 'test', 4);
INSERT INTO test_savedata1.d2  VALUES ('2018-10-10 10:00:04', true, 5);
INSERT INTO test_savedata1.d2  VALUES ('2018-10-10 10:00:05', 2147483647, 6);
INSERT INTO test_savedata1.d2  VALUES ('2018-10-10 10:00:06', 2147483648, 7);
INSERT INTO test_savedata1.d2  VALUES ('2018-10-10 10:00:07', -2147483648, 8);
INSERT INTO test_savedata1.d2  VALUES ('2018-10-10 10:00:08', 123.679, 9);
INSERT INTO test_savedata1.d2  VALUES ('2018-10-10 10:00:09', '中文', 10);
INSERT INTO test_savedata1.d2  VALUES ('2018-10-10 10:00:10', -12147483649, 11);

insert into test_savedata1.nst1 values('2023-8-23 12:13:14', 1, 2.2, true, 4, 'five', 'six', 'seven', 1);
insert into test_savedata1.nst1(ts,e8,e6,e4,e2,e5,e3,e1,e7,e9, tag1) values('2023-8-23 12:13:14', 'eight', 'six', 4, 2.2, 'five', true, 1, 'seven',9, 2);
insert into test_savedata1.nst1(ts,e8,e6,e4,e2,e5,e3,e1, tag1) values('2023-8-23 12:13:14', 'eight', 'six', 4, 2.2, 'five', true, 3);
insert into test_savedata1.nst1(ts,e8,e6,e4,e2,e5,e3, tag1) values('2023-8-23 12:13:14', 'eight', 'six', 4, 2.2, 'five', true, 1, 4);
insert into test_savedata1.nst1(ts,e8,e6,e4,e2, tag1) values('2023-8-23 12:13:14', 'eight', 'six', 4, 2.2, 5);

insert into test.tb values
                        ('2018-10-10 10:00:00',1,2,3,4.4,5.5,6.6,'a','aaaaaaaaaa','a','aaaaaaaaaa',true,b'\xaa',b'\xaa','2011-11-11 11:11:11',
                         1,2,3,4.4,5.5,6.6,'a','aaaaaaaaaa','a','aaaaaaaaaa',true,b'\xaa',b'\xaa'),
                        ('2018-10-10 10:00:01',2,4,6,8.8,10.10,12.12,'b','bbbbbbbbbb','b','bbbbbbbbbb',false,b'\xbb',b'\xbb','2022-02-02 22:22:22',
                         2,4,6,8.8,10.10,12.12,'b','bbbbbbbbbb','b','bbbbbbbbbb',false,b'\xbb',b'\xbb'),
                        ('2018-10-10 10:00:02',3,5,7,9.9,11.11,13.13,'c','cccccccccc','c','cccccccccc',true,b'\xcc',b'\xcc','2033-03-03 23:23:32',
                         3,5,7,9.9,11.11,13.13,'c','cccccccccc','c','cccccccccc',true,b'\xcc',b'\xcc');

insert into test_insert.tb values('2023-05-01 10:10',0,1);
insert into test_insert.tb values('2023-05-01 10:11',-1,0);
insert into test_insert.tb values('2023-05-01 10:12',true,2);
insert into test_insert.tb values('2023-05-01 10:13',false,3);
insert into test_insert.tb values('2023-05-01 10:14',-32768,4);
insert into test_insert.tb values('2023-05-01 10:15',32767,5);
insert into test_insert.tb values('2023-06-15',null);

insert into ts_db.t1 values('2024-01-01 10:00:00',10000,1000000,1000,1047200.0000,109810.0,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',100,200,300,false,100.0,200.0,'beijing','red',b'\x26','fuluolidazhou','160','big','2','社会性别女','1','cuteandlovely');
insert into ts_db.t1(k_timestamp,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,flag,color) values(2021688553000,32050,NULL,4000,9845.87,200.123456,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1\0','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',700,false,'red');
insert into ts_db.t1(k_timestamp,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,flag,color) values(2021688554000,32050,NULL,4000,9845.87,200.123456,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1\0','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',700,false,'red');
insert into ts_db.t1(k_timestamp,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,flag,color) values(2021688555000,32050,NULL,4000,9845.87,200.123456,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1\0','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',700,false,'red');

insert into test_insert2.t1(ts, e7, tag1, tag2, tag3) values
                                                          ('2022-02-02 03:11:11+00',true,false,1111,11110),
                                                          ('2022-02-02 03:11:12+00',0,1,2222,22220),
                                                          ('2022-02-02 03:11:13+00',FALSE,TRUE,3333,33330);