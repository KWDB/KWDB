--ZDP-23663
create ts database test_last;
create table test_last.tb (k_timestamp timestamp not null,e1 timestamp,e2 int2,e3 int,e4 int8,e5 float,e6 float8,e7 bool,e8 char,e9 char(1023),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(255),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(1023),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (att1 int2 not null,att2 int,att3 int8,att4 bool,att5 float4,att6 float8,att7 varchar,att8 varchar(64)) primary tags (att1);
create table test_last.stb (k_timestamp timestamp not null,e1 timestamp,e2 int2,e3 int,e4 int8,e5 float,e6 float8,e7 bool,e8 char,e9 char(1023),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(255),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(1023),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (name varchar(10) not null, att1 int2,att2 int,att3 int8,att4 bool,att5 float4,att6 float8,att7 varchar,att8 varchar(3000),att9 char(1000),att10 varbytes(10),att11 varbytes(100)) primary tags (name);

insert into test_last.tb values('2022-03-03 03:13:00.133',111111110000,100,10000,100000,1000000.101,100000000.1010111,true,
'','','','','','','','','','','','','','','', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2018-05-05 14:13:45.113',111111110011,100,10000,100000,1000000.101,100000000.1010111,false,
'c','char1','n','nchar255定长类型^&@#nchar255定长类型^&@#nchar255定长类型^&@#',
'varchar_11','varchar255测试cdf~#varchar255测试cdf~#','varcharlengthis4096_1字符类型测试%%&!+varcharlengthis4096字符类型测试%%&!+',
'nvarchar-0','nvarchar255_1()&^%{}','nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}',
'b','bytes1023','varbytes_1',b'\xaa\xbb\xcc',b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xdd\xdd\xdd', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2023-01-15 21:36:21.22', 111111110022, 200, 20000, 200000, 2000000.202, 200000000.2020222, true,
'h', 'char2', 'c', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_22', 'varchar255_1中文测试abc!<>&^%$varchar255中文测试abc!<>&^%$', 'varcharlengthis4096_2字符类型测试%%&!+varcharlengthis4096字符类型测试%%&!+',
'nvarchar-1', 'nvarchar255nvarchar255()&^%{}','nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}',
'y', b'bytes', 'varbytes_2', b'\xdd\xee\xff', b'\xee\xee\xee\xff\xff\xff\xee\xee\xee\xff\xff\xff', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2023-07-10 20:00:00', 111111110044, 300, 30000, 300000, 3000000.303, 300000000.3030333, false,
'r', 'char3', 'a', 'nchar255_1定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_44', 'varchar255_中文测试abc!<>&^%$varchar255_中文测试abc!<>&^%$', 'varcharlengthof4096通用查询test%%&!+varcharlengthof4096通用查询test%%&!+',
'nvarchar-2', 'nvarchar255()&^%{}', 'nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}',
'e', b'byte12', 'varbytes_3', b'\xbb\xcc\xff', 'varbytes4096$%^varbytes4096$%^varbytes4096$%^', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2023-07-02 08:10:00', 111111110033, 700, 70000, 700000, 7000000.707, 700000000.7070777, true,
'a', 'char4', 'h', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_33', 'varchar255_123456中文测试abc!<>&^%$varchar255_123456中文测试abc!<>&^%$', 'varcharlengthof4096字符类型test%%&!+varcharlengthof4096字符类型test%%&!+',
'nvarchar-3', 'nvarchar255_+!@#nvarchar255_+!@#', 'nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}',
't', b'bytes1', 'varbytes_2', b'\xaa\xdd\xee', b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xdd\xdd\xdd', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.stb values('2022-03-03 03:13:00.133',111111110000,100,10000,100000,1000000.101,100000000.1010111,true,
'','','','','','','','','','','','','','','','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2018-05-05 14:13:45.113',111111110011,100,10000,100000,1000000.101,100000000.1010111,false,
'c','char1','n','nchar255定长类型^&@#nchar255定长类型^&@#nchar255定长类型^&@#',
'varchar_11','varchar255测试cdf~#varchar255测试cdf~#','varcharlengthis4096_1字符类型测试%%&!+varcharlengthis4096字符类型测试%%&!+',
'nvarchar-0','nvarchar255_1()&^%{}','nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}',
'b','bytes1023','varbytes_1',b'\xaa\xbb\xcc',b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xdd\xdd\xdd','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2023-01-15 21:36:21.22', 111111110022, 200, 20000, 200000, 2000000.202, 200000000.2020222, true,
'h', 'char2', 'c', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_22', 'varchar255_1中文测试abc!<>&^%$varchar255中文测试abc!<>&^%$', 'varcharlengthis4096_2字符类型测试%%&!+varcharlengthis4096字符类型测试%%&!+',
'nvarchar-1', 'nvarchar255nvarchar255()&^%{}','nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}',
'y', b'bytes', 'varbytes_2', b'\xdd\xee\xff', b'\xee\xee\xee\xff\xff\xff\xee\xee\xee\xff\xff\xff','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2022-03-04 03:13:00.133',111111110000,100,10000,100000,1000000.101,100000000.1010111,true,
'','','','','','','','','','','','','','','','stb_2',null,null,null,null,null,null,null,'shanghai',null,'blue',null);
insert into test_last.stb values('2023-07-10 20:00:00', 111111110044, 300, 30000, 300000, 3000000.303, 300000000.3030333, false,
'r', 'char3', 'a', 'nchar255_1定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_44', 'varchar255_中文测试abc!<>&^%$varchar255_中文测试abc!<>&^%$', 'varcharlengthof4096通用查询test%%&!+varcharlengthof4096通用查询test%%&!+',
'nvarchar-2', 'nvarchar255()&^%{}', 'nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}',
'e', b'byte12', 'varbytes_3', b'\xbb\xcc\xff', 'varbytes4096$%^varbytes4096$%^varbytes4096$%^','stb_2',null,null,null,null,null,null,null,'shanghai',null,'blue',null);
insert into test_last.stb values('2023-07-02 08:10:00', 111111110033, 700, 70000, 700000, 7000000.707, 700000000.7070777, true,
'a', 'char4', 'h', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_33', 'varchar255_123456中文测试abc!<>&^%$varchar255_123456中文测试abc!<>&^%$', 'varcharlengthof4096字符类型test%%&!+varcharlengthof4096字符类型test%%&!+',
'nvarchar-3', 'nvarchar255_+!@#nvarchar255_+!@#', 'nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}',
't', b'bytes1', 'varbytes_2', b'\xaa\xdd\xee', b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xdd\xdd\xdd','stb_2',null,null,null,null,null,null,null,'shanghai',null,'blue',null);
insert into test_last.stb values('2023-05-10 20:20:00', 111111110033, 700, 70000, 700000, 7000000.707, 700000000.7070777, true,
'a', 'char4', 'h', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_33', 'varchar255_123456中文测试abc!<>&^%$varchar255_123456中文测试abc!<>&^%$', 'varcharlengthof4096字符类型test%%&!+varcharlengthof4096字符类型test%%&!+',
'nvarchar-3', 'nvarchar255_+!@#nvarchar255_+!@#', 'nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}',
't', b'bytes', 'varbytes_2', b'\xaa\xdd\xee', b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xdd\xdd\xdd','stb_3',null,null,null,null,null,null,null,'shanghai',null,'blue',null);
insert into test_last.stb values('2023-05-10 20:20:00', 111111110044, 300, 30000, 300000, 3000000.303, 300000000.3030333, false,
'r', 'char3', 'a', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_44', 'varchar255_中文测试abc!<>&^%$varchar255_中文测试abc!<>&^%$', 'varcharlengthof4096通用查询test%%&!+varcharlengthof4096通用查询test%%&!+',
'nvarchar-2', 'nvarchar255()&^%{}', 'nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096中英文测试1中英文测试1中英文测试1()*&^%{}',
'e', b'byte12', 'varbytes_3', b'\xbb\xcc\xff', 'varbytes4096$%^varbytes4096$%^varbytes4096$%^','stb_3',null,null,null,null,null,null,null,'shanghai',null,'blue',null);


select last(e2),last(e3),last(e4) from test_last.tb;
select last_row(e2),last_row(e3),last_row(e4) from test_last.tb;
select last(e10),last(e11) from test_last.tb;
select last_row(e10),last_row(e11) from test_last.tb;
select last(e12),last(e13),last(e14) from test_last.tb;
select last_row(e12),last_row(e13),last_row(e14) from test_last.tb;
select last(e15),last(e16),last(e17) from test_last.tb;
select last_row(e15),last_row(e16),last_row(e17) from test_last.tb;
select last(e20),last(e21),last(e22) from test_last.tb;
select last_row(e20),last_row(e21),last_row(e22) from test_last.tb;
select last(e3),last(e12) from test_last.tb;
select last_row(e3),last_row(e12) from test_last.tb;
select last(*) from test_last.tb;
select last_row(*) from test_last.tb;


select last(e12),last(e13),last(e14) from test_last.stb; 
select last_row(e12),last_row(e13),last_row(e14) from test_last.stb;
select last(e15),last(e16),last(e17) from test_last.stb;
select last_row(e15),last_row(e16),last_row(e17) from test_last.stb;
select last(e20),last(e21),last(e22) from test_last.stb;
select last_row(e20),last_row(e21),last_row(e22) from test_last.stb;
select last(e3),last(e12) from test_last.stb;
select last_row(e3),last_row(e12) from test_last.stb;
select last(e12),last(e7) from test_last.stb;
select last(e9),last(e12),last(e18) from test_last.stb;
select last_row(e9),last_row(e12),last_row(e18) from test_last.stb;


select last(e2),last(e3),last(e4) from test_last.stb where name = 'stb_1';
select last_row(e2),last_row(e3),last_row(e4) from test_last.stb where name = 'stb_1';
select last(e5),last(e6) from test_last.stb where name = 'stb_2';
select last_row(e5),last_row(e6) from test_last.stb where name = 'stb_2';
select last(e12),last(e13),last(e14) from test_last.stb where name = 'stb_1';
select last_row(e12),last_row(e13),last_row(e14) from test_last.stb where name = 'stb_1';
select last(e15),last(e16),last(e17) from test_last.stb where name = 'stb_2';
select last_row(e15),last_row(e16),last_row(e17) from test_last.stb where name = 'stb_2';
select last(e3),last(e12) from test_last.stb where name = 'stb_1';
select last_row(e3),last_row(e12) from test_last.stb where name = 'stb_1';
select last(e9),last(e12),last(e18) from test_last.stb where name = 'stb_1';
select last_row(e9),last_row(e12),last_row(e18) from test_last.stb where name = 'stb_1';
select last_row(*) from test_last.stb where name = 'stb_1';
select last_row(*) from test_last.stb where name = 'stb_2';

drop database test_last cascade;

-- ZDP-3670
create ts database test_last;
create table test_last.tb (k_timestamp timestamp not null,e1 timestamp,e2 int2,e3 int,e4 int8,e5 float,e6 float8,e7 bool,e8 char,e9 char(1023),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(255),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(1023),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (att1 int2 not null,att2 int,att3 int8,att4 bool,att5 float4,att6 float8,att7 varchar,att8 varchar(64)) primary tags (att1);
create table test_last.stb (k_timestamp timestamp not null,e1 timestamp,e2 int2,e3 int,e4 int8,e5 float,e6 float8,e7 bool,e8 char,e9 char(1023),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(255),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(1023),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (name varchar(10) not null, att1 int2,att2 int,att3 int8,att4 bool,att5 float4,att6 float8,att7 varchar,att8 varchar(3000),att9 char(1000),att10 varbytes(10),att11 varbytes(100)) primary tags (name);
insert into test_last.tb values('2023-05-15 09:10:11.123',111111110000,100,10000,100000,1000000.101,100000000.1010111,true,
'','','','','','','','','','','','','','','', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2023-05-15 09:10:14.678',111111110011,400,40000,400000,4000000.404,400000000.4040444,false,
'c','char1','n','nchar255定长类型^&@#nchar255定长类型^&@#nchar255定长类型^&@#',
'varchar_11','varchar255测试cdf~#varchar255测试cdf~#','varcharlengthis4096_0字符类型测试%%&!+varcharlengthis4096字符类型测试%%&!+',
'nvarchar-0','nvarchar255()&^%{}','nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}',
'b','bytes1023','varbytes_1',b'\xaa\xbb\xcc',b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xdd\xdd\xdd', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2023-05-15 09:10:12.345', 111111110022, 200, 20000, 200000, 2000000.202, 200000000.2020222, true,
'h', 'char2', 'c', 'nchar255_1定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_22', 'varchar255中文测试abc!<>&^%$varchar255中文测试abc!<>&^%$', 'varcharlengthis4096字符类型测试%%&!+varcharlengthis4096字符类型测试%%&!+',
'nvarchar-1', 'nvarchar255nvarchar255()&^%{}','nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}',
'y', b'bytes', 'varbytes_2', b'\xdd\xee\xff', b'\xee\xee\xee\xff\xff\xff\xee\xee\xee\xff\xff\xff', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2023-07-10 20:18:43.145', 111111110044, 300, 30000, 300000, 3000000.303, 300000000.3030333, false,
'r', 'char3', 'a', 'nchar255_0定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_44', 'varchar255_中文测试abc!<>&^%$varchar255_中文测试abc!<>&^%$', 'varcharlengthof4096通用查询test%%&!+varcharlengthof4096通用查询test%%&!+',
'nvarchar-2', 'nvarchar255()&^%{}', 'nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}',
'e', b'byte12', 'varbytes_3', b'\xbb\xcc\xff', 'varbytes4096$%^varbytes4096$%^varbytes4096$%^', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2023-07-10 20:18:41.456', 111111110033, 700, 70000, 700000, 7000000.707, 700000000.7070777, true,
'a', 'char4', 'h', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_33', 'varchar255_123456中文测试abc!<>&^%$varchar255_123456中文测试abc!<>&^%$', 'varcharlengthof4096字符类型test%%&!+varcharlengthof4096字符类型test%%&!+',
'nvarchar-3', 'nvarchar255_+!@#nvarchar255_+!@#', 'nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}',
't', b'bytes', 'varbytes_2', b'\xaa\xdd\xee', b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xee\xee\xee', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.stb values('2023-05-15 09:10:11.123',111111110000,100,10000,100000,1000000.101,100000000.1010111,true,
'','','','','','','','','','','','','','','','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2023-05-15 09:10:14.678', 111111110022, 200, 20000, 200000, 2000000.202, 200000000.2020222, true,
'h', 'char2', 'c', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_22', 'varchar255中文测试abc!<>&^%$varchar255中文测试abc!<>&^%$', 'varcharlengthis4096字符类型测试%%&!+varcharlengthis4096字符类型测试%%&!+',
'nvarchar-1', 'nvarchar255nvarchar255()&^%{}','nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}',
'y', b'bytes', 'varbytes_2', b'\xdd\xee\xff', b'\xee\xee\xee\xff\xff\xff\xee\xee\xee\xff\xff\xff','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2023-05-15 09:10:12.345',111111110011,100,10000,100000,1000000.101,100000000.1010111,false,
'c','char1','n','nchar255定长类型^&@#nchar255定长类型^&@#nchar255定长类型^&@#',
'varchar_11','varchar255测试cdf~#varchar255测试cdf~#','varcharlengthis4096字符类型测试%%&!+varcharlengthis4096字符类型测试%%&!+',
'nvarchar-0','nvarchar255()&^%{}','nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}',
'b','bytes1023','varbytes_1',b'\xaa\xbb\xcc',b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xdd\xdd\xdd','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2023-07-10 20:18:43.446', 111111110044, 300, 30000, 300000, 3000000.303, 300000000.3030333, false,
'r', 'char3', 'a', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_44', 'varchar255_中文测试abc!<>&^%$varchar255_中文测试abc!<>&^%$', 'varcharlengthof4096通用查询test%%&!+varcharlengthof4096通用查询test%%&!+',
'nvarchar-2', 'nvarchar255()&^%{}', 'nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}',
'e', b'byte12', 'varbytes_3', b'\xbb\xcc\xff', 'varbytes4096$%^varbytes4096$%^varbytes4096$%^','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2023-07-10 20:18:43.145', 111111110033, 700, 70000, 700000, 7000000.707, 700000000.7070777, true,
'a', 'char4', 'h', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_33', 'varchar255_123456中文测试abc!<>&^%$varchar255_123456中文测试abc!<>&^%$', 'varcharlengthof4096字符类型test%%&!+varcharlengthof4096字符类型test%%&!+',
'nvarchar-3', 'nvarchar255_+!@#nvarchar255_+!@#', 'nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}',
't', b'bytes', 'varbytes_2', b'\xaa\xdd\xee', b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xdd\xdd\xdd','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2022-06-16 10:18:30.123',111111110011,100,10000,100000,1000000.101,100000000.1010111,false,
'c','char1','n','nchar255定长类型^&@#nchar255定长类型^&@#nchar255定长类型^&@#',
'varchar_11','varchar255测试cdf~#varchar255测试cdf~#','varcharlengthis4096字符类型测试%%&!+varcharlengthis4096字符类型测试%%&!+',
'nvarchar-0','nvarchar255()&^%{}','nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}',
'b','bytes1023','varbytes_1',b'\xaa\xbb\xcc',b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xdd\xdd\xdd','stb_2',null,null,null,null,null,null,null,'shanghai',null,'blue',null);
insert into test_last.stb values('2022-06-16 10:18:35.981', 111111110022, 200, 20000, 200000, 2000000.202, 200000000.2020222, true,
'h', 'char2', 'c', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_22', 'varchar255中文测试abc!<>&^%$varchar255中文测试abc!<>&^%$', 'varcharlengthis4096字符类型测试%%&!+varcharlengthis4096字符类型测试%%&!+',
'nvarchar-1', 'nvarchar255nvarchar255()&^%{}','nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}',
'y', b'bytes', 'varbytes_2', b'\xdd\xee\xff', b'\xee\xee\xee\xff\xff\xff\xee\xee\xee\xff\xff\xff','stb_2',null,null,null,null,null,null,null,'shanghai',null,'blue',null);
insert into test_last.stb values('2023-02-10 12:05:51.445',111111110000,100,10000,100000,1000000.101,100000000.1010111,true,
'','','','','','','','','','','','','','','','stb_3',null,null,null,null,null,null,null,'shanghai',null,'blue',null);
insert into test_last.stb values('2023-02-10 12:05:52.556', 111111110033, 700, 70000, 700000, 7000000.707, 700000000.7070777, true,
'a', 'char4', 'h', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_33', 'varchar255_123456中文测试abc!<>&^%$varchar255_123456中文测试abc!<>&^%$', 'varcharlengthof4096字符类型test%%&!+varcharlengthof4096字符类型test%%&!+',
'nvarchar-3', 'nvarchar255_+!@#nvarchar255_+!@#', 'nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}',
't', b'bytes', 'varbytes_2', b'\xaa\xdd\xee', b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xdd\xdd\xdd','stb_3',null,null,null,null,null,null,null,'shanghai',null,'blue',null);
insert into test_last.stb values('2023-02-10 12:05:50.166', 111111110044, 300, 30000, 300000, 3000000.303, 300000000.3030333, false,
'r', 'char3', 'a', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
'varchar_44', 'varchar255_中文测试abc!<>&^%$varchar255_中文测试abc!<>&^%$', 'varcharlengthof4096通用查询test%%&!+varcharlengthof4096通用查询test%%&!+',
'nvarchar-2', 'nvarchar255()&^%{}', 'nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}',
'e', b'byte12', 'varbytes_3', b'\xbb\xcc\xff', 'varbytes4096$%^varbytes4096$%^varbytes4096$%^','stb_3',null,null,null,null,null,null,null,'shanghai',null,'blue',null);


select time_bucket(k_timestamp, '10s') as tb, last(e20),last(e21),last(e22) from test_last.tb group by tb order by tb;
select time_bucket(k_timestamp, '10s') as tb, last_row(e20),last_row(e21),last_row(e22) from test_last.tb group by tb order by tb;
select time_bucket(k_timestamp, '15s') as tb, last_row(e8),last_row(e12) from test_last.tb where e8 in(' ','c','h') group by tb order by tb;

select time_bucket(k_timestamp, '10s') as tb, last(e20),last(e21),last(e22) from test_last.stb group by tb order by tb;
select time_bucket(k_timestamp, '10s') as tb, last_row(e20),last_row(e21),last_row(e22) from test_last.stb group by tb order by tb;
select time_bucket(k_timestamp, '15s') as tb, last_row(e8),last_row(e9) from test_last.stb where e8 in(' ','c','h','r') group by tb order by tb;
select time_bucket(k_timestamp, '15s') as tb, last(e12),last(e15) from test_last.stb group by tb,att7 order by tb;
select time_bucket(k_timestamp, '15s') as tb, last_row(e12),last_row(e15) from test_last.stb group by tb,att7 order by tb;

drop database test_last cascade;

-- ZDP-24522
create ts database test_last;
create table test_last.tb (k_timestamp timestamp not null,e1 timestamp,e2 int2,e3 int,e4 int8,e5 float,e6 float8,e7 bool,e8 char,e9 char(1023),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(255),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(1023),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (att1 int2 not null,att2 int,att3 int8,att4 bool,att5 float4,att6 float8,att7 varchar,att8 varchar(64)) primary tags (att1);
create table test_last.stb (k_timestamp timestamp not null,e1 timestamp,e2 int2,e3 int,e4 int8,e5 float,e6 float8,e7 bool,e8 char,e9 char(1023),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(255),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(1023),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (name varchar(10) not null, att1 int2,att2 int,att3 int8,att4 bool,att5 float4,att6 float8,att7 varchar,att8 varchar(3000),att9 char(1000),att10 varbytes(10),att11 varbytes(100)) primary tags (name);
insert into test_last.tb values('2022-03-03 03:13:00.133',111111110000,100,10000,100000,1000000.101,100000000.1010111,true,
                                '','','','',
                                '','','',
                                '','','',
                                '','','','','', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2022-02-03 13:13:11',111111110000,100,10000,100000,1000000.101,100000000.1010111,true,
                                '','','','',
                                '','','',
                                '','','',
                                '','','','','', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2023-01-15 21:36:21.22', 111111110033,600,60000,600000,6000000.606,600000000.6060666,false,
                                'h', 'char2', 'c', 'nchar255定长类型测试1测试2测试3()&^%{}',
                                'varchar_22', 'varchar255中文测试abc!<>&^%$', 'varcharlengthis4096字符类型测试%%&!+',
                                'nvarchar-1', 'nvarchar255nvarchar255()&^%{}','nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}',
                                'y', b'bytes', 'varbytes_2', b'\xdd\xee\xff', b'\xee\xee\xee\xff\xff\xff\xee\xee\xee\xff\xff\xff', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2018-05-05 14:13:45.113',111111110033,600,60000,600000,6000000.606,600000000.6060666,false,
                                'c','char1','n','nchar255定长类型^&*@#',
                                'varchar_11','varchar255测试cdf~#varchar255测试cdf~#','varcharlengthis4096字符类型测试%%&!+',
                                'nvarchar-0','nvarchar255()&^%{}','nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}',
                                'b','bytes1023','varbytes_2',b'\xaa\xbb\xcc',b'\xaa\xaa\xaa\xbb\xbb\xbb', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2023-07-10 20:00:00', 111111110011, 700, 70000, 700000, 7000000.707, 700000000.7070777, true,
                                'r', 'char3', 'a', 'nchar255定长类型测试1测试2测试3()&^%{}',
                                'varchar_44', 'varchar255_中文测试abc!<>&^%$', 'varcharlengthof4096通用查询test%%&!+',
                                'nvarchar-2', 'nvarchar255()&^%{}', 'nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}',
                                'e', b'byte12', 'varbytes_3', b'\xbb\xcc\xff', 'varbytes4096$%^varbytes4096$%^varbytes4096$%^', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2023-07-02 08:10:00', 111111110011, 700, 70000, 700000, 7000000.707, 700000000.7070777, true,
                                'a', 'char4', 'h', 'nchar255定长类型测试1测试2测试3()&^%{}',
                                'varchar_33', 'varchar255_123456中文测试abc!<>&^%$', 'varcharlengthof4096字符类型test%%&!+',
                                'nvarchar-3', 'nvarchar255_+!@#nvarchar255_+!@#', 'nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}',
                                't', b'bytes', 'varbytes_3', b'\xaa\xdd\xee', b'\xaa\xaa\xaa\xbb\xbb\xbb', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.stb values('2018-01-10 10:08:15.113',111111110000,100,10000,100000,1000000.101,100000000.1010111,true,
                                   '','','','',
                                   '','','',
                                   '','','',
                                   '','','','','','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2022-08-28 08:58:16',111111110011,100,10000,100000,1000000.101,100000000.1010111,false,
                                   'c','char1','n','nchar255定长类型^&@#',
                                   'varchar_11','varchar255测试cdf~#varchar255测试cdf~#','varcharlengthis4096字符类型测试%%&!+',
                                   'nvarchar-0','nvarchar255()&^%{}','nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}',
                                   'b','bytes1023','varbytes_1',b'\xaa\xbb\xcc',b'\xaa\xaa\xaa\xbb\xbb\xbb','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2022-09-15 11:23:12.712', 111111110022, 200, 20000, 200000, 2000000.202, 200000000.2020222, true,
                                   'h', 'char2', 'c', 'nchar255定长类型测试1测试2测试3()&^%{}',
                                   'varchar_22', 'varchar255中文测试abc!<>&^%$', 'varcharlengthis4096字符类型测试%%&!+',
                                   'nvarchar-1', 'nvarchar255nvarchar255()&^%{}','nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}',
                                   'y', b'bytes', 'varbytes_2', b'\xdd\xee\xff', b'\xee\xee\xee\xff\xff\xff\xee\xee\xee\xff\xff\xff','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2023-07-10 20:00:00', 111111110033, 700, 70000, 700000, 7000000.707, 700000000.7070777, true,
                                   'a', 'char4', 'h', 'nchar255定长类型测试1测试2测试3()&^%{}',
                                   'varchar_33', 'varchar255_123456中文测试abc!<>&^%$', 'varcharlengthof4096字符类型test%%&!+',
                                   'nvarchar-3', 'nvarchar255+!@#nvarchar255_+!@#', 'nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}',
                                   't', b'bytes', 'varbytes_2', b'\xaa\xdd\xee', b'\xaa\xaa\xaa\xbb\xbb\xbb','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2022-08-02 08:10:00', 111111110044, 300, 30000, 300000, 3000000.303, 300000000.3030333, false,
                                   'r', 'char3', 'a', 'nchar255定长类型测试1测试2测试3()&^%{}',
                                   'varchar_44', 'varchar255_中文测试abc!<>&^%$', 'varcharlengthof4096通用查询test%%&!+_',
                                   'nvarchar-2', 'nvarchar255()&^%{}', 'nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}',
                                   'e', b'byte12', 'varbytes_3', b'\xbb\xcc\xff', 'varbytes4096$%^varbytes4096$%^varbytes4096$%^','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2017-07-07 17:46:15',111111110011,100,10000,100000,1000000.101,100000000.1010111,false,
                                   'c','char1','n','nchar255定长类型^&@#',
                                   'varchar_11','varchar255测试cdf~#varchar255测试cdf~#','varcharlengthis4096字符类型测试%%&!+',
                                   'nvarchar-0','nvarchar255()&^%{}','nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}',
                                   'b','bytes1023','varbytes_1',b'\xaa\xbb\xcc',b'\xaa\xaa\xaa\xbb\xbb\xbb','stb_2',null,null,null,null,null,null,null,'shanghai',null,'blue',null);
insert into test_last.stb values('2020-03-30 05:06:00', 111111110022, 200, 20000, 200000, 2000000.202, 200000000.2020222, true,
                                   'h', 'char2', 'c', 'nchar255定长类型测试1测试2测试3()&^%{}',
                                   'varchar_22', 'varchar255中文测试abc!<>&^%$', 'varcharlengthis4096字符类型测试%%&!+',
                                   'nvarchar-1', 'nvarchar255nvarchar255()&^%{}','nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}',
                                   'y', b'bytes', 'varbytes_2', b'\xdd\xee\xff', b'\xee\xee\xee\xff\xff\xff\xee\xee\xee\xff\xff\xff','stb_2',null,null,null,null,null,null,null,'shanghai',null,'blue',null);
insert into test_last.stb values('2023-06-10 06:10:53',111111110000,100,10000,100000,1000000.101,100000000.1010111,true,
                                   '','','','',
                                   '','','',
                                   '','','',
                                   '','','','','','stb_3',null,null,null,null,null,null,null,'shanghai',null,'blue',null);
insert into test_last.stb values('2023-07-02 08:10:00', 111111110033, 700, 70000, 700000, 7000000.707, 700000000.7070777, true,
                                   'a', 'char4', 'h', 'nchar255定长类型测试1测试2测试3()&^%{}',
                                   'varchar_33', 'varchar255_123456中文测试abc!<>&^%$', 'varcharlengthof4096字符类型test%%&!+',
                                   'nvarchar-3', 'nvarchar255+!@#nvarchar255_+!@#', 'nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}',
                                   't', b'bytes', 'varbytes_2', b'\xaa\xdd\xee', b'\xaa\xaa\xaa\xbb\xbb\xbb','stb_3',null,null,null,null,null,null,null,'shanghai',null,'blue',null);
insert into test_last.stb values('2023-05-10 20:20:00', 111111110044, 300, 30000, 300000, 3000000.303, 300000000.3030333, false,
                                   'r', 'char3', 'a', 'nchar255定长类型测试1测试2测试3()&^%{}',
                                   'varchar_44', 'varchar255_中文测试abc!<>&^%$', 'varcharlengthof4096通用查询test%%&!+_',
                                   'nvarchar-2', 'nvarchar255()&^%{}', 'nvarchar4096中英文测试1中英文测试1中英文测试1()*&^%{}',
                                   'e', b'byte12', 'varbytes_3', b'\xbb\xcc\xff', 'varbytes4096$%^varbytes4096$%^varbytes4096$%^','stb_3',null,null,null,null,null,null,null,'shanghai',null,'blue',null);

select last(*) from test_last.stb group by e7 order by e7 desc;
select last_row(*) from test_last.stb group by e7 order by e7 desc;

create table test_last.t1(ts timestamp not null,a int, b int) tags(tag1 int not null, tag2 int) primary tags(tag1);
create statistics st from test_last.t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 'st';
select last(*) from test_last.t1;

delete from system.table_statistics where name = 'st';
drop database test_last cascade;