-- test varchar, varbytes, varbinary types
create ts database test_last;
create table test_last.tb (k_timestamp timestamp not null,e1 varchar,e2 varchar(32),e3 nvarchar,e4 nvarchar(32),e5 varbytes,e6 varbytes(32)) attributes (attr1 varchar(32) not null, attr2 int not null) primary tags(attr1, attr2);
create table test_last.stb (k_timestamp timestamp not null,e1 varchar,e2 varchar(32),e3 nvarchar,e4 nvarchar(32),e5 varbytes,e6 varbytes(32)) attributes (name varchar(10) not null, attr1 varchar(32), attr2 int) primary tags(name);

insert into test_last.tb values('2022-03-03 03:13:00.133','','','','','','', '', 1);
insert into test_last.tb values('2018-05-05 14:13:45.113','varchar_1','varchar_11','nvarchar_1','nvarchar_11','varbytes_1',b'\xaa\xbb\xdd', '', 1);
insert into test_last.tb values('2023-01-15 21:36:21.22','varchar_2','varchar_22','nvarchar_2','nvarchar_22','varbytes_2',b'\xaa\xbb\xee', '', 1);
insert into test_last.tb values('2023-07-10 20:00:00','varchar_3','varchar_33','nvarchar_3','nvarchar_33','varbytes_3',b'\xaa\xbb\xff', '', 1);
insert into test_last.tb values('2023-07-02 08:10:00','varchar_4','varchar_44','nvarchar_4','nvarchar_44','varbytes_4',b'\xaa\xbb\x00', '', 1);

insert into test_last.stb values('2022-03-03 03:13:00.133','','','','','','', 'stb_1', 'beijing',2);
insert into test_last.stb values('2018-05-05 14:13:45.113','varchar_11','varchar_111','nvarchar_11','nvarchar_111','varbytes_11',b'\xaa\xbb\xcc', 'stb_1', 'beijing',2);
insert into test_last.stb values('2023-01-15 21:36:21.22','varchar_12','varchar_122','nvarchar_12','nvarchar_122','varbytes_12',b'\xaa\xbb\xbb', 'stb_1', 'beijing',2);

insert into test_last.stb values('2022-03-04 03:13:00.133','','','','','','', 'stb_2', 'shanghai',3);
insert into test_last.stb values('2023-07-10 20:00:00','varchar_21','varchar_211','nvarchar_21','nvarchar_211','varbytes_21',b'\xaa\xbb\xdd', 'stb_2', 'shanghai',3);
insert into test_last.stb values('2023-07-02 08:10:00','varchar_22','varchar_222','nvarchar_22','nvarchar_222','varbytes_22',b'\xaa\xbb\xaa', 'stb_2', 'shanghai',3);

insert into test_last.stb values('2023-05-10 20:20:00','varchar_31','varchar_311','nvarchar_31','nvarchar_311','varbytes_31',b'\xaa\xbb\xff', 'stb_3', 'shanghai',4);
insert into test_last.stb values('2023-07-02 08:10:00','varchar_32','varchar_322','nvarchar_32','nvarchar_322','varbytes_32',b'\xaa\xbb\xee', 'stb_3', 'shanghai',4);

select last(k_timestamp),last(e1) from test_last.tb;
select last(k_timestamp),last(e3) from test_last.tb;
select last(k_timestamp),last(e5) from test_last.tb;

select last(k_timestamp),last(e1),last(e3),last(e5) from test_last.tb;

select last(e1) from test_last.tb;

select last(*) from test_last.tb;
select last_row(*) from test_last.tb;

select last(*) from test_last.stb;
select last_row(*) from test_last.stb;
select last(*) from test_last.stb group by attr1 order by attr1;
select last_row(*) from test_last.stb group by attr1 order by attr1;
select last(*) from test_last.stb group by attr2 order by attr2;
select last_row(*) from test_last.stb group by attr2 order by attr2;
select last(*) from test_last.stb group by attr1, attr2 order by attr1, attr2;
select last_row(*) from test_last.stb group by attr1, attr2 order by attr1, attr2;

select last(*) from test_last.stb where name = 'stb_1';
select last_row(*) from test_last.stb where name = 'stb_1';
select last(*) from test_last.stb where name = 'stb_2';
select last_row(*) from test_last.stb where name = 'stb_2';
select last(*) from test_last.stb where name = 'stb_3';
select last_row(*) from test_last.stb where name = 'stb_3';

drop database test_last cascade;