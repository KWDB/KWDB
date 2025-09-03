create ts database test_select4;

CREATE TABLE test_select4.t1 (ts TIMESTAMPTZ(3) NOT NULL, e1 TIMESTAMP(3) NULL,     e2 INT2 NULL,     e3 INT4 NULL,     e4 INT8 NULL,     e5 FLOAT4 NULL,     e6 FLOAT8 NULL,     e7 BOOL NULL,     e8 CHAR NULL,     e9 CHAR(64) NULL,     e10 NCHAR NULL,     e11 NCHAR(64) NULL,     e12 VARCHAR(254) NULL,     e13 VARCHAR(64) NULL,     e14 NVARCHAR(63) NULL,     e15 NVARCHAR(64) NULL,     e16 VARBYTES(254) NULL,     e17 VARBYTES(64) NULL ) TAGS (     tag1 BOOL NOT NULL,     tag2 INT2 NOT NULL,     tag3 INT4 NOT NULL,     tag4 INT8 NOT NULL,     tag5 FLOAT4,     tag6 FLOAT8,     tag7 VARBYTES(254),     tag8 VARBYTES(64),     tag9 CHAR(1),     tag10 CHAR(64),     tag11 NCHAR(1),     tag12 NCHAR(64),     tag13 VARCHAR(254),     tag14 VARCHAR(64) ) PRIMARY TAGS(tag1);
insert into test_select4.t1 values
('2022-02-02 03:11:11', 1643771471000, 32767, 2147483647, 9223372036854775807, 1.2e+3, 1.655, true, 'a', 'aaa', '一', '一一一', '一', '一一一','一', '一一一', 'a', 'aa', true, 111, 1111, 11111, 1.11, 1.23e+2, 'a', 'aa', 'a', 'aaa', '一', '一一一', '一', '一一一'),
('2022-02-02 03:11:12', 1643771472000, 32766, 2147483646, 9223372036854775806, 1.2e+4, 2.655, false, 'b', 'bbb', '二', '二二二', '二', '二二二','二', '二二二', 'b', 'bb', true, 111, 1111, 11111, 2.11,2.23e+2, 'b', 'bb', 'a', 'aaa', '一', '一一一', '一', '一一一'),
('2022-02-02 03:11:13', 1643771473000, 32765, 2147483645, 9223372036854775805, 2.2e+3, 3.655, true, 'c', 'ccc', '三', '三三三', '三', '三三三', '三', '三三三', 'c', 'cc', false, 333, 3333, 33333, 3.11,3.23e+2, 'c', 'cc', 'c', 'ccc', '三', '三三三', '三', '三三三'),
('2022-02-02 03:11:14', 1643771474000, 32764, 2147483644, 9223372036854775804, 3.2e+3, 4.655, false, 'd', 'ddd', '四', '四四四', '四', '四四四','四', '四四四', 'd', 'dd', true, 444, 4444, 44444, 4.11,4.23e+2, 'd', 'dd', 'd', 'ddd', '四', '四四四', '四', '四四四'),
('2022-02-02 03:11:15', 1643771475000, 32763, 2147483643, 9223372036854775803, 4.2e+3, 5.655, true, 'e', 'eee', '五', '五五五', '五', '五五五','五', '五五五', 'e', 'ee',  true, 444, 4444, 44444, 5.11,5.23e+2, 'e', 'ee', 'd', 'ddd', '四', '四四四', '四', '四四四'),
('2022-02-02 03:11:16', 1643771476000, 32762, 2147483642, 9223372036854775802, 5.2e+3, 6.655, false, 'f', 'fff', '六', '六六六', '六', '六六六','六', '六六六', 'f', 'ff', false, 666, 6666, 66666, 6.11,6.23e+2, 'f', 'ff', 'f', 'fff', '五', '五五五', '五', '五五五');
select * from test_select4.t1 where e1 between 1643771471000 and 1643771476000;
prepare pa as select * from test_select4.t1 where e1 between $1 and $2;
execute pa(1643771471000,1643771476000);
drop database test_select4 cascade;

create ts database test_select3;
create table test_select3.t_int2(ts timestamptz not null, e1 int2 not null, e2 int4 not null, e3 int8 not null)  tags(tag1 int2 not null, tag2 int4 not null, tag3 int8 not null) primary tags(tag1);
insert into test_select3.t_int2 values (1643771471000,32767,2147483647,9223372036854775807,32767,2147483647,9223372036854775807),                
(1643771472000,-32768,-2147483648,-9223372036854775808,-32768,-32768,-9223372036854775808),                
(1643771473000,-444,-4444,-4444,-444,-4444,-44444),                
(1643771474000,-444,-4444,-4444,-444,-4444,-44444),                
(1643771475000,32767,9999,99999,32767,66666,66666); 
select * from test_select3.t_int2 where tag1='-444' order by ts;
prepare p1 as  select * from test_select3.t_int2 where tag1=$1  order by ts;
execute p1('-444');
drop database test_select3 cascade;

create ts database test_select1e;
create table test_select1e.t_int2(ts timestamptz not null, e1 int2 not null, e2 int4 not null, e3 int8 not null)  tags(tag1 int2 not null, tag2 int4 not null, tag3 int8 not null) primary tags(tag1);
insert into test_select1e.t_int2 values(1643771471000,32767,2147483647,9223372036854775807,32767,2147483647,9223372036854775807), 
(1643771472000,-32768,-2147483648,-9223372036854775808,-32768,-32768,-9223372036854775808), 
(1643771473000,-444,-4444,-4444,-444,-4444,-44444), 
(1643771474000,-444,-4444,-4444,-444,-4444,-44444), 
(1643771475000,32767,9999,99999,666,66666,66666); 
select * from test_select1e.t_int2 where e1 between 32767 and 32768 order by ts;
prepare a1 as  select * from test_select1e.t_int2 where e1 between $1 and $2 order by ts;execute a1 (32767,32768);
drop database test_select1e cascade;