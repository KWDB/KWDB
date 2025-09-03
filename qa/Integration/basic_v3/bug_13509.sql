create ts database test_genquery1;
create TABLE test_genquery1.d1 (
k_timestamp timestamp not null,
e1 timestamp not null,
e2 smallint not null,
e3 int not null,
e4 bigint not null,
e5 float not null,
e6 double not null,
e7 char(11) not null,
e8 bool not null,
e9 varbytes(10) not null)
tags (tag1 int not null) primary tags(tag1);

INSERT INTO test_genquery1.d1 VALUES(111111111000,111111111000,1,1,1,100000000000000000.101,100000000000000000.10101010101,'tes中文!1',true,'1010101010', 1);
INSERT INTO test_genquery1.d1 VALUES(111111112000,222222222000,1,2,3,200000000000000000.202,200000000000000000.20202020202,'tes中文!2',false,'0101010101', 1);
INSERT INTO test_genquery1.d1 VALUES(111111113000,333333333000,1,1,3,300000000000000000.303,300000000000000000.30303030303,'tes中文!3',true,'0101010110', 1);

SELECT e2 from test_genquery1.d1 WHERE k_timestamp < now() ORDER BY d1.k_timestamp;

drop database test_genquery1 cascade;

--bug 13911 and bug 13901
create ts database test_char1;
create table test_char1.d1 (
k_timestamp timestamp not null,
e0 int not null,
e1 char(20) not null,
e2 nchar(20) not null,
e3 varchar(20) not null,
e4 nvarchar(20) not null,
e5 varbytes(20) not null,
e6 varbytes(20) not null )
tags (tag1 int not null) primary tags(tag1);

INSERT INTO test_char1.d1 VALUES(now(),1,'中文！!1','中文！!1','中文！!1','中文！!1','中文！!1','中文！!1', 1);
INSERT INTO test_char1.d1 VALUES(now(),2,'中文！!1','中文！!1','中文！!1','中文！!1','中文！!1','中文！!1', 1);
INSERT INTO test_char1.d1 VALUES(now(),3,'中文！!1','中文！!1','中文！!1','中文！!1','中文！!1','中文！!1', 1);
INSERT INTO test_char1.d1 VALUES(now(),4,'中文！!1','中文！!1','中文！!1','中文！!1','中文！!1','中文！!1', 1);
INSERT INTO test_char1.d1 VALUES(now(),5,'中文！!1','中文！!1','中文！!1','中文！!1','中文！!1','中文！!1', 1);
INSERT INTO test_char1.d1 VALUES(now(),6,'中文！!1','中文！!1','中文！!1','中文！!1','中文！!1','中文！!1', 1);

select k_timestamp from test_char1.d1 where k_timestamp < '1970-03-22 02:06:01.445' ;

drop database test_char1 cascade;