create ts database d1;
use d1;

CREATE TABLE tb1
(
    k_timestamp TIMESTAMPTZ NOT NULL,
    e1          INT2,
    e2          INT,
    e3          INT8,
    e4          FLOAT4,
    e5          FLOAT,
    e6          BOOL,
    e7          TIMESTAMP,
    e8          CHAR(1023),
    e9          NCHAR(255),
    e10         varbytes(255)
) TAGS (tag1    CHAR(63) NOT NULL,
        tag2    CHAR(10) NOT NULL,
        tag3    INT2 NOT NULL,
        tag4    INT NOT NULL,
        tag5    INT8 NOT NULL,
        tag6    FLOAT4 NOT NULL,
        tag7    FLOAT NOT NULL,
        tag8    BOOL NOT NULL,
        tag9    CHAR(1023) NOT NULL,
        tag10   NCHAR(64) NOT NULL,
        tag11   varbytes(255) NOT NULL
)PRIMARY TAGS (tag4, tag5, tag8);

INSERT INTO tb1 VALUES(10001,10001,1000001,100000001,110011.110011,110011.110011,true,'2010-10-10 10:10:11.101','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',b'\x61','《test1中文YingYu@!!!》','ATEST(1'
                      ,10001,1000001,100000001,110011.110011,110011.110011,true,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',b'\x61');
INSERT INTO tb1 VALUES(2200000000200,20002,2000002,200000002,220022.220022,220022.220022,false,'2020-12-12 00:10:59.222','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',b'\x80','《test2中文YingYu@!!!》','BTEST(2'
                      ,20002,2000002,200000002,220022.220022,220022.220022,false,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',b'\x80');
INSERT INTO tb1 VALUES(3003000000003,30003,3000003,300000003,330033.330033,330033.33033,true,'2230-3-30 10:10:00.3','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',b'\xcc','《test3中文YingYu@!!!》','CTEST(3'
                      ,30003,3000003,300000003,330033.330033,330033.33033,true,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',b'\xcc');

CREATE TABLE tb1_1
(
    k_timestamp TIMESTAMPTZ NOT NULL,
    e1          INT2,
    e2          INT,
    e3          INT8,
    e4          FLOAT4,
    e5          FLOAT,
    e6          BOOL,
    e7          TIMESTAMP,
    e8          CHAR(1023),
    e9          NCHAR(255),
    e10         varbytes(255)
) TAGS (tag1    CHAR(63) NOT NULL,
        tag2    CHAR(10) NOT NULL,
        tag3    INT2 NOT NULL,
        tag4    INT NOT NULL,
        tag5    INT8 NOT NULL,
        tag6    FLOAT4 NOT NULL,
        tag7    FLOAT NOT NULL,
        tag8    BOOL NOT NULL,
        tag9    CHAR(1023) NOT NULL,
        tag10   NCHAR(64) NOT NULL,
        tag11   varbytes(255) NOT NULL
)PRIMARY TAGS (tag1, tag2, tag3);

INSERT INTO tb1_1 VALUES(10001,10001,1000001,100000001,110011.110011,110011.110011,true,'2010-10-10 10:10:11.101','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',b'\x61','《test1中文YingYu@!!!》','ATEST(1'
                      ,10001,1000001,100000001,110011.110011,110011.110011,true,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',b'\x61');
INSERT INTO tb1_1 VALUES(2200000000200,20002,2000002,200000002,220022.220022,220022.220022,false,'2020-12-12 00:10:59.222','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',b'\x80','《test2中文YingYu@!!!》','BTEST(2'
                      ,20002,2000002,200000002,220022.220022,220022.220022,false,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',b'\x80');
INSERT INTO tb1_1 VALUES(3003000000003,30003,3000003,300000003,330033.330033,330033.33033,true,'2230-3-30 10:10:00.3','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',b'\xcc','《test3中文YingYu@!!!》','CTEST(3'
                      ,30003,3000003,300000003,330033.330033,330033.33033,true,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',b'\xcc');

CREATE TABLE tb2
(
    k_timestamp TIMESTAMPTZ NOT NULL,
    e1          INT2,
    e2          INT,
    e3          INT8,
    e4          FLOAT4,
    e5          FLOAT,
    e6          BOOL,
    e7          TIMESTAMP,
    e8          CHAR(1023),
    e9          NCHAR(255),
    e10         varbytes(255)
) TAGS (tag1    CHAR(63) NOT NULL,
        tag2    CHAR(10) NOT NULL,
        tag3    INT2 NOT NULL,
        tag4    INT NOT NULL,
        tag5    INT8 NOT NULL,
        tag8    BOOL NOT NULL,
        tag9    VARCHAR(1023) NOT NULL,
        tag10   VARCHAR(255) NOT NULL,
        tag11   VARCHAR(255) NOT NULL
)PRIMARY TAGS (tag1, tag2, tag3);

INSERT INTO tb2 VALUES(10001,10001,1000001,100000001,110011.110011,110011.110011,true,'2010-10-10 10:10:11.101','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',b'\x61','《test1中文YingYu@!!!》','ATEST(1'
                      ,10001,1000001,100000001,true,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',b'\x61');
INSERT INTO tb2 VALUES(2200000000200,20002,2000002,200000002,220022.220022,220022.220022,false,'2020-12-12 00:10:59.222','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',b'\x80','《test2中文YingYu@!!!》','BTEST(2'
                      ,20002,2000002,200000002,false,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',b'\x80');
INSERT INTO tb2 VALUES(3003000000003,30003,3000003,300000003,330033.330033,330033.33033,true,'2230-3-30 10:10:00.3','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',b'\xcc','《test3中文YingYu@!!!》','CTEST(3'
                      ,30003,3000003,300000003,true,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',b'\xcc');

CREATE TABLE tb2_1
(
    k_timestamp TIMESTAMPTZ NOT NULL,
    e1          INT2,
    e2          INT,
    e3          INT8,
    e4          FLOAT4,
    e5          FLOAT,
    e6          BOOL,
    e7          TIMESTAMP,
    e8          CHAR(1023),
    e9          NCHAR(255),
    e10         varbytes(255)
) TAGS (tag1    CHAR(63) NOT NULL,
        tag2    CHAR(10) NOT NULL,
        tag3    INT2 NOT NULL,
        tag4    INT NOT NULL,
        tag5    INT8 NOT NULL,
        tag8    BOOL NOT NULL,
        tag9    VARCHAR(1023) NOT NULL,
        tag10   VARCHAR(64) NOT NULL,
        tag11   VARCHAR(64) NOT NULL
)PRIMARY TAGS (tag10, tag11);

INSERT INTO tb2_1 VALUES(10001,10001,1000001,100000001,110011.110011,110011.110011,true,'2010-10-10 10:10:11.101','《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',b'\x61','《test1中文YingYu@!!!》','ATEST(1'
                      ,10001,1000001,100000001,true,'《test1中文YingYu@!!!》','《test1中文YingYu@!!!》',b'\x61');
INSERT INTO tb2_1 VALUES(2200000000200,20002,2000002,200000002,220022.220022,220022.220022,false,'2020-12-12 00:10:59.222','《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',b'\x80','《test2中文YingYu@!!!》','BTEST(2'
                      ,20002,2000002,200000002,false,'《test2中文YingYu@!!!》','《test2中文YingYu@!!!》',b'\x80');
INSERT INTO tb2_1 VALUES(3003000000003,30003,3000003,300000003,330033.330033,330033.33033,true,'2230-3-30 10:10:00.3','《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',b'\xcc','《test3中文YingYu@!!!》','CTEST(3'
                      ,30003,3000003,300000003,true,'《test3中文YingYu@!!!》','《test3中文YingYu@!!!》',b'\xcc');

show create table tb1;
show create table tb1_1;
show create table tb2;
show create table tb2_1;

show columns from tb1;
show columns from tb2;
show columns from tb1_1;
show columns from tb2_1;

show tags from tb1;
show tags from tb2;
show tags from tb1_1;
show tags from tb2_1;

show tag values from tb1;
show tag values from tb2;
show tag values from tb1_1;
show tag values from tb2_1;

-- fix-bug-31898
create table "tag" (ts timestamp not null, a int) tags(b int not null) primary tags(b);
create table "table" (ts timestamp not null, a int) tags(b int not null) primary tags(b);
create table t4 (ts timestamp not null, a int) tags("tag" int not null) primary tags("tag");
create table t5 (ts timestamp not null, a int) tags("table" int not null) primary tags("table");
insert into "tag" values(0,1,1);
insert into "table" values(0,1,1);
insert into t4 values(0,1,1);
insert into t5 values(0,1,1);
show tag values from "tag";
show tag values from "table";
show tag values from t4;
show tag values from t5;

--- fix-bug-32001
create table test__add_col(ts timestamp not null, a int) tags(b int not null) primary tags(b);
insert into test__add_col values(0,1,1);
show tag values from test__add_col;
alter table test__add_col add if not exists c1 int8;
show tag values from test__add_col;

-- ZDP-36659
select * from [show tag values from tb1] where tag8 = false order by tag8;
select * from [show tag values from tb1] where tag8 = true order by tag8,tag1;

--- fix-bug-37003
select count(*) from [show tag values from tb1];
select count(*) from [show tag values from tb1];
select count(*) from [show tag values from tb1] where tag8 = false;
select count(*) from [show tag values from tb1] where tag8 = true;

drop database d1 cascade;

create ts database tsdb;
use tsdb;
CREATE TABLE s11(k_timestamp TIMESTAMPTZ NOT NULL,"1e" INT8 ) TAGS (primary_tag VARCHAR(64) NOT NULL,"1t" VARCHAR(254) ) PRIMARY TAGS(primary_tag);
show create s11;
CREATE TABLE s12(k_timestamp TIMESTAMPTZ NOT NULL,"1e" INT8 ) TAGS ("1p" VARCHAR(64) NOT NULL,"1t" VARCHAR(254) ) PRIMARY TAGS("1p");
show create s12;

drop database tsdb cascade;
