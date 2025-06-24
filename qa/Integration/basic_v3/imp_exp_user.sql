--test basic user/role
create user u1;
create user u2;
create user u3;
create role r1;
create role r2;
create role r3;
--test login
create user u4 with nologin;
create role r4 with login;
--test valid until
create user u5 with valid until '2025-01-22';
create user r5 with valid until '2025-01-22';
--test muti-option
create user u6 with nologin valid until '2025-01-22';
create user r6 with login valid until '2025-01-22';
--test member of
grant r1 to u1;
grant u2 to r2;
grant r3 to u4 with admin option;

--export user/role
export users to sql "nodelocal://1/user";

--clean user/role/
drop user u1;
drop user u2;
drop user u3;
drop user u4;
drop user u5;
drop user u6;
drop role r1;
drop role r2;
drop role r3;
drop role r4;
drop role r5;
drop role r6;

--import user/role
import users sql data ("nodelocal://1/user/users.sql");
select * from system.users;
show users;

--clean user/role/
drop user u1;
drop user u2;
drop user u3;
drop user u4;
drop user u5;
drop user u6;
drop role r1;
drop role r2;
drop role r3;
drop role r4;
drop role r5;
drop role r6;

CREATE TS DATABASE db;
USE db;
CREATE TABLE db.t1( "time" timestamptz not null,e8 char(1023), e9 nchar(255))tags(i int not null) primary tags(i);

INSERT INTO db.t1 values('2023-12-1 12:00:12.000+00:00',null, '基础测试',1);
INSERT INTO db.t1 values('2023-12-2 12:00:12.000+00:00',null, '基础测试',1);
EXPORT INTO CSV "nodelocal://1/test_sql" FROM TABLE db.t1;
IMPORT INTO db.t1 SQL DATA ("nodelocal://1/test_sql");
DROP TABLE t1;
IMPORT TABLE CREATE USING "nodelocal://1/test_sql/meta.sql" SQL DATA ("nodelocal://1/test_sql");
IMPORT TABLE CREATE USING "nodelocal://1/test_sql/meta.sql";
USE defaultdb;
DROP DATABASE db;