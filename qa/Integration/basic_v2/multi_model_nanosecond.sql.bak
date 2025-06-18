-- ZDP-44676 bug
drop database if EXISTS tsdb cascade;
drop database if EXISTS rdb cascade;

create ts database tsdb;
create database rdb;

CREATE TABLE rdb.r (id INT4 NOT NULL, rname VARCHAR NOT NULL, create_time TIMESTAMP, CONSTRAINT "primary" PRIMARY KEY (id ASC), FAMILY "primary" (id, rname));
CREATE TABLE tsdb.ts (ts TIMESTAMPTZ NOT NULL, e1 int) TAGS (tag1 VARCHAR not null, tag2 VARCHAR) PRIMARY TAGS (tag1);

insert into tsdb.ts values('2020-01-01 00:00:00', 1, 'a', 'b');
insert into rdb.r values(1, 'b', '2021-01-01 00:00:00.123456');

set enable_multimodel=true;

SELECT r.create_time FROM rdb.r r, tsdb.ts ts WHERE ts.tag2 = r.rname;

set enable_multimodel=false;

drop database tsdb cascade;
drop database rdb cascade;