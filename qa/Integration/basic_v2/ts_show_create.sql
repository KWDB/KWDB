--- relational db
CREATE DATABASE reldb1;
CREATE DATABASE reldb2 WITH ENCODING = 'UTF8';
SHOW CREATE DATABASE reldb1;
SHOW CREATE DATABASE reldb2;

CREATE TS DATABASE tsdb1;
CREATE TS DATABASE tsdb2 RETENTIONS 10d;
CREATE TS DATABASE tsdb3 RETENTIONS 11d;
CREATE TS DATABASE tsdb4 RETENTIONS 11600s;
CREATE TS DATABASE tsdb5 PARTITION INTERVAL 10d;
CREATE TS DATABASE tsdb6 RETENTIONS 9d PARTITION INTERVAL 9d;
SHOW CREATE DATABASE tsdb1;
SHOW CREATE DATABASE tsdb2;
SHOW CREATE DATABASE tsdb3;
SHOW CREATE DATABASE tsdb4;
SHOW CREATE DATABASE tsdb5;
SHOW CREATE DATABASE tsdb6;

create table tsdb1.t(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag);
create table tsdb2.t(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag);
create table tsdb3.t(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag);
create table tsdb4.t(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag);
create table tsdb5.t(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag);
create table tsdb6.t(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag);
SHOW CREATE TABLE tsdb1.t;
SHOW CREATE TABLE tsdb2.t;
SHOW CREATE TABLE tsdb3.t;
SHOW CREATE TABLE tsdb4.t;
SHOW CREATE TABLE tsdb5.t;
SHOW CREATE TABLE tsdb6.t;

DROP DATABASE reldb1 CASCADE;
DROP DATABASE reldb2 CASCADE;
DROP DATABASE tsdb1 CASCADE;
DROP DATABASE tsdb2 CASCADE;
DROP DATABASE tsdb3 CASCADE;
DROP DATABASE tsdb4 CASCADE;
DROP DATABASE tsdb5 CASCADE;
DROP DATABASE tsdb6 CASCADE;