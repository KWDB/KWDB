-- ZDP-25983
DROP DATABASE IF EXISTS default_expr_db;
CREATE DATABASE default_expr_db;
DROP TABLE IF EXISTS default_expr_tb;
CREATE TABLE default_expr_tb1 (id int default unique_rowid(), name string);
CREATE TABLE default_expr_tb2 (id int8 default unique_rowid(), name string);
insert into default_expr_tb2(name) values ('aaa');

drop database default_expr_db;
