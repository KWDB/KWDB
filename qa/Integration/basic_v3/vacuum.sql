create ts database test partition interval 1d;
use test;
create table test.t1(ts timestamp not null, c1 int) tags(ptag int not null) primary tags(ptag);

insert into test.t1 values('2024-12-22 09:35:01', 1, 1);
insert into test.t1 values('2024-12-22 09:35:02', 2, 1);
insert into test.t1 values('2024-12-22 09:35:03', 3, 1);
insert into test.t1 values('2024-12-22 09:35:04', 4, 1);
insert into test.t1 values('2024-12-22 09:35:05', 5, 1);
insert into test.t1 values('2024-12-22 09:35:06', 6, 1);
insert into test.t1 values('2024-12-22 09:35:07', 7, 1);
insert into test.t1 values('2024-12-22 09:35:08', 8, 1);
insert into test.t1 values('2024-12-22 09:35:09', 9, 1);
insert into test.t1 values('2024-12-22 09:35:10', 10, 1);

insert into test.t1 values('2024-12-22 09:35:11', 11, 2);
insert into test.t1 values('2024-12-22 09:35:12', 12, 2);
insert into test.t1 values('2024-12-22 09:35:13', 13, 2);
insert into test.t1 values('2024-12-22 09:35:14', 14, 2);
insert into test.t1 values('2024-12-22 09:35:15', 15, 2);
insert into test.t1 values('2024-12-22 09:35:16', 16, 2);
insert into test.t1 values('2024-12-22 09:35:17', 17, 2);
insert into test.t1 values('2024-12-22 09:35:18', 18, 2);
insert into test.t1 values('2024-12-22 09:35:19', 19, 2);
insert into test.t1 values('2024-12-22 09:35:20', 20, 2);

insert into test.t1 values('2024-12-22 09:35:21', 21, 3);
insert into test.t1 values('2024-12-22 09:35:22', 22, 3);
insert into test.t1 values('2024-12-22 09:35:23', 23, 3);
insert into test.t1 values('2024-12-22 09:35:24', 24, 3);
insert into test.t1 values('2024-12-22 09:35:25', 25, 3);
insert into test.t1 values('2024-12-22 09:35:26', 26, 3);
insert into test.t1 values('2024-12-22 09:35:27', 27, 3);
insert into test.t1 values('2024-12-22 09:35:28', 28, 3);
insert into test.t1 values('2024-12-22 09:35:29', 29, 3);
insert into test.t1 values('2024-12-22 09:35:30', 30, 3);

select count(c1) from t1;
vacuum ts databases;
select count(c1) from t1;
delete from t1 where ts='2024-12-22 09:35:01';
select count(c1) from t1;
vacuum ts databases;
select count(c1) from t1;
