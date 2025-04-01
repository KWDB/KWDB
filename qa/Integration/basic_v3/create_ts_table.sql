create ts database test;
create table test.sjcx01(k_timestamp timestamp not null,A INT4 not null,B INT4 not null,C VARCHAR(10) not null,D VARCHAR(10) not null)attributes (t1_attribute varchar not null) primary tags(t1_attribute);
insert into test.sjcx01 values (1681111110000,1,1,'A','DFFDARG', 'qqq');
select * from test.sjcx01 order by k_timestamp;
select k_timestamp, A, B from test.sjcx01;
drop database test cascade;