create ts database tb2;
create table tb2.t1(k_timestamp timestamp not null, e1 float, e2 int, e3 int) tags (attr1 int not null) primary tags(attr1);    
INSERT INTO tb2.t1 values(1, NULL, 1,10, 1);    
INSERT INTO tb2.t1 values(2, NULL, 2,10, 1); 
INSERT INTO tb2.t1 values(3, NULL, 3,10, 1); 
INSERT INTO tb2.t1 values(4, NULL, 4,10, 1); 
INSERT INTO tb2.t1 values(5, NULL, 5,10, 1); 
INSERT INTO tb2.t1 values(6, NULL, 6,10, 1); 
INSERT INTO tb2.t1 values(7, NULL, 7,10, 1); 
INSERT INTO tb2.t1 values(8, NULL, 8,10, 1); 
INSERT INTO tb2.t1 values(9, NULL, 9,10, 1); 
INSERT INTO tb2.t1 values(10, NULL, 10,10, 1); 
INSERT INTO tb2.t1 values(11, NULL, 11,10, 1);    
INSERT INTO tb2.t1 values(12, NULL, 12,10, 1); 
INSERT INTO tb2.t1 values(13, NULL, 13,10, 1); 
INSERT INTO tb2.t1 values(14, NULL, 14,10, 1); 
INSERT INTO tb2.t1 values(15, NULL, 15,10, 1); 
INSERT INTO tb2.t1 values(16, NULL, 16,10, 1); 
INSERT INTO tb2.t1 values(17, NULL, 17,10, 1); 
INSERT INTO tb2.t1 values(18, NULL, 18,10, 1); 
INSERT INTO tb2.t1 values(19, NULL, 19,10, 1); 
INSERT INTO tb2.t1 values(20, NULL, 20,10, 1); 

     
INSERT INTO tb2.t1 values(21, 1.21, 21,10, 1);  
INSERT INTO tb2.t1 values(22, 1.22, 22,10, 1);   
INSERT INTO tb2.t1 values(23, 1.23, 23,10, 1);   
INSERT INTO tb2.t1 values(24, 1.24, 24,10, 1);   
INSERT INTO tb2.t1 values(25, 1.25, 25,10, 1);   
INSERT INTO tb2.t1 values(26, 1.26, 26,10, 1);   
INSERT INTO tb2.t1 values(27, 1.27, 27,10, 1);   
INSERT INTO tb2.t1 values(28, 1.28, 28,10, 1);   
INSERT INTO tb2.t1 values(29, 1.29, 29,10, 1);   
INSERT INTO tb2.t1 values(30, 1.30, 30,10, 1);   
INSERT INTO tb2.t1 values(31, 1.31, 31,10, 1);   
INSERT INTO tb2.t1 values(32, 1.32, 32,10, 1);   
INSERT INTO tb2.t1 values(33, 1.33, 33,10, 1);   
INSERT INTO tb2.t1 values(34, 1.34, 34,10, 1);   
INSERT INTO tb2.t1 values(35, 1.35, 35,10, 1);   
INSERT INTO tb2.t1 values(36, 1.36, 36,10, 1);   
INSERT INTO tb2.t1 values(37, 1.37, 37,10, 1);   
INSERT INTO tb2.t1 values(38, 1.38, 38,10, 1);   
INSERT INTO tb2.t1 values(39, 1.39, 39,10, 1);     
INSERT INTO tb2.t1 values(40, 1.40, 40,10, 1); 

select * from tb2.t1;
select count(e1),count(e2) from tb2.t1;
select min(e1),min(e2) from tb2.t1;
select max(e1),max(e2) from tb2.t1;
select avg(e1),avg(e2) from tb2.t1;
select count(e1),count(e2) from tb2.t1;
select last(*) from tb2.t1;
drop table tb2.t1;
drop database tb2 cascade;