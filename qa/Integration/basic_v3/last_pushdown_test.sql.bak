create ts database last_db;
use last_db;
/* normal time-series table */
create table t(
                  k_timestamp timestamp not null,
                  x float,
                  y int,
                  z char(10),
                  x2 float8,
                  y2 int2,
                  y3 int8,
                  z2 char(10),
                  z3 nchar(10),
                  z4 nvarchar(32),
                  z5 varbytes(10),
                  z6 varbytes(10),
                  z7 varchar(32)
) tags(a int not null) primary tags(a);
insert into t values('2023-10-10 09:10:00.688', 1.0,  1, 'a',1.1, 10, 100,'aa','last测试','表达式测试','aaa','aaaa','aaaaa', 1);
insert into t values('2023-10-10 09:20:00.688', 2.0,  2, 'b',2.1, 20, 200,'bb','last测试','表达式测试','bbb','bbbb','bbbbb', 1);
insert into t values('2023-10-10 09:30:00.688', 3.0,  1, 'c',3.1, 10, 100,'cc','last测试','表达式测试','ccc','cccc','ccccc', 1);
insert into t values('2023-10-10 09:40:00.688', 4.0,  2, 'd',4.1, 20, 200,'dd','last测试','表达式测试','ddd','dddd','ddddd', 1);
insert into t values('2023-10-10 09:50:00.688', 5.0,  5, 'e',5.1, 50, 500,'ee','last测试','表达式测试','eee','eeee','eeeee', 1);
insert into t values('2023-10-10 10:10:00.688', 6.0,  6, 'e',6.1, 60, 600,'ee','last测试','表达式测试','eee','eeee','eeeee', 1);
insert into t values('2023-10-10 10:20:00.688', 0.1,  2, 'f',0.2, 20, 200,'ff','last测试','表达式测试','fff','ffff','fffff', 1);
insert into t values('2023-10-10 10:30:00.688', 5.5,  5, 'e',5.6, 50, 500,'ee','last测试','表达式测试','eee','eeee','eeeee', 1);
insert into t values('2023-10-10 10:40:00.688', 7.0, -1, 'z',7.1,-10,-100,'zz','last测试','表达式测试','zzz','zzzz','zzzzz', 1);
insert into t values('2023-10-10 10:50:00.688', 8.0, -2, 'z',8.1,-20,-200,'zz','last测试','表达式测试','zzz','zzzz','zzzzz', 1);
insert into t values('2023-10-10 11:00:00.688', 8.0, -3, 'z',8.1,-30,-300,'zz','last测试','表达式测试','zzz','zzzz','zzzzz', 1);
select last(k_timestamp),last(x),last(y),last(z),last_row(k_timestamp),last_row(x),last_row(y),last_row(z) from t;
select lastts(k_timestamp),lastts(x),lastts(y),lastts(z) from t;

/* test last expressions */
/* + - * / % = < <= > >= != << >>*/
/* ****************last last_row with constant**************** */
select last(y),last(y)+1,1+last(y)+last(y)+1,last_row(y),last_row(y)+2,2+last_row(y)+last_row(y)+2 from t;
select last(y),last(y)-1,1+last(y)+last(y)-1,last_row(y),last_row(y)-2,2+last_row(y)+last_row(y)-2 from t;
select last(y),last(y)*1,1+last(y)+last(y)*1,last_row(y),last_row(y)*2,2+last_row(y)+last_row(y)*2 from t;
select last(y),last(y)/1,1+last(y)+last(y)/1,last_row(y),last_row(y)/2,2+last_row(y)+last_row(y)/2 from t;
select last(y),last(y)%1,1+last(y)+last(y)%1,last_row(y),last_row(y)%2,2+last_row(y)+last_row(y)%2 from t;
select last(y),last(y)=1,1+last(y)+last(y)=1,last_row(y),last_row(y)=2,2+last_row(y)+last_row(y)=2 from t;
select last(y),last(y)<1,1+last(y)+last(y)<1,last_row(y),last_row(y)<2,2+last_row(y)+last_row(y)<2 from t;
select last(y),last(y)<=1,1+last(y)+last(y)<=1,last_row(y),last_row(y)<=2,2+last_row(y)+last_row(y)<=2 from t;
select last(y),last(y)>1,1+last(y)+last(y)>1,last_row(y),last_row(y)>2,2+last_row(y)+last_row(y)>2 from t;
select last(y),last(y)>=1,1+last(y)+last(y)>=1,last_row(y),last_row(y)>=2,2+last_row(y)+last_row(y)>=2 from t;
select last(y),last(y)!=1,1+last(y)+last(y)!=1,last_row(y),last_row(y)!=2,2+last_row(y)+last_row(y)!=2 from t;
select last(x),last(x)+1,1+last(x)+last(x)+1,last_row(x),last_row(x)+2,2+last_row(x)+last_row(x)+2 from t;
select last(x),last(x)-1,1+last(x)+last(x)-1,last_row(x),last_row(x)-2,2+last_row(x)+last_row(x)-2 from t;
select last(x),last(x)*1,1+last(x)+last(x)*1,last_row(x),last_row(x)*2,2+last_row(x)+last_row(x)*2 from t;
select last(x),last(x)/1,1+last(x)+last(x)/1,last_row(x),last_row(x)/2,2+last_row(x)+last_row(x)/2 from t;
select last(x),last(x)%1,1+last(x)+last(x)%1,last_row(x),last_row(x)%2,2+last_row(x)+last_row(x)%2 from t;
select last(x),last(x)=1,1+last(x)+last(x)=1,last_row(x),last_row(x)=2,2+last_row(x)+last_row(x)=2 from t;
select last(x),last(x)<1,1+last(x)+last(x)<1,last_row(x),last_row(x)<2,2+last_row(x)+last_row(x)<2 from t;
select last(x),last(x)<=1,1+last(x)+last(x)<=1,last_row(x),last_row(x)<=2,2+last_row(x)+last_row(x)<=2 from t;
select last(x),last(x)>1,1+last(x)+last(x)>1,last_row(x),last_row(x)>2,2+last_row(x)+last_row(x)>2 from t;
select last(x),last(x)>=1,1+last(x)+last(x)>=1,last_row(x),last_row(x)>=2,2+last_row(x)+last_row(x)>=2 from t;
select last(x),last(x)!=1,1+last(x)+last(x)!=1,last_row(x),last_row(x)!=2,2+last_row(x)+last_row(x)!=2 from t;
select last(x),last(y),last(y)<<cast(1.0 as int),1+last(y)+last(y)<<cast(1.0 as int),last_row(y),last_row(y)<<1,1+last_row(y)+last_row(y)<<1 from t;
select last(x),last(y),last(y)>>cast(1.0 as int),1+last(y)+last(y)>>cast(2.0 as int),last_row(y),last_row(y)>>2,1+last_row(y)+last_row(y)>>2 from t;
/* with where and having */
select last(y),last(y)+1,1+last(y)+last(y)+1,last_row(y),last_row(y)+2,2+last_row(y)+last_row(y)+2 from t where y > 1 group by y having last(y)+last_row(y)>4 order by last(y);
select last(y),last(y)-1,1+last(y)+last(y)-1,last_row(y),last_row(y)-2,2+last_row(y)+last_row(y)-2 from t where y+y>1 group by y having last(y)-last_row(y)=0 order by last(y);
select last(y),last(y)*1,1+last(y)+last(y)*1,last_row(y),last_row(y)*2,2+last_row(y)+last_row(y)*2 from t where y < 9 group by y having last(y)*last_row(y)>4 order by last(y);
select last(y),last(y)/1,1+last(y)+last(y)/1,last_row(y),last_row(y)/2,2+last_row(y)+last_row(y)/2 from t group by y having last(y)/y>=1 order by last(y);
select last(y),last(y),last(y)%1,1+last(y)+last(y)%1,last_row(y),last_row(y)%2,2+last_row(y)+last_row(y)%2 from t group by y having last(y)%2=0 order by last(y);
select last(y),last(y),last(y)%1,1+last(y)+last(y)%1,last_row(y),last_row(y)%2,2+last_row(y)+last_row(y)%2 from t group by y having last(y)+last_row(y)>=2.1 order by last(y);
select last(y),last(y),last(y)%1,1+last(y)+last(y)%1,last_row(y),last_row(y)%2,2+last_row(y)+last_row(y)%2 from t group by y having last(y)+last_row(y)>2.1 order by last(y);
select last(y),last(y),last(y)%1,1+last(y)+last(y)%1,last_row(y),last_row(y)%2,2+last_row(y)+last_row(y)%2 from t group by y having last(y)+last_row(y)<10.5 order by last(y);
select last(y),last(y),last(y)%1,1+last(y)+last(y)%1,last_row(y),last_row(y)%2,2+last_row(y)+last_row(y)%2 from t group by y having last(y)+last_row(y)<=10.5 order by last(y);
select last(y),last(y),last(y)%1,1+last(y)+last(y)%1,last_row(y),last_row(y)%2,2+last_row(y)+last_row(y)%2 from t group by y having last(y)+last_row(y)>=10.5 order by last(y);
select last(y),last(y),last(y)%1,1+last(y)+last(y)%1,last_row(y),last_row(y)%2,2+last_row(y)+last_row(y)%2 from t group by y having last(x)!=last(y) order by last(y);
select last(x),last(x)+1,1+last(x)+last(x)+1,last_row(x),last_row(x)+2,2+last_row(x)+last_row(x)+2 from t where x > 1 group by x having last(x)+last_row(x)>4 order by last(x);
select last(x),last(x)-1,1+last(x)+last(x)-1,last_row(x),last_row(x)-2,2+last_row(x)+last_row(x)-2 from t where x+y>1 group by x having last(x)-last_row(x)>2 order by last(x);
select last(x),last(x)*1,1+last(x)+last(x)*1,last_row(x),last_row(x)*2,2+last_row(x)+last_row(x)*2 from t where x < 9 group by x having last(x)*last_row(x)>4 order by last(x);
select last(x),last(x)/1,1+last(x)+last(x)/1,last_row(x),last_row(x)/2,2+last_row(x)+last_row(x)/2 from t group by x having last(x)/x>=1 order by last(x);
select last(x),last(y),last(x)%1,1+last(x)+last(x)%1,last_row(x),last_row(x)%2,2+last_row(x)+last_row(x)%2 from t group by y having last(y)%2=0 order by last(x);
select last(x),last(y),last(x)%1,1+last(x)+last(x)%1,last_row(x),last_row(x)%2,2+last_row(x)+last_row(x)%2 from t group by y having last(x)+last_row(y)=2.1 order by last(x), y;
select last(x),last(y),last(x)%1,1+last(x)+last(x)%1,last_row(x),last_row(x)%2,2+last_row(x)+last_row(x)%2 from t group by y having last(x)+last_row(y)>2.1 order by last(x), y;
select last(x),last(y),last(x)%1,1+last(x)+last(x)%1,last_row(x),last_row(x)%2,2+last_row(x)+last_row(x)%2 from t group by y having last(x)+last_row(y)<10.5 order by last(x), y;
select last(x),last(y),last(x)%1,1+last(x)+last(x)%1,last_row(x),last_row(x)%2,2+last_row(x)+last_row(x)%2 from t group by y having last(x)+last_row(y)<=10.5 order by last(x), y;
select last(x),last(y),last(x)%1,1+last(x)+last(x)%1,last_row(x),last_row(x)%2,2+last_row(x)+last_row(x)%2 from t group by y having last(x)+last_row(y)>=10.5 order by last(x), y;
select last(x),last(y),last(x)%1,1+last(x)+last(x)%1,last_row(x),last_row(x)%2,2+last_row(x)+last_row(x)%2 from t group by y having last(y)!=last(x) order by last(x), y;
select last(x),last(y),last(y)<<cast(1.0 as int),1+last(y)+last(y)<<cast(1.0 as int),last_row(y),last_row(y)<<1,1+last_row(y)+last_row(y)<<1 from t group by y having last(y)<<1<5 order by last(x), y;
select last(x),last(y),last(y)<<cast(1.0 as int),1+last(y)+last(y)<<cast(1.0 as int),last_row(y),last_row(y)<<1,1+last_row(y)+last_row(y)<<1 from t where x+y = 12 group by y having last(y)<<1>5 order by last(x);


/* ****************last last_row with column**************** */
select last(x),last(y),last(x)+y from t group by y order by y;
select last(y)+y,last_row(y)+y,last(y)+y+1 from t group by y order by y;
select last(y)-y,last_row(y)-y,last(y)-y-1 from t group by y order by y;
select last(y)*y,last_row(y)*y,last(y)*y*1 from t group by y order by y;
select last(y)/y,last_row(y)/y,last(y)/y/1 from t group by y order by y;
select last(x)+y,last_row(x)+y,last(x)+y+1 from t group by y order by y;
select last(x)-y,last_row(x)-y,last(x)-y-1 from t group by y order by y;
select last(x)*y,last_row(x)*y,last(x)*y*1 from t group by y order by y;
select last(x)/y,last_row(x)/y,last(x)/y/1 from t group by y order by y;
select last(y)%y,last_row(y)%y,last(y)%y%1 from t group by y order by y;
select last(x)=y,last_row(x)=y,last(x)=y+1 from t group by y order by y;
select last(x)<y,last_row(x)<y,last(x)<y+1 from t group by y order by y;
select last(x)<=y,last_row(x)<=y,last(x)<=y+1 from t group by y order by y;
select last(x)>y,last_row(x)>y,last(x)>y+1 from t group by y order by y;
select last(x)>=y,last_row(x)>=y,last(x)>=y+1 from t group by y order by y;
select last(x)!=y,last_row(x)!=y,last(x)!=y+1 from t group by y order by y;
select last(y)<<y,last_row(y)<<y,last(y)<<y+1 from t group by y order by y;
select last(y)>>y,last_row(y)>>y,last(y)>>y+1 from t group by y order by y;


/* with where and having */
select y,last(x),last(y),last(x)+y from t where y>1 group by y having last(x)+y>2.1 order by y;
select y,last(x)+y,last_row(x)+y,last(x)+y+1 from t where x+y =2 group by y having last(x)+y-2=1 order by y;
select y,last(x)+y,last_row(x)+y,last(x)+y+1 from t where x+y =2 group by y having last(x)+y-2=0 order by y;
select y,last(x)+y,last_row(x)+y,last(x)+y+1 from t where x+y =2 group by y having last(x)-y=0 order by y;
select y,last(x)+y,last_row(x)+y,last(x)+y+1 from t where x+y =2 group by y having last(x)*y=1 order by y;
select y,last(x)+y,last_row(x)+y,last(x)+y+1 from t where x+y =2 group by y having last(x)/y=1 order by y;
select y,last(x)+y,last_row(x)+y,last(x)+y+1,last(y) from t where x+y =2 group by y having last(y)%y=0 order by y;
select y,last(x)+y,last_row(x)+y,last(x)+y+1,last(y) from t where x+y =2 group by y having last(y)+last(y)=2 order by y;
select y,last(x)+y,last_row(x)+y,last(x)+y+1,last(y) from t where x+y =2 group by y having last(y)+last(y)<2 order by y;
select y,last(x)+y,last_row(x)+y,last(x)+y+1,last(y) from t where x+y =2 group by y having last(y)+last(y)<=2 order by y;
select y,last(x)+y,last_row(x)+y,last(x)+y+1,last(y) from t where x+y =2 group by y having last(y)+last(y)>2 order by y;
select y,last(x)+y,last_row(x)+y,last(x)+y+1,last(y) from t where x+y =2 group by y having last(y)+last(y)>=2 order by y;
select y,last(x)+y,last_row(x)+y,last(x)+y+1,last(y) from t where x+y =2 group by y having last(y)+last(y)!=2 order by y;
select y,last(x)+y,last_row(x)+y,last(x)+y+1,last(y) from t where x+y =2 group by y having last(y)+last(y)!=2 order by y;
select y,last(x)+y,last_row(x)+y,last(x)+y+1,last(y) from t where x+y =2 group by y having last(y)+last(y)!=2 order by y;
select y,last(x)+y,last_row(x)+y,last(x)+y+1,last(y) from t where x+y =2 group by y having last(y)<<y=2 order by y;
select y,last(x)+y,last_row(x)+y,last(x)+y+1,last(y) from t where x+y =2 group by y having last(y)>>y=2 order by y;

-- /* with different data types */
-- /* +-*/ 
select last(y),x,last(y)+x,last(y)-x,last(y)*x,last(y)/x from t group by x order by x;
select last(y),x2,last(y)+x2,last(y)-x2,last(y)*x2,last(y)/x2 from t group by x2 order by x2;
select last(x),y2,last(x)+y2,last(x)-y2,last(x)*y2,last(x)/y2 from t group by y2 order by y2;
select last(x),y3,last(x)+y3,last(x)-y3,last(x)*y3,last(x)/y3 from t group by y3 order by y3;
select last(y),y,last(y)+y,last(y)-y,last(y)*y,last(y)/y from t group by y order by y;
select last(y),y2,last(y)+y2,last(y)-y2,last(y)*y2,last(y)/y2 from t group by y2 order by y2;
select last(y),y,last(y)+y,last(y)-y,last(y)*y,last(y)/y from t group by y order by y;
select last(y),y2,last(y)+y2,last(y)-y2,last(y)*y2,last(y)/y2 from t group by y2 order by y2;
select last(y),y3,last(y)+y3,last(y)-y3,last(y)*y3,last(y)/y3 from t group by y3 order by y3;

/* % >> << */
select last(x),x2,last(x)%x2  from t group by x2 order by x2;

select last(y),y,last(y)<<y,last(y)>>y  from t group by y order by y;
select last(y),y,last(y)<<y,last(y)>>y  from t where y > 0 group by y order by y;
select last(y),y2,last(y)<<y2,last(y)>>y2  from t group by y2 order by y2;
select last(y),y2,last(y)<<y2,last(y)>>y2 from t where y2 > 0 group by y2 order by y2;
select last(y),y3,last(y)<<y3,last(y)>>y3  from t group by y3 order by y3;

select last(x),x,last(x)%x from t group by x order by x;
select last(x),x2,last(x)%x2  from t group by x2 order by x2;
select last(y),y,last(y)%y from t group by y order by y;
select last(y),y2,last(y)%y2 from t group by y2 order by y2;
select last(y),y3,last(y)%y3 from t group by y3 order by y3;

/* = != > >= < <= */
/* = != */
select last(x),last(x2),last(y),last(y2),last(y3),last(x)=last(x),last(x)=last(x2),last(x)=last(y),last(x)=last(y2),last(x)=last(y3) from t;
select k_timestamp,last_row(k_timestamp),last(k_timestamp),k_timestamp=last(k_timestamp),last(k_timestamp)=last_row(k_timestamp) from t group by k_timestamp order by k_timestamp;
select k_timestamp,last_row(k_timestamp),last(k_timestamp),k_timestamp=last(k_timestamp),'2023-10-10 09:10:00.688'=last_row(k_timestamp) from t group by k_timestamp order by k_timestamp;

select k_timestamp,last_row(k_timestamp),last(k_timestamp),lastts(k_timestamp),last_row(k_timestamp)=lastts(k_timestamp) from t group by k_timestamp order by k_timestamp;
select k_timestamp,last_row(k_timestamp),last(k_timestamp),lastts(k_timestamp),lastts(k_timestamp)=last(k_timestamp) from t group by k_timestamp order by k_timestamp;
select k_timestamp,last_row(k_timestamp),last(k_timestamp),lastts(k_timestamp),k_timestamp=lastts(x) from t group by k_timestamp order by k_timestamp;
select k_timestamp,last_row(k_timestamp),last(k_timestamp),lastts(k_timestamp),lastts(x)=k_timestamp from t group by k_timestamp order by k_timestamp;
-- k_timestamp='2023-10-10 09:10:00.688' Unknown function: =
select k_timestamp,last_row(k_timestamp),last(k_timestamp),lastts(k_timestamp),k_timestamp='2023-10-10 09:10:00.688' from t group by k_timestamp order by k_timestamp;

select last(z),last(z2),last(z3),last(z4),last(z5),last(z6),last(z7),last(z)=last_row(z2) from t;
select last_row(x),last_row(x2),last_row(y),last_row(y2),last_row(y3),last_row(y)!=last(x),last_row(y)!=last(x2),last_row(y)!=last(y),last_row(y)!=last(y2),last_row(y)!=last(y3) from t;
select k_timestamp,last_row(k_timestamp),last(k_timestamp),k_timestamp!=last(k_timestamp),last(k_timestamp)!=last_row(k_timestamp) from t group by k_timestamp order by k_timestamp;
select k_timestamp,last_row(k_timestamp),last(k_timestamp),k_timestamp!=last(k_timestamp),'2023-10-10 09:10:00.688'!=last_row(k_timestamp) from t group by k_timestamp  order by k_timestamp;

select k_timestamp,last_row(k_timestamp),last(k_timestamp),lastts(k_timestamp),last_row(k_timestamp)!=lastts(k_timestamp) from t group by k_timestamp  order by k_timestamp;
select k_timestamp,last_row(k_timestamp),last(k_timestamp),lastts(k_timestamp),lastts(k_timestamp)!=last(k_timestamp) from t group by k_timestamp order by k_timestamp;
select k_timestamp,last_row(k_timestamp),last(k_timestamp),lastts(k_timestamp),k_timestamp!=lastts(x) from t group by k_timestamp  order by k_timestamp;
-- k_timestamp='2023-10-10 09:10:00.688' Unknown function: =
select k_timestamp,last_row(k_timestamp),last(k_timestamp),lastts(k_timestamp),k_timestamp!='2023-10-10 09:10:00.688' from t group by k_timestamp  order by k_timestamp;

select last(z),last(z2),last(z3),last(z4),last(z5),last(z6),last(z7),last(z)!=last_row(z2) from t;

/* > >= < <= */
select last(x),last(x2),last(y),last(y2),last(y3),last(x)>last(x),last(x)>last(x2),last(x)>last(y),last(x)>last(y2),last(x)>last(y3) from t;
select last(x),last(x2),last(y),last(y2),last(y3),last(x)>=last(x),last(x)>=last(x2),last(x)>=last(y),last(x)>=last(y2),last(x)>=last(y3) from t;
select last(x),last(x2),last(y),last(y2),last(y3),last(x)<last(x),last(x)<last(x2),last(x)<last(y),last(x)<last(y2),last(x)<last(y3) from t;
select last(x),last(x2),last(y),last(y2),last(y3),last(x)<=last(x),last(x)<=last(x2),last(x)<=last(y),last(x)<=last(y2),last(x)<=last(y3) from t;
select z2,last(z),last(z2),last(z3),last(z4),last(z5),last(z6),last(z7),last(z)>z2 from t group by z2 order by z2;
select z2,last(z),last(z2),last(z3),last(z4),last(z5),last(z6),last(z7),last(z)>=z2 from t group by z2 order by z2;
select z2,last(z),last(z2),last(z3),last(z4),last(z5),last(z6),last(z7),last(z)<z2 from t group by z2 order by z2;
select z2,last(z),last(z2),last(z3),last(z4),last(z5),last(z6),last(z7),last(z)<=z2 from t group by z2 order by z2;


/* ****************last last_row with other agg functions***************** */
select last(x),last(x2),last(y),last(y2),last(y3) from t;
select avg(x),avg(x2),round(avg(y),2),round(avg(y2),2),round(avg(y3),2) from t;
select sum(x),sum(x2),sum(y),sum(y2),sum(y3) from t;
select count(k_timestamp),count(x),count(x2),count(y),count(y2),count(y3),count(z) from t;
select max(k_timestamp),max(x),max(x2),max(y),max(y2),max(y3),max(z),max(z7) from t;
select min(k_timestamp),min(x),min(x2),min(y),min(y2),min(y3),min(z),min(z7) from t;
/*!!!*/
select first(k_timestamp),first(x),first(x2),first(y),first(y2),first(y3),first(z),first(z7) from t;
select variance(x),variance(x2),variance(y),variance(y2),variance(y3) from t;
select stddev_samp(x),stddev_samp(x2),stddev_samp(y),stddev_samp(y2),stddev_samp(y3) from t;
select corr(x,x),corr(x,x2),corr(x,y),corr(x,y2),corr(x,y3) from t;

/* +-*/
select x,y,last(x),last_row(x),avg(y),last(x)+avg(y),last_row(x)-avg(y),last_row(x)*avg(x),last(x)/avg(x) from t group by x,y order by x,y;
-- float bug
select x,y,last(x),last_row(x),sum(y),last(x)+sum(y),last_row(x)-sum(y2),last_row(x)*sum(x2),last(x)/sum(x2) from t group by x,y order by x,y;
-- float bug
select x,y,last(x),last_row(x),count(y),last(x)+count(y),last_row(x)-count(x),last_row(x)*count(x2),last(x)/count(x2) from t group by x,y order by x,y;
select x,y,last(x),last_row(x),first(y),last(x)+first(y),last_row(x)-first(y),last_row(x)*first(x),last(x)/first(x2) from t group by x,y order by x,y;
select x,y,last(x),last_row(x),max(y),last(x)+max(y),last_row(x)-max(y),last_row(x)*max(x),last(x)/max(x2) from t group by x,y order by x,y;
select x,y,last(x),last_row(x),min(y),last(x)+min(y),last_row(x)-min(y),last_row(x)*min(x),last(x)/min(x2) from t group by x,y order by x,y;
-- float bug
select last(x),last_row(x),variance(y),last(x)+variance(y),variance(y)-last_row(x2),last_row(x)*variance(x),variance(x2)/last(y) from t;
select last(x),last_row(x),stddev_samp(y),last(x)+stddev_samp(y),last_row(x)-stddev_samp(y),last_row(x)*stddev_samp(x),last(x)/stddev_samp(x2) from t;
select last(x),last_row(x),corr(x,y),last(x)+corr(x,y),last_row(x)-corr(x,y3),last_row(x)*corr(x,x2),last(x)/corr(x,x2) from t;
-- float bug

/* % << >> */
select x,y,last(x),last_row(x),sum(y),last(x)%sum(x2),last_row(y)<<count(y),last(y)>>count(y) from t group by x,y order by x,y;
select x,y,last(y),last_row(y),sum(y),last(y)%sum(y),last_row(y)<<count(y),last(y)>>count(y) from t group by x,y order by x,y;
select x,y,last(y),last_row(y),avg(y),last(y)%avg(y) from t group by x,y order by x,y;
-- fix shift
-- select x,y,last(y),last_row(y),max(y),last(y)%max(y),last_row(y)<<max(y),last(y)>>max(y) from t group by x,y order by x,y;
select x,y,last(y),last_row(y),max(y),last(y)%max(y),last_row(y)<<max(y),last(y)>>max(y) from t group by x,y having max(y) > 0 order by x,y;
-- select x,y,last(y),last_row(y),min(y),last(y)%min(y),last_row(y)<<min(y),last(y)>>min(y) from t group by x,y order by x,y;
select x,y,last(y),last_row(y),min(y),last(y)%min(y),last_row(y)<<min(y),last(y)>>min(y) from t group by x,y having min(y) > 0 order by x,y;
-- fix shift
-- fix bo mod;eg:-3%8.45454545
select last(y),last_row(y),variance(y),last(y)%variance(y)from t;
--
select last(y),last_row(y),stddev_samp(y),last(y)%stddev_samp(y),last(y)<<stddev_samp(y) from t;
select last(x),last_row(x),corr(x,y),last(x)%corr(x,y) from t;

/* = != > >= < <= */
select last(x),last_row(x),last(z),lastts(k_timestamp),last(x)=avg(x),last_row(x)=sum(x),last(x)=count(x),last(x)=max(x),last(x)=min(x),last(x)=variance(x),last(x)=stddev_samp(x),last(x)=corr(x,x) from t;
select last(x),last_row(x),last(z),lastts(k_timestamp),last(x)!=avg(x),last_row(x)!=sum(x),last(x)!=count(x),last(x)!=max(x),last(x)!=min(x),last(x)!=variance(x),last(x)!=stddev_samp(x),last(x)!=corr(x,x) from t;
select lastts(k_timestamp),lastts(k_timestamp)=max(k_timestamp),lastts(k_timestamp)=min(k_timestamp) from t;
select lastts(k_timestamp),lastts(k_timestamp)!=max(k_timestamp),lastts(k_timestamp)!=min(k_timestamp) from t;
select last(x),last_row(x),last(z),lastts(k_timestamp),last(x)>avg(x),last_row(x)>sum(x),last(x)>count(x),last(x)>max(x),last(x)>min(x),last(x)>variance(x),last(x)>stddev_samp(x),last(x)>corr(x,x) from t;
select last(x),last_row(x),last(z),lastts(k_timestamp),last(x)>=avg(x),last_row(x)>=sum(x),last(x)>=count(x),last(x)>=max(x),last(x)>=min(x),last(x)>=variance(x),last(x)>=stddev_samp(x),last(x)>=corr(x,x) from t;
select lastts(k_timestamp),lastts(k_timestamp)>max(k_timestamp),lastts(k_timestamp)>min(k_timestamp) from t;
select lastts(k_timestamp),lastts(k_timestamp)>=max(k_timestamp),lastts(k_timestamp)>=min(k_timestamp) from t;
select last(x),last_row(x),last(z),lastts(k_timestamp),last(x)<avg(x),last_row(x)<sum(x),last(x)<count(x),last(x)<max(x),last(x)<min(x),last(x)<variance(x),last(x)<stddev_samp(x),last(x)<corr(x,x) from t;
select last(x),last_row(x),last(z),lastts(k_timestamp),last(x)<=avg(x),last_row(x)<=sum(x),last(x)<=count(x),last(x)<=max(x),last(x)<=min(x),last(x)<=variance(x),last(x)<=stddev_samp(x),last(x)<=corr(x,x) from t;
select lastts(k_timestamp),lastts(k_timestamp)<max(k_timestamp),lastts(k_timestamp)<min(k_timestamp) from t;
select lastts(k_timestamp),lastts(k_timestamp)<=max(k_timestamp),lastts(k_timestamp)<=min(k_timestamp) from t;
select last(z),last_row(z2),max(z),max(z2),min(z),min(z2),last(z)=max(z),last(z)>min(z),last(z)>=max(z),LAST_ROW(z2)<min(z2)from t;

/* with where and having */
select x,y,last(y),last_row(y),max(y),last(y)+max(y),last_row(y)+max(y) from t where x+y > 2 group by x,y having last(y)+max(y)>2 order by x,y;
select x,y,last(y),last_row(y),min(y),last(y)+min(y),last_row(y)+min(y) from t where x+y>4 group by x,y having last_row(y)-min(y)>=0 order by x,y;
select x,y,last(y),last_row(y),avg(y),last(y)+avg(y),last_row(y)+avg(y) from t where x+y > 2 group by x,y having last(y)-avg(y)=0 order by x,y;
select x,y,last(y),last_row(y),sum(y),last(y)+sum(y),last_row(y)+sum(y) from t where x+y > 2 group by x,y having last(y)*sum(y)=4 order by x,y;
select x,y,last(y),last_row(y),count(y),last(y)+count(y),last_row(y)+count(y) from t where x+y > 2 group by x,y having last(y)/count(y)=1 order by x,y;
select x,y,last(y),last_row(y),last(y)+last(y),last_row(y)+last(y) from t where x+y > 2 group by x,y having last(y)%last(y)=0 order by x,y;
-- float bug
select last(y),last_row(y),variance(y),last(y)+variance(y),last_row(y)+variance(y) from t where x+y > 2;
select last(y),last_row(y),stddev_samp(y),last(y)+stddev_samp(y),last_row(y)+stddev_samp(y) from t where x+y > 2;
-- float bug
select last(y),last_row(y),corr(x,y),last(y)+corr(x,y),last_row(y)+corr(x,y) from t where x+y > 2;
select x,y,last(y),last_row(y),sum(y),last(y)+sum(y),last_row(y)+sum(y) from t where x+y > 2 group by x,y having last(y)+sum(y)>2 order by x,y;
select x,y,last(y),last_row(y),count(y),last(y)+count(y),last_row(y)+count(y) from t where x+y > 2 group by x,y having last(y)+count(y)>=2 order by x,y;
select x,y,last(y),last_row(y),avg(y),last(y)+avg(y),last_row(y)+avg(y) from t where x+y > 2 group by x,y having last(y)+avg(y)<2 order by x,y;
select x,y,last(y),last_row(y),max(y),last(y)+max(y),last_row(y)+max(y) from t where x+y > 2 group by x,y having last(y)+max(y)<=2 order by x,y;
select x,y,last(y),last_row(y),min(y),last(y)+min(y),last_row(y)+min(y) from t where x+y > 2 group by x,y having last(y)+min(y)!=2 order by x,y;
--fix shift
select x,y,last(y),last_row(y),max(y),last(y)+max(y),last_row(y)+max(y) from t where x+y > 2 group by x,y having last(y)<<max(y)>8 order by x,y;
select x,y,last(y),last_row(y),min(y),last(y)+min(y),last_row(y)+min(y) from t where x+y > 2 group by x,y having last(y)<<min(y)<8 order by x,y;
--fix shift

/* with time_bucket */
select time_bucket(k_timestamp, '1200s') bucket, last_row(z),last(z2),last(x),count(z),avg(x),sum(x),max(y),min(y),lastts(z) from t group by bucket order by bucket;
select time_bucket(k_timestamp, '1200s') bucket, last_row(z),last(z2),last(x),count(z),avg(x),sum(x),max(y),min(y),lastts(z) from t where x+y>3 group by bucket order by bucket;
select time_bucket(k_timestamp, '1200s') bucket, last_row(z),last(z2),last(x),count(z),last_row(z)>last(z2),last(x)+count(z) from t where z6 in ('aaaa', 'bbbb', 'eeee') group by bucket order by bucket;
select time_bucket(k_timestamp, '1200s') bucket, last_row(z),last(z2),last(x),count(z),avg(x),sum(x),max(y),min(y),lastts(z) from t where k_timestamp between '2023-10-10 09:00:00' and '2023-10-10 10:20:00' group by bucket order by bucket;
select time_bucket(k_timestamp, '1200s') bucket, last_row(z),last(z2),last(x),count(z),avg(x),sum(x),max(y),min(y),lastts(z) from t where x+y>3 and k_timestamp >= '2023-10-10 10:00:00' and k_timestamp <= '2023-10-10 11:00:00' group by bucket order by bucket;
select time_bucket(k_timestamp, '1200s') bucket, last_row(z),last(z2),last(x),count(z),avg(x),sum(x),max(y),min(y),lastts(z) from t where x+y<3 and k_timestamp >= '2023-10-10 10:00:00' and k_timestamp <= '2023-10-10 11:00:00' group by bucket order by bucket;
select time_bucket(k_timestamp, '1200s') bucket, last_row(z),last(z2),last(x),count(z),avg(x),sum(x),max(y),min(y),lastts(z) from t where x+y<3 or k_timestamp >= '2023-10-10 10:00:00' and k_timestamp <= '2023-10-10 11:00:00' group by bucket order by bucket;
select time_bucket(k_timestamp, '1200s') bucket, last_row(z),last(z2),last(x),count(z),avg(x),sum(x),max(y),min(y),lastts(z) from t where k_timestamp between '2023-10-10 09:00:00' and '2023-10-10 10:20:00' group by bucket having avg(x)+sum(x)=7.5 order by bucket;
select time_bucket(k_timestamp, '1200s') bucket, last_row(z),last(z2),last(x),count(z),avg(x),sum(x),max(y),min(y),lastts(z) from t where k_timestamp between '2023-10-10 09:00:00' and '2023-10-10 10:20:00' group by bucket having last(x)=3 order by bucket;
select time_bucket(k_timestamp, '1200s') bucket, last_row(z),last(z2),last(x),count(z),avg(x),sum(x),max(y),min(y),lastts(z) from t where k_timestamp between '2023-10-10 09:00:00' and '2023-10-10 10:20:00' group by bucket having last(x)-count(x)>=min(y) order by bucket;
select time_bucket(k_timestamp, '1200s') bucket, last_row(z),last(z2),last(x),count(z),avg(x),sum(x),max(y),min(y),lastts(z) from t where k_timestamp between '2023-10-10 09:00:00' and '2023-10-10 10:20:00' group by bucket having last(x)-count(x)>=min(y) and max(y)+min(y)!=3 order by bucket;

/* with project sub-query */
select last(x),max(y),last(x)+1,(select last(x)+1 as a from t),(select last(x)+max(y) as b from t) from t;
select x,last(x),max(y),last(x)+1,(select last(x)+count(z) as a from t) from t group by x order by x;
select x,last(x),max(y),last(x)+1,(select last(x)+max(y) as a from t where x > 5) from t group by x order by x;
/* with from sub-query */
select a+b,a,b,c from (select last(x) as a,min(x) as b,last(x)+min(x) as c from t)tt order by a;
select a+b,a,b,c from (select last(x) as a,min(x) as b,last(x)+min(x) as c from t group by x)tt order by a;
select a+b,a,b,c from (select last(x) as a,min(x) as b,last(x)+min(x) as c from t where x > 2 group by x)tt order by a;

-- has bug TODO for AE where filter
-- select a+b,a,b,c from (select last(x) as a,min(x) as b,last(x)+min(x) as c from t where x > 2 and x/y>1  group by x)tt order by a;
-- select a+b,a,b,c from (select last(x) as a,min(x) as b,last(x)+min(x) as c from t where x > 2 and x/y>1  group by x having last(x)+min(x)<11)tt order by a;

/* with lastts compare */
select last(k_timestamp)=lastts(k_timestamp) from t;
select lastts(k_timestamp),max(k_timestamp),min(k_timestamp),lastts(k_timestamp)<max(k_timestamp),lastts(k_timestamp)<min(k_timestamp) from t;
select lastts(k_timestamp),max(k_timestamp),min(k_timestamp),lastts(k_timestamp)<max(k_timestamp),lastts(k_timestamp)>min(k_timestamp) from t;
select lastts(k_timestamp),max(k_timestamp),min(k_timestamp),lastts(k_timestamp)=max(k_timestamp),lastts(k_timestamp)>min(k_timestamp) from t;
select lastts(k_timestamp),lastts(k_timestamp)+'1d' from t;
explain select lastts(k_timestamp),lastts(k_timestamp)+'1d' from t;
select lastts(k_timestamp),lastts(k_timestamp)+'1s' from t;
explain select lastts(k_timestamp),lastts(k_timestamp)+'1s' from t;

-- float compare
insert into t values('2023-10-10 11:00:00.688', 0.1, -3, 'z',0.033333,-30,-300,'zz','last测试','表达式测试','zzz','zzzz','zzzzz', 1);
insert into t values('2023-10-10 11:00:00.688', 0.3, -3, 'z',0.0033333,-30,-300,'zz','last测试','表达式测试','zzz','zzzz','zzzzz', 1);
select last(x),last(x2),last(x)%last(x2) from t;
insert into t values('2023-10-10 11:00:00.688', 0.1, -3, 'z',0.100000000000001,-30,-300,'zz','last测试','表达式测试','zzz','zzzz','zzzzz', 1);
select last(x),last(x2) from t where x = 0.1;
-- select y<<y from t;
drop table t;

-- over flow check
create table test_int(k_timestamp timestamp not null,e1 smallint,e2 int,e3 bigint) attributes (att1 int2 not null) primary tags(att1);
insert into test_int values('2023-10-10 09:10:00.688',    10,     1000000,             3000000, 1);
insert into test_int values('2023-10-10 09:20:00.688',  32767, 2147483647, 9223372036854775807, 1);
insert into test_int values('2023-10-10 09:30:00.688', -32768,-2147483648,-9223372036854775808, 1);
-- + - * /
-- select e2+e3 from test_int;
select last(e2)+last(e3) from test_int;
-- select e3-(-1) from test_int;
-- select e3*e3 from test_int;
select first(e2)*first(e3) from test_int;
explain select e2/e3 from test_int;
drop table test_int;
use defaultdb;
drop database last_db cascade;


-- fix bug
create ts database test_query;
create table test_query.d1(k_timestamp timestamp not null,e1 timestamp,e2 smallint,e3 int,e4 bigint,e5 float,e6 float8,e7 bool) attributes (att1 int2 not null) primary tags(att1);
insert into test_query.d1 values ('2023-05-10 09:04:18.223',111111110000,1000,1000000,100000000000000000,100000000000000000.101,100000000000000000.10101010101,true, 1);
insert into test_query.d1 values ('2023-04-10 08:04:15.783',222222220000,2000,2000000,200000000000000000,200000000000000000.202,200000000000000000.20202020202,true, 1);
insert into test_query.d1 values ('2023-03-01 17:30:32',    333333330000,3000,2000000,300000000000000000,300000000000000000.303,300000000000000000.30303030303,true, 1);
insert into test_query.d1 values ('2023-12-21 17:30:25.167',444444440000,3000,3000000,400000000000000000,400000000000000000.404,400000000000000000.40404040404,false, 1);
insert into test_query.d1 values ('2023-05-01 08:00:00',    555555550000,5000,5000000,500000000000000000,500000000000000000.505,500000000000000000.50505050505,true, 1);
insert into test_query.d1 values ('2023-06-01 08:00:00',    666666660000,6000,6000000,600000000000000000,600000000000000000.606,600000000000000000.60606060606,false, 1);
insert into test_query.d1 values ('2020-11-06 17:10:23',    666666660000,7000,7000000,700000000000000000,700000000000000000.707,700000000000000000.10101010101,true, 1);
insert into test_query.d1 values (168111110011,                     null,null,null,null,null,null,null, 1);
insert into test_query.d1 values ('2021-04-01 15:00:00',111111110000,1000,3000000,400000000000000000,600000000000000000.606,400000000000000000.40404040404,false, 1);
insert into test_query.d1 values ('2022-05-01 17:00:00',666666660000,5000,5000000,600000000000000000,500000000000000000.505,500000000000000000.50505050505,false, 1);
-- ZDP-27256
select e3*e3 from test_query.d1 order by k_timestamp;
-- ZDP-27255
select max(e2) * max(e3), max(e4) - max(e3) from test_query.d1;
create table test_query.s1 (k_timestamp timestamp not null,e1 timestamp,e2 smallint,e3 int,e4 bigint,e5 float,e6 float8,e7 bool) attributes(name varchar(10) not null, t1_p1 varchar) primary tags(name);
insert into test_query.s1 values ('2023-05-10 09:04:18.223',111111110000,1000,1000000,100000000000000000,100000000000000000.101,100000000000000000.10101010101, true, 's1_1', 'd1');
insert into test_query.s1 values ('2023-04-10 08:04:15.783',222222220000,2000,2000000,200000000000000000,200000000000000000.202,200000000000000000.20202020202, true, 's1_1', 'd1');
insert into test_query.s1 values ('2023-03-01 17:30:32',    333333330000,3000,2000000,300000000000000000,300000000000000000.303,300000000000000000.30303030303, true, 's1_1', 'd1');
insert into test_query.s1 values ('2023-12-21 17:30:25.167',444444440000,3000,3000000,400000000000000000,400000000000000000.404,400000000000000000.40404040404,false, 's1_1', 'd1');
insert into test_query.s1 values ('2023-05-01 08:00:00',            null,null,   null,              null,                  null,                          null, null, 's1_1', 'd1');
insert into test_query.s1 values ('2023-06-01 08:00:00',    666666660000,6000,6000000,600000000000000000,600000000000000000.606,600000000000000000.60606060606,false, 's1_1', 'd1');
insert into test_query.s1 values ('2020-11-06 17:10:23',    666666660000,7000,7000000,700000000000000000,700000000000000000.707,700000000000000000.10101010101, true, 's1_1', 'd1');
insert into test_query.s1 values ('2022-08-16 10:23:05.123',888888880000,8000,8000000,800000000000000000,800000000000000000.808,800000000000000000.20202020202,false, 's1_1', 'd1');
insert into test_query.s1 values ('2021-04-01 15:00:00',    111111110000,1000,3000000,400000000000000000,600000000000000000.606,400000000000000000.40404040404,false, 's1_1', 'd1');
insert into test_query.s1 values ('2022-05-01 17:00:00',    666666660000,5000,5000000,600000000000000000,500000000000000000.505,500000000000000000.50505050505,false, 's1_1', 'd1');
-- ZDP-27256
select e3 * e3 from test_query.s1 order by k_timestamp;
select e3 * e3 from test_query.s1 where name = 's1_1' order by k_timestamp;
-- ZDP-27255
select max(e2) * max(e3), max(e4) - max(e3) from test_query.s1 where name = 's1_1';
drop database test_query CASCADE;

