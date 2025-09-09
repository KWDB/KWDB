create ts database test_select3;
create table test_select3.t_int2(ts timestamptz not null, e1 int2 not null, e2 int4 not null, e3 int8 not null)  tags(
    tag1 int2 not null, tag2 int4 not null, tag3 int8 not null) primary tags(tag1);
insert into test_select3.t_int2 values
                (1643771471000,32767,2147483647,9223372036854775807,32767,2147483647,9223372036854775807),
                (1643771472000,-32768,-2147483648,-9223372036854775808,-32768,-32768,-9223372036854775808),
                (1643771473000,-444,-4444,-4444,-444,-4444,-44444),
                (1643771474000,-444,-4444,-4444,-444,-4444,-44444),
                (1643771475000,32767,9999,99999,32767,66666,66666);

PREPARE p as select * from test_select3.t_int2 where tag1=$1 order by ts;
execute p(-444);
execute p(999999999);
drop database test_select3 cascade;


create ts database test_select3;
create table test_select3.t_int2(ts timestamptz not null, e1 int2 not null, e2 int4 not null, e3 int8 not null)  tags(
    tag1 int2 not null, tag2 int4 not null, tag3 int8 not null) primary tags(tag2);
insert into test_select3.t_int2 values
                                    (1643771471000,32767,2147483647,9223372036854775807,32767,2147483647,9223372036854775807),
                                    (1643771472000,-32768,-2147483648,-9223372036854775808,-32768,-32768,-9223372036854775808),
                                    (1643771473000,-444,-4444,-4444,-444,-4444,-44444),
                                    (1643771474000,-444,-4444,-4444,-444,-4444,-44444),
                                    (1643771475000,32767,9999,99999,32767,66666,66666);
PREPARE p1 as select * from test_select3.t_int2 where tag2=$1 order by ts;
execute p1(9223372036854775806);
drop database test_select3 cascade;


create ts database test_select3;
create table test_select3.t_int2(ts timestamptz not null, e1 int2 not null, e2 int4 not null, e3 int8 not null)  tags(
    tag1 int2 not null, tag2 int4 not null, tag3 int8 not null) primary tags(tag3);
insert into test_select3.t_int2 values
                                    (1643771471000,32767,2147483647,9223372036854775807,32767,2147483647,9223372036854775807),
                                    (1643771472000,-32768,-2147483648,-9223372036854775808,-32768,-32768,-9223372036854775808),
                                    (1643771473000,-444,-4444,-4444,-444,-4444,-44444),
                                    (1643771474000,-444,-4444,-4444,-444,-4444,-44444),
                                    (1643771475000,32767,9999,99999,32767,66666,66666);
PREPARE p2 as select * from test_select3.t_int2 where tag3=$1 order by ts;
execute p2(9223372036854775808);
drop database test_select3 cascade;
