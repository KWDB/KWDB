set cluster setting sql.stats.ts_automatic_collection.enabled = false;
set cluster setting sql.stats.tag_automatic_collection.enabled = false;
set cluster setting ts.ordered_table.enabled = true;
SET vectorize_row_count_threshold = 100000000;

create ts database test_ts;
use test_ts;

create table order_table
(time timestamp not null, e1 smallint, e2 int, e3 bigint, e4 float, e5 bool, e6 varchar)
    attributes (attr1 smallint not null, attr2 int, attr3 bigint, attr4 float, attr5 bool, attr6 varchar)
primary attributes (attr1);
ALTER TABLE order_table INJECT STATISTICS '[
  {
    "columns": ["attr1"],
    "created_at": "2024-09-06",
    "row_count": 100000,
    "distinct_count": 100000,
    "null_count": 0,
    "sort_histogram_buckets": [
      {"row_count": 100,"unordered_row_count": 0,"ordered_entities": 100,"unordered_entities": 0,"upper_bound": "2023-12-31 17:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 0,"ordered_entities": 10000,"unordered_entities": 0,"upper_bound": "2023-12-31 18:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 0,"ordered_entities": 10000,"unordered_entities": 0,"upper_bound": "2023-12-31 19:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 0,"ordered_entities": 10000,"unordered_entities": 0,"upper_bound": "2023-12-31 20:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 0,"ordered_entities": 10000,"unordered_entities": 0,"upper_bound": "2023-12-31 21:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 0,"ordered_entities": 10000,"unordered_entities": 0,"upper_bound": "2023-12-31 22:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 0,"ordered_entities": 10000,"unordered_entities": 0,"upper_bound": "2023-12-31 23:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 0,"ordered_entities": 10000,"unordered_entities": 0,"upper_bound": "2024-01-01 00:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 0,"ordered_entities": 10000,"unordered_entities": 0,"upper_bound": "2024-01-01 01:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 0,"ordered_entities": 10000,"unordered_entities": 0,"upper_bound": "2024-01-01 02:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 0,"ordered_entities": 10000,"unordered_entities": 0,"upper_bound": "2024-01-01 03:00:00+00:00"}
    ],
    "histo_col_type": "TIMESTAMPTZ"
  }]';
show sort_histogram for table order_table;

explain select * from order_table;
explain select * from order_table where attr1 = 1;
explain (opt,verbose) select * from order_table where attr1 = 1;
explain select * from order_table where attr1 in (1,2,3,4,5,6,7,8,9,10);
explain (opt,verbose) select * from order_table where attr1 in (1,2,3,4,5,6,7,8,9,10);
explain select * from order_table where attr1 > 500;
explain select * from order_table where attr1 > 500 and attr1 < 1000;
explain select attr2 from test_ts.order_table where attr1 < 2000 group by attr2 having attr2 < 3000000;
explain select e1 from test_ts.order_table order by e1;
explain select e1 from test_ts.order_table order by time;
explain select * from test_ts.order_table where attr1 in (1,2,3,4,5,6,7,8,9,10) and time >= '2023-12-31 18:00:00+00:00' and time <= '2024-01-01 03:00:00+00:00';
explain (opt,verbose) select * from test_ts.order_table where attr1 in (1,2,3,4,5,6,7,8,9,10) and time >= '2023-12-31 18:00:00+00:00' and time <= '2024-01-01 03:00:00+00:00';
explain select * from test_ts.order_table where attr1 in (1,2,3,4,5,6,7,8,9,10) and time >= '2023-12-31 20:30:00+00:00' and time <= '2024-01-01 00:30:00+00:00';
explain (opt,verbose) select * from test_ts.order_table where attr1 in (1,2,3,4,5,6,7,8,9,10) and time >= '2023-12-31 20:30:00+00:00' and time <= '2024-01-01 00:30:00+00:00';
explain select * from test_ts.order_table where attr1 in (1,2,3,4,5,6,7,8,9,10) and time >= '2025-01-01 00:30:00+00:00' and time <= '2025-12-31 20:30:00+00:00';
explain (opt,verbose) select * from test_ts.order_table where attr1 in (1,2,3,4,5,6,7,8,9,10) and time >= '2025-01-01 00:30:00+00:00' and time <= '2025-12-31 20:30:00+00:00';

create table unordered_table
(time timestamp not null, e1 smallint, e2 int, e3 bigint, e4 float, e5 bool, e6 varchar)
    attributes (attr1 smallint not null, attr2 int, attr3 bigint, attr4 float, attr5 bool, attr6 varchar)
primary attributes (attr1);
ALTER TABLE unordered_table INJECT STATISTICS '[
  {
    "columns": ["attr1"],
    "created_at": "2024-09-06",
    "row_count": 100000,
    "distinct_count": 100000,
    "null_count": 0,
    "sort_histogram_buckets": [
      {"row_count": 100,"unordered_row_count": 0,"ordered_entities": 100,"unordered_entities": 0,"upper_bound": "2023-12-31 17:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 10000000,"ordered_entities": 0,"unordered_entities": 10000,"upper_bound": "2023-12-31 18:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 10000000,"ordered_entities": 0,"unordered_entities": 10000,"upper_bound": "2023-12-31 19:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 10000000,"ordered_entities": 0,"unordered_entities": 10000,"upper_bound": "2023-12-31 20:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 10000000,"ordered_entities": 0,"unordered_entities": 10000,"upper_bound": "2023-12-31 21:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 10000000,"ordered_entities": 0,"unordered_entities": 10000,"upper_bound": "2023-12-31 22:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 10000000,"ordered_entities": 0,"unordered_entities": 10000,"upper_bound": "2023-12-31 23:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 10000000,"ordered_entities": 0,"unordered_entities": 10000,"upper_bound": "2024-01-01 00:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 10000000,"ordered_entities": 0,"unordered_entities": 10000,"upper_bound": "2024-01-01 01:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 10000000,"ordered_entities": 0,"unordered_entities": 10000,"upper_bound": "2024-01-01 02:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 10000000,"ordered_entities": 0,"unordered_entities": 10000,"upper_bound": "2024-01-01 03:00:00+00:00"}
    ],
    "histo_col_type": "TIMESTAMPTZ"
  }]';
show sort_histogram for table unordered_table;

explain select * from unordered_table;
explain select * from unordered_table where attr1 = 1;
explain (opt,verbose) select * from unordered_table where attr1 = 1;
explain select * from unordered_table where attr1 in (1,2,3,4,5,6,7,8,9,10);
explain (opt,verbose) select * from unordered_table where attr1 in (1,2,3,4,5,6,7,8,9,10);
explain select * from unordered_table where attr1 > 500;
explain select * from unordered_table where attr1 > 500 and attr1 < 1000;
explain select attr2 from test_ts.order_table where attr1 < 2000 group by attr2 having attr2 < 3000000;
explain select e1 from test_ts.unordered_table order by e1;
explain select e1 from test_ts.unordered_table order by time;
explain select * from test_ts.unordered_table where attr1 in (1,2,3,4,5,6,7,8,9,10) and time >= '2023-12-31 18:00:00+00:00' and time <= '2024-01-01 03:00:00+00:00';
explain (opt,verbose) select * from test_ts.unordered_table where attr1 in (1,2,3,4,5,6,7,8,9,10) and time >= '2023-12-31 18:00:00+00:00' and time <= '2024-01-01 03:00:00+00:00';
explain select * from test_ts.unordered_table where attr1 in (1,2,3,4,5,6,7,8,9,10) and time >= '2023-12-31 20:30:00+00:00' and time <= '2024-01-01 00:30:00+00:00';
explain (opt,verbose) select * from test_ts.unordered_table where attr1 in (1,2,3,4,5,6,7,8,9,10) and time >= '2023-12-31 20:30:00+00:00' and time <= '2024-01-01 00:30:00+00:00';
explain select * from test_ts.unordered_table where attr1 in (1,2,3,4,5,6,7,8,9,10) and time >= '2025-01-01 00:30:00+00:00' and time <= '2025-12-31 20:30:00+00:00';
explain (opt,verbose) select * from test_ts.unordered_table where attr1 in (1,2,3,4,5,6,7,8,9,10) and time >= '2025-01-01 00:30:00+00:00' and time <= '2025-12-31 20:30:00+00:00';

create table unordered_table2
(time timestamp not null, e1 smallint, e2 int, e3 bigint, e4 float, e5 bool, e6 varchar)
    attributes (attr1 smallint not null, attr2 int not null, attr3 bigint not null, attr4 float, attr5 bool, attr6 varchar)
primary attributes (attr1,attr2,attr3);
ALTER TABLE unordered_table2 INJECT STATISTICS '[
  {
    "columns": ["attr1","attr2","attr3"],
    "created_at": "2024-09-06",
    "row_count": 100000,
    "distinct_count": 100000,
    "null_count": 0,
    "sort_histogram_buckets": [
      {"row_count": 100,"unordered_row_count": 0,"ordered_entities": 100,"unordered_entities": 0,"upper_bound": "2023-12-31 17:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 5000000,"ordered_entities": 5000,"unordered_entities": 5000,"upper_bound": "2023-12-31 18:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 5000000,"ordered_entities": 1000,"unordered_entities": 9000,"upper_bound": "2023-12-31 19:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 2000000,"ordered_entities": 2000,"unordered_entities": 8000,"upper_bound": "2023-12-31 20:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 0,"ordered_entities": 0,"unordered_entities": 10000,"upper_bound": "2023-12-31 21:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 3000000,"ordered_entities": 5000,"unordered_entities": 5000,"upper_bound": "2023-12-31 22:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 6000000,"ordered_entities": 6000,"unordered_entities": 4000,"upper_bound": "2023-12-31 23:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 7000000,"ordered_entities": 1000,"unordered_entities": 9000,"upper_bound": "2024-01-01 00:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 8000000,"ordered_entities": 1,"unordered_entities": 1,"upper_bound": "2024-01-01 01:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 9000000,"ordered_entities": 1,"unordered_entities": 1,"upper_bound": "2024-01-01 02:00:00+00:00"},
      {"row_count": 10000000,"unordered_row_count": 10000000,"ordered_entities": 0,"unordered_entities": 1,"upper_bound": "2024-01-01 03:00:00+00:00"}
    ],
    "histo_col_type": "TIMESTAMPTZ"
  }]';
show sort_histogram for table unordered_table2;


explain select * from unordered_table2 where attr1 = 1 and attr2 = 2 and attr3 = 3;
explain (opt,verbose) select * from unordered_table2 where attr1 = 1 and attr2 = 2 and attr3 = 3;

drop database test_ts cascade;
reset cluster setting sql.stats.ts_automatic_collection.enabled;
reset cluster setting sql.stats.tag_automatic_collection.enabled;
reset cluster setting ts.ordered_table.enabled;
SET vectorize_row_count_threshold = 1000;