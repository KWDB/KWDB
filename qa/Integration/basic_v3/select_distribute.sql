-- create
CREATE TS DATABASE tsdb;
CREATE TABLE tsdb.tab
(
    ts   TIMESTAMP NOT NULL,
    col1 INT,
    col2 CHAR(3)
) TAGS (tag1 INT8 NOT NULL, tag2 FLOAT4, tag3 CHAR(3)) PRIMARY TAGS(tag1);
SELECT pg_sleep(1);
SELECT count(*)
FROM kwdb_internal.ranges
WHERE table_name = 'tab';
-- insert
INSERT INTO tsdb.tab
VALUES ('2024-01-05 01:00:00', 1, NULL, 222, 1.11, 'one'),
       ('2024-01-05 02:00:00', 2, NULL, 211, 1.311, 'one'),
       ('2024-01-05 03:00:00', 3, NULL, 122, 12.11, 'one');
-- select
SELECT pg_sleep(1);
SELECT *
FROM tsdb.tab
WHERE col1 = 1;
SELECT *
FROM tsdb.tab
WHERE col1 = 2;
SELECT *
FROM tsdb.tab
WHERE col1 = 3;
-- insert
INSERT INTO tsdb.tab
VALUES ('2024-01-05 04:00:00', 4, NULL, 222, 1.11, 'one'),
       ('2024-01-05 05:00:00', 5, NULL, 211, 1.311, 'one'),
       ('2024-01-05 06:00:00', 6, NULL, 122, 12.11, 'one');
-- select
SELECT *
FROM tsdb.tab
WHERE col1 = 4;
SELECT *
FROM tsdb.tab
WHERE col1 = 5;
SELECT *
FROM tsdb.tab
WHERE col1 = 6;

DROP DATABASE tsdb;
