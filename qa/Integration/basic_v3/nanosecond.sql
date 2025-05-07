set cluster setting sql.query_cache.enabled=false;

-- 1. calculate
--- 1.1 +
select '2024-01-01 00:00:00.123456789'::date, '2024-01-01 00:00:00.123456789'::time(6), '2024-01-01 00:00:00.123456789'::date + '2024-01-01 00:00:00.123456789'::time(6);

select '2024-01-01 00:00:00.123456789'::date, '2024-01-01 00:00:00.123456789'::time(6), '2024-01-01 00:00:00.123456789'::time(6) + '2024-01-01 00:00:00.123456789'::date;

select '2024-01-01 00:00:00.123456789'::date, '2024-01-01 00:00:00.123456789'::timetz(6), '2024-01-01 00:00:00.123456789'::date + '2024-01-01 00:00:00.123456789'::timetz(6);

select '2024-01-01 00:00:00.123456789'::date, '2024-01-01 00:00:00.123456789'::timetz(6), '2024-01-01 00:00:00.123456789'::timetz(6) + '2024-01-01 00:00:00.123456789'::date;

select '2024-01-01 00:00:00.123456789'::time(6), '0.000000100s'::interval, '2024-01-01 00:00:00.123456789'::time(6) + '0.000000100s'::interval;

select '2024-01-01 00:00:00.123456789'::time(6), '0.000000100s'::interval, '0.000000100s'::interval + '2024-01-01 00:00:00.123456789'::time(6);

select '2024-01-01 00:00:00.123456789'::timetz(6), '0.000000100s'::interval, '2024-01-01 00:00:00.123456789'::timetz(6) + '0.000000100s'::interval;

select '2024-01-01 00:00:00.123456789'::timetz(6), '0.000000100s'::interval, '0.000000100s'::interval + '2024-01-01 00:00:00.123456789'::timetz(6);

select '2024-01-01 00:00:00.123456789'::timestamp(9), '0.000000100s'::interval, '2024-01-01 00:00:00.123456789'::timestamp(9) + '0.000000100s'::interval;

select '2024-01-01 00:00:00.123456789'::timestamp(9), '0.000000100s'::interval, '0.000000100s'::interval + '2024-01-01 00:00:00.123456789'::timestamp(9);

select '2024-01-01 00:00:00.123456789'::timestamptz(9), '0.000000100s'::interval, '2024-01-01 00:00:00.123456789'::timestamptz(9) + '0.000000100s'::interval;

select '2024-01-01 00:00:00.123456789'::timestamptz(9), '0.000000100s'::interval, '0.000000100s'::interval + '2024-01-01 00:00:00.123456789'::timestamptz(9);

select '0.000000001s'::interval + '0.000000001s'::interval;

select '2024-01-01 00:00:00.123456789'::date, '0.000000100s'::interval, '2024-01-01 00:00:00.123456789'::date + '0.000000100s'::interval;

select '2024-01-01 00:00:00.123456789'::date, '0.000000100s'::interval, '0.000000100s'::interval + '2024-01-01 00:00:00.123456789'::date;


--- 1.2 -
select '2024-01-01 00:00:00.123456789'::date, '2024-01-01 00:00:00.123456789'::time(6), '2024-01-01 00:00:00.123456789'::date - '2024-01-01 00:00:00.123456789'::time(6);

select '2024-01-01 00:00:00.123456789'::time(6), '2024-01-01 00:00:00.123456788'::time(6), '2024-01-01 00:00:00.123456789'::time(6) - '2024-01-01 00:00:00.123456788'::time(6);

select '2024-01-01 00:00:00.123456789'::timestamp(9), '2024-01-01 00:00:00.123456788'::timestamp(9), '2024-01-01 00:00:00.123456789'::timestamp(9) - '2024-01-01 00:00:00.123456788'::timestamp(9);

select '2024-01-01 00:00:00.123456789'::timestamptz(9), '2024-01-01 00:00:00.123456788'::timestamptz(9), '2024-01-01 00:00:00.123456789'::timestamptz(9) - '2024-01-01 00:00:00.123456788'::timestamptz(9);

select '2024-01-01 00:00:00.123456789'::timestamp(9), '2024-01-01 00:00:00.123456788'::timestamptz(9), '2024-01-01 00:00:00.123456789'::timestamp(9) - '2024-01-01 00:00:00.123456788'::timestamptz(9);

select '2024-01-01 00:00:00.123456789'::timestamptz(9), '2024-01-01 00:00:00.123456788'::timestamp(9), '2024-01-01 00:00:00.123456789'::timestamptz(9) - '2024-01-01 00:00:00.123456788'::timestamp(9);

select '2024-01-01 00:00:00.123456789'::time(6), '0.000000100s'::interval, '2024-01-01 00:00:00.123456789'::time(6) - '0.000000100s'::interval;

select '2024-01-01 00:00:00.123456789'::timetz(6), '0.000000100s'::interval, '2024-01-01 00:00:00.123456789'::timetz(6) - '0.000000100s'::interval;

select '2024-01-01 00:00:00.123456789'::timestamp(9), '0.000000100s'::interval, '2024-01-01 00:00:00.123456789'::timestamp(9) - '0.000000100s'::interval;

select '2024-01-01 00:00:00.123456789'::timestamptz(9), '0.000000100s'::interval, '2024-01-01 00:00:00.123456789'::timestamptz(9) - '0.000000100s'::interval;

select '2024-01-01 00:00:00.123456789'::date, '0.000000001s'::interval, '2024-01-01 00:00:00.123456789'::date - '0.000000001s'::interval;

select '0.000000002s'::interval - '0.000000001s'::interval;

--- 1.3 *

select int'5' * interval'1ns';

select interval'1ns' * int'5';

select float8'5.5' * interval'2ns';

select interval'2ns' * float8'5.5';

select decimal'5.5' * interval'2ns';

select interval'2ns' * decimal'5.5';

--- 1.4 /

select interval'12ns' / int'2';

select interval'12ns' / float'2.4';

-- 2. PerformCast

--- 2.1 as int family

select '2024-01-01 00:00:00.123456789'::timestamp(9)::int;

select '2024-01-01 00:00:00.123456789'::timestamptz(9)::int;

select '2024-01-01 00:00:00.123456789'::date::int;

select '2.000000001s'::interval::int;

--- 2.2 as float family

select '2024-01-01 00:00:00.123456789'::timestamp(9)::float;

select '2024-01-01 00:00:00.123456789'::timestamptz(9)::float;

select '2024-01-01 00:00:00.123456789'::date::float;

select '2.000000001s'::interval::float;

--- 2.3 as decimal family
--- todo(haokaiwei) is it necessary?
select '2024-01-01 00:00:00.123456789'::date::decimal;

select '2024-01-01 00:00:00.123456789'::timestamp(9)::decimal;

select '2024-01-01 00:00:00.123456789'::timestamptz(9)::decimal;

select '2.000000001s'::interval::decimal;

--- 2.4 as string family

select '2024-01-01 00:00:00.123456789'::date::string;

select '2024-01-01 00:00:00.123456789'::timestamp(9)::string;

select '2024-01-01 00:00:00.123456789'::timestamptz(9)::string;

select '2024-01-01 00:00:00.123456789'::time(6)::string;

select '2024-01-01 00:00:00.123456789'::timetz(6)::string;

select '2.000000001s'::interval::string;

--- 2.5 as date family

select 1::int::date;

select '2024-01-01 00:00:00.123456789'::timestamp::date;

select '2024-01-01 00:00:00.123456789'::timestamptz::date;

--- 2.6 as time family

select '2024-01-01 00:00:00.123456789'::timetz(6)::time(6), '2024-01-01 00:00:00.123456789'::timetz(6)::time;

select '2024-01-01 00:00:00.123456789'::timestamp(9)::time(6), '2024-01-01 00:00:00.123456789'::timestamp(9)::time;

select '2024-01-01 00:00:00.123456789'::timestamptz(9)::time(6), '2024-01-01 00:00:00.123456789'::timestamptz(9)::time;

select '2.000000001s'::interval::time(6), '2.000000001s'::interval::time;

--- 2.6 as timetz family

select '2024-01-01 00:00:00.123456789'::time(6)::timetz(6), '2024-01-01 00:00:00.123456789'::time(6)::timetz;

select '2024-01-01 00:00:00.123456789'::timestamptz(9)::timetz(6), '2024-01-01 00:00:00.123456789'::timestamptz(9)::timetz;

--- 2.7 as timestamp family

select '2024-01-01 00:00:00.123456789'::date::timestamp(9), '2024-01-01 00:00:00.123456789'::date::timestamp;

select '2024-01-01 00:00:00.123456789'::timestamp::int::timestamp;

select '2024-01-01 00:00:00.123456789'::timestamptz(9)::timestamp(9), '2024-01-01 00:00:00.123456789'::timestamptz(9)::timestamp;

--- 2.8 as timestamptz family

select '2024-01-01 00:00:00.123456789'::date::timestamptz(9), '2024-01-01 00:00:00.123456789'::date::timestamptz;

select '2024-01-01 00:00:00.123456789'::timestamp::int::timestamptz;

select '2024-01-01 00:00:00.123456789'::timestamp(9)::timestamptz(9), '2024-01-01 00:00:00.123456789'::timestamp(9)::timestamptz;

--- 2.9 as interval family

select 2::int::interval;

select 2.000000001::float::interval;

select '12:00:00.123456789'::time(6)::interval;

select 2.000000001::decimal::interval;

-- 3. function
select age(timestamptz(9)'2024-01-01 00:00:00.123456789', timestamptz(9)'2024-01-01 00:00:00.123456788');

select extract('nanosecond',timestamp(9)'1970-01-01 00:00:00.123456789');
select extract('nanosecond',timestamptz(9)'1970-01-01 00:00:00.123456789');
select extract('nanosecond',interval'2.000000001s');
select extract('nanosecond',time(6)'1970-01-01 00:00:00.123456789');
select extract('nanosecond',timetz(6)'1970-01-01 00:00:00.123456789');
select extract('nanosecond',1);

select date_trunc('nanosecond',timestamp(9)'1970-01-01 00:00:00.123456789');
select date_trunc('nanosecond',time(6)'1970-01-01 00:00:00.123456789');
select date_trunc('nanosecond',timestamptz(9)'1970-01-01 00:00:00.123456789');

select time_bucket(timestamp(9)'1970-01-01 00:00:00.123456789','1ns');
select time_bucket(timestamptz(9)'1970-01-01 00:00:00.123456789','1ns');

select time_bucket_gapfill(timestamp(9)'1970-01-01 00:00:00.123456789','1ns') tbg group by tbg;
select time_bucket_gapfill(timestamptz(9)'1970-01-01 00:00:00.123456789','1ns') tbg group by tbg;
select time_bucket_gapfill(timestamp(9)'1970-01-01 00:00:00.123456789',1) tbg group by tbg;
select time_bucket_gapfill(timestamptz(9)'1970-01-01 00:00:00.123456789',1) tbg group by tbg;

select extract_duration('nanosecond','2.000000001s');

select timezone('+8:00', '1970-01-01 00:00:00.123456789');
select timezone('+8:00', timestamp(9)'1970-01-01 00:00:00.123456789');
select timezone('+8:00', timestamptz(9)'1970-01-01 00:00:00.123456789');
select timezone('+8:00', time(6)'1970-01-01 00:00:00.123456789');
select timezone('+8:00', timetz(6)'1970-01-01 00:00:00.123456789');

select timezone(timestamp(9)'1970-01-01 00:00:00.123456789','+8:00');
select timezone(timestamptz(9)'1970-01-01 00:00:00.123456789','+8:00');
select timezone(time(6)'1970-01-01 00:00:00.123456789','+8:00');
select timezone(timetz(6)'1970-01-01 00:00:00.123456789','+8:00');



select experimental_strftime(timestamp(9)'1970-01-01 00:00:00.123456789', '%f');
select experimental_strptime('1970-01-01 00:00:00.123456789', '%Y-%m-%d %H:%M:%S.%f');