SELECT pg_typeof(CITEXT 'Foo');
SELECT pg_typeof('Foo'::CITEXT);
SELECT pg_typeof('Foo'::CITEXT::TEXT::CITEXT);
SELECT 'Foo'::CITEXT;
CREATE DATABASE citext_regression;
USE citext_regression;
CREATE TABLE t (
    c CITEXT
);
SHOW CREATE TABLE t;
INSERT INTO t VALUES ('test');
SELECT pg_typeof(c) FROM t LIMIT 1;
SELECT c FROM t WHERE c = 'tEsT';

CREATE TABLE r (
    c CITEXT[]
);
SHOW CREATE TABLE r;
INSERT INTO r VALUES (ARRAY['test', 'TESTER']);
SELECT pg_typeof(c) FROM r LIMIT 1;
SELECT c FROM r WHERE c = ARRAY['test', 'TESTER'];
SELECT c FROM r WHERE c = ARRAY['tEsT', 'tEsTeR']::CITEXT[];

SELECT 'test'::CITEXT = 'TEST'::CITEXT;
SELECT 'test'::CITEXT = 'TEST';
SELECT 'test' = 'TEST'::CITEXT;
SELECT 'test'::CITEXT = 'TESTER';
SELECT e'\u047D'::CITEXT = e'\u047C'::CITEXT;
SELECT e'\u00E9'::CITEXT = e'\u00E8'::CITEXT;
SELECT e'\u00E9'::CITEXT = e'\u0065\u0301'::CITEXT;
SELECT e'\u00C9'::CITEXT = e'\u0065\u0301'::CITEXT;
SELECT e'\u0065\u0301'::CITEXT = e'\u0045\u0301'::CITEXT;
SELECT e'\u00E9'::CITEXT = e'\u00C9'::CITEXT;
SELECT 'I'::CITEXT = 'ı'::CITEXT;
SELECT 'ß'::CITEXT = 'SS'::CITEXT;
SELECT NULL::CITEXT = NULL::CITEXT IS NULL;
SELECT 'A'::CITEXT < 'a'::CITEXT;
SELECT 'a'::CITEXT < 'A'::CITEXT;

SELECT 'test'::CITEXT LIKE 'TEST';
SELECT 'test'::CITEXT ILIKE 'TEST'::CITEXT;
SELECT ARRAY['test', 'TESTER']::CITEXT[] = ARRAY['tEsT', 'tEsTeR']::CITEXT[];
SELECT ARRAY['test', 'TESTER']::CITEXT[] = ARRAY['TESTING', 'TESTER']::CITEXT[];
SELECT ARRAY[]::CITEXT[] = ARRAY['TESTING', 'TESTER']::CITEXT[];

CREATE TABLE citext_with_width_tbl (a CITEXT(10));


CREATE TABLE t_func (c CITEXT);
INSERT INTO t_func VALUES ('TeSting'), ('Alpha-Beta');
-- strpos
SELECT strpos(c, 'test') FROM t_func WHERE c = 'TeSting';
SELECT strpos(c, 'TEST') FROM t_func WHERE c = 'TeSting';

-- replace
SELECT replace(c, 'test', 'X') FROM t_func WHERE c = 'TeSting';
SELECT replace( c, 'TEST', 'X') FROM t_func WHERE c = 'TeSting';

-- split_part
SELECT split_part(c, '-', 1) FROM t_func WHERE c = 'Alpha-Beta';
SELECT split_part(c, '-', 2) FROM t_func WHERE c = 'Alpha-Beta';

-- translate
SELECT translate(c, 'ab', '12') FROM t_func WHERE c = 'Alpha-Beta';
SELECT translate(c, 'AB', '12') FROM t_func WHERE c = 'Alpha-Beta';

-- regexp_replace
SELECT regexp_replace(c, 'test', 'X') FROM t_func WHERE c = 'TeSting';
SELECT regexp_replace(c, 'TEST', 'X') FROM t_func WHERE c = 'TeSting';
SELECT regexp_replace(c, 'test', 'X', 'g') FROM t_func WHERE c = 'TeSting';

-- POSTGRES TEST
-- Multibyte sanity tests.
SELECT 'À'::citext =  'À'::citext AS t;
SELECT 'À'::citext =  'à'::citext AS t;
SELECT 'À'::text   =  'à'::text   AS f;
SELECT 'À'::citext <> 'B'::citext AS t;

-- Test combining characters making up canonically equivalent strings.
SELECT 'Ä'::text   <> 'Ä'::text   AS t;
SELECT 'Ä'::citext <> 'Ä'::citext AS t;

-- Test the Turkish dotted I. The lowercase is a single byte while the
-- uppercase is multibyte. This is why the comparison code can't be optimized
-- to compare string lengths.
SELECT 'i'::citext = 'İ'::citext AS t;

-- Regression.
SELECT 'láska'::citext <> 'laská'::citext AS t;

SELECT 'Ask Bjørn Hansen'::citext = 'Ask Bjørn Hansen'::citext AS t;
SELECT 'Ask Bjørn Hansen'::citext = 'ASK BJØRN HANSEN'::citext AS t;
SELECT 'Ask Bjørn Hansen'::citext <> 'Ask Bjorn Hansen'::citext AS t;
SELECT 'Ask Bjørn Hansen'::citext <> 'ASK BJORN HANSEN'::citext AS t;

-- Test the operators

-- Test = and <>.
SELECT 'a'::citext = 'a'::citext AS t;
SELECT 'a'::citext = 'A'::citext AS t;
SELECT 'a'::citext = 'A'::text AS f;        -- text wins the discussion
SELECT 'a'::citext = 'b'::citext AS f;
SELECT 'a'::citext = 'ab'::citext AS f;
SELECT 'a'::citext <> 'ab'::citext AS t;

-- Test > and >=
SELECT 'B'::citext > 'a'::citext AS t;
SELECT 'b'::citext >  'A'::citext AS t;
SELECT 'B'::citext >  'b'::citext AS f;
SELECT 'B'::citext >= 'b'::citext AS t;

-- Test < and <=
SELECT 'a'::citext <  'B'::citext AS t;
SELECT 'a'::citext <= 'B'::citext AS t;

-- Test implicit casting. citext casts to text, but not vice-versa.
SELECT 'a'::citext = 'a'::text   AS t;
SELECT 'A'::text  <> 'a'::citext AS t;

SELECT 'B'::citext <  'a'::text AS t;  -- text wins.
SELECT 'B'::citext <= 'a'::text AS t;  -- text wins.

SELECT 'a'::citext >  'B'::text AS t;  -- text wins.
SELECT 'a'::citext >= 'B'::text AS t;  -- text wins.

-- Test implicit casting. citext casts to varchar, but not vice-versa.
SELECT 'a'::citext = 'a'::varchar   AS t;
SELECT 'A'::varchar  <> 'a'::citext AS t;

SELECT 'B'::citext <  'a'::varchar AS t;  -- varchar wins.
SELECT 'B'::citext <= 'a'::varchar AS t;  -- varchar wins.

SELECT 'a'::citext >  'B'::varchar AS t;  -- varchar wins.
SELECT 'a'::citext >= 'B'::varchar AS t;  -- varchar wins.

SELECT 'aardvark'::citext = 'aardvark'::citext AS t;
SELECT 'aardvark'::citext = 'aardVark'::citext AS t;

-- Do some tests using a table
CREATE TABLE try (
   name citext PRIMARY KEY
);

INSERT INTO try (name)
VALUES ('a'), ('ab'), ('â'), ('aba'), ('b'), ('ba'), ('bab'), ('AZ');

SELECT name, 'a' = name AS eq_a   FROM try WHERE name <> 'â' order by name;
SELECT name, 'a' = name AS t      FROM try where name = 'a' order by name;
SELECT name, 'A' = name AS "eq_A" FROM try WHERE name <> 'â' order by name;
SELECT name, 'A' = name AS t      FROM try where name = 'A' order by name;
SELECT name, 'A' = name AS t      FROM try where name = 'A' order by name;

-- expected failures on duplicate key
INSERT INTO try (name) VALUES ('a');
INSERT INTO try (name) VALUES ('A');
INSERT INTO try (name) VALUES ('aB');


-- Test aggregate functions and sort ordering
CREATE TABLE srt (
   name CITEXT
);

INSERT INTO srt (name)
VALUES ('abb'),
       ('ABA'),
       ('ABC'),
       ('abd');

CREATE INDEX srt_name ON srt (name);

-- Check the min() and max() aggregates, with and without index.
SELECT MIN(name) AS "ABA" FROM srt;
SELECT MAX(name) AS abd FROM srt;
SELECT MIN(name) AS "ABA" FROM srt;
SELECT MAX(name) AS abd FROM srt;

-- Check sorting likewise
SELECT name FROM srt ORDER BY name;
SELECT name FROM srt ORDER BY name;

-- Test assignment casts.
SELECT LOWER(name) as aba FROM srt WHERE name = 'ABA'::text order by name;
SELECT LOWER(name) as aba FROM srt WHERE name = 'ABA'::varchar order by name;
SELECT LOWER(name) as aba FROM srt WHERE name = 'ABA'::bpchar order by name;
SELECT LOWER(name) as aba FROM srt WHERE name = 'ABA' order by name;
SELECT LOWER(name) as aba FROM srt WHERE name = 'ABA'::citext order by name;

-- LIKE should be case-insensitive
SELECT name FROM srt WHERE name     LIKE '%a%' ORDER BY name;
SELECT name FROM srt WHERE name NOT LIKE '%b%' ORDER BY name;
SELECT name FROM srt WHERE name     LIKE '%A%' ORDER BY name;
SELECT name FROM srt WHERE name NOT LIKE '%B%' ORDER BY name;

-- ~ should be case-insensitive
SELECT name FROM srt WHERE name ~  '^a' ORDER BY name;
SELECT name FROM srt WHERE name !~ 'a$' ORDER BY name;
SELECT name FROM srt WHERE name ~  '^A' ORDER BY name;
SELECT name FROM srt WHERE name !~ 'A$' ORDER BY name;

-- SIMILAR TO should be case-insensitive.
SELECT name FROM srt WHERE name SIMILAR TO '%a.*';
SELECT name FROM srt WHERE name SIMILAR TO '%A.*';

-- IN: bare string literals should follow CITEXT semantics.
SELECT 'ABA'::citext IN ('aba', 'zzz') AS t;
SELECT 'ABA'::citext IN ('ABC', 'abd') AS f;

-- IN: typed plain string should win.
SELECT 'ABA'::citext IN ('aba'::text) AS f;
SELECT 'ABA'::citext IN ('ABA'::text) AS t;
SELECT 'ABA'::citext IN ('aba'::varchar) AS f;
SELECT 'ABA'::citext IN ('ABA'::varchar) AS t;

-- IN against table values.
SELECT name FROM srt WHERE name IN ('aba', 'ABC') ORDER BY name;
SELECT name FROM srt WHERE name IN ('ABA'::text, 'abc'::text) ORDER BY name;

-- IN subquery: bare string literal should be coerced to CITEXT.
SELECT 'ABA' IN (SELECT name FROM srt) AS t;
SELECT 'zzz' IN (SELECT name FROM srt) AS f;

-- IN subquery: typed string should win.
SELECT 'ABA'::text IN (SELECT name FROM srt) AS t;
SELECT 'aba'::text IN (SELECT name FROM srt) AS f;
SELECT 'ABA'::citext IN (SELECT name::text FROM srt) AS t;
SELECT 'aba'::citext IN (SELECT name::text FROM srt) AS f;

-- BETWEEN: bare string literals should follow CITEXT semantics.
SELECT 'ABC'::citext BETWEEN 'aba' AND 'abd' AS t;
SELECT 'abb'::citext BETWEEN 'ABA' AND 'ABC' AS t;
SELECT 'zzz'::citext BETWEEN 'aba' AND 'abd' AS f;

-- BETWEEN: typed plain string should win.
SELECT 'ABC'::citext BETWEEN 'aba'::text AND 'abd'::text AS f;
SELECT 'ABC'::citext BETWEEN 'ABA'::text AND 'abd'::text AS t;
SELECT 'abb'::citext BETWEEN 'ABA'::varchar AND 'abc'::varchar AS f;
SELECT 'abb'::citext BETWEEN 'ABA'::varchar AND 'abd'::varchar AS t;

-- BETWEEN with table values.
SELECT name, name BETWEEN 'aba' AND 'abd' AS in_range FROM srt ORDER BY name;
SELECT name, name BETWEEN 'aba'::text AND 'abd'::text AS in_range_text FROM srt ORDER BY name;

-- DISTINCT: use a dedicated table with case variants.
CREATE TABLE srt_distinct (
    name CITEXT
);

INSERT INTO srt_distinct (name)
VALUES ('abb'),
       ('ABB'),
       ('aba'),
       ('ABA'),
       ('abc');

SELECT DISTINCT name FROM srt_distinct ORDER BY name;
SELECT COUNT(DISTINCT name) = 3::bigint AS t FROM srt_distinct;

-- SIMILAR TO: case-insensitive for CITEXT.
SELECT name FROM srt WHERE name SIMILAR TO '%a.*' ORDER BY name;
SELECT name FROM srt WHERE name SIMILAR TO '%A.*' ORDER BY name;
SELECT 'ABC'::citext SIMILAR TO 'a.*' AS t;
SELECT 'ABC'::citext SIMILAR TO 'A.*' AS t;
SELECT 'ABC'::citext NOT SIMILAR TO 'z.*' AS t;

-- SIMILAR TO: typed plain string should win.
SELECT 'ABC'::citext SIMILAR TO 'a.*'::text AS f;
SELECT 'ABC'::citext SIMILAR TO 'A.*'::text AS t;

-- || : ordinary string concatenation, then compare using CITEXT semantics.
SELECT 'D'::citext || 'avid'::citext = 'David'::citext AS t;
SELECT 'AB'::citext || 'C' = 'abc'::citext AS t;
SELECT 'AB' || 'C'::citext = 'abc'::citext AS t;
SELECT name || '_x' = lower(name) || '_X'::citext AS t FROM srt ORDER BY name;

-- concat(): ordinary string concatenation result compared with CITEXT.
SELECT concat('D'::citext, 'avid'::citext) = 'David'::citext AS t;  -- fail
SELECT concat('A'::citext, 'B', 'C') = 'abc'::citext AS t;          -- fail
SELECT concat(name, '_x') = lower(name) || '_X'::citext AS t FROM srt ORDER BY name; -- fail

-- IS DISTINCT FROM / IS NOT DISTINCT FROM
SELECT 'ABA'::citext IS DISTINCT FROM 'aba'::citext AS f;
SELECT 'ABA'::citext IS NOT DISTINCT FROM 'aba'::citext AS t;
SELECT 'ABA'::citext IS DISTINCT FROM 'abc'::citext AS t;
SELECT 'ABA'::citext IS NOT DISTINCT FROM 'abc'::citext AS f;

-- Typed plain string should win.
SELECT 'ABA'::citext IS DISTINCT FROM 'aba'::text AS t;
SELECT 'ABA'::citext IS NOT DISTINCT FROM 'ABA'::text AS t;
SELECT 'ABA'::citext IS DISTINCT FROM 'aba'::varchar AS t;
SELECT 'ABA'::citext IS NOT DISTINCT FROM 'ABA'::varchar AS t;

-- NULL handling.
SELECT NULL::citext IS DISTINCT FROM NULL::citext AS f;
SELECT NULL::citext IS NOT DISTINCT FROM NULL::citext AS t;
SELECT NULL::citext IS DISTINCT FROM 'abc'::citext AS t;
SELECT NULL::citext IS NOT DISTINCT FROM 'abc'::citext AS f;

-- Explicit casts.
SELECT true::citext = 'true' AS t;
SELECT 'true'::citext::boolean = true AS t;

SELECT 4::citext = '4' AS t;
SELECT 4::int4::citext = '4' AS t;
SELECT '4'::citext::int4 = 4 AS t;
SELECT 4::integer::citext = '4' AS t;
SELECT '4'::citext::integer = 4 AS t;

SELECT 4::int8::citext = '4' AS t;
SELECT '4'::citext::int8 = 4 AS t;
SELECT 4::bigint::citext = '4' AS t;
SELECT '4'::citext::bigint = 4 AS t;

SELECT 4::int2::citext = '4' AS t;
SELECT '4'::citext::int2 = 4 AS t;
SELECT 4::smallint::citext = '4' AS t;
SELECT '4'::citext::smallint = 4 AS t;

SELECT 4.0::numeric = '4.0' AS t;
SELECT '4.0'::citext::numeric = 4.0 AS t;
SELECT 4.0::decimal = '4.0' AS t;
SELECT '4.0'::citext::decimal = 4.0 AS t;

SELECT 4.0::real = '4.0' AS t;
SELECT '4.0'::citext::real = 4.0 AS t;
SELECT 4.0::float4 = '4.0' AS t;
SELECT '4.0'::citext::float4 = 4.0 AS t;

SELECT 4.0::double precision = '4.0' AS t;
SELECT '4.0'::citext::double precision = 4.0 AS t;
SELECT 4.0::float8 = '4.0' AS t;
SELECT '4.0'::citext::float8 = 4.0 AS t;

SELECT 'foo'::name::citext = 'foo' AS t;
SELECT 'foo'::citext::name = 'foo'::name AS t;

SELECT 'f'::char::citext = 'f' AS t;
SELECT 'f'::citext::char = 'f'::char AS t;

SELECT 'f'::"char"::citext = 'f' AS t;
SELECT 'f'::citext::"char" = 'f'::"char" AS t;

SELECT 'a'::char::citext = 'a' AS t;
SELECT 'a'::citext::char = 'a'::char AS t;

SELECT 'foo'::varchar::citext = 'foo' AS t;
SELECT 'foo'::citext::varchar = 'foo'::varchar AS t;

SELECT 'foo'::text::citext = 'foo' AS t;
SELECT 'foo'::citext::text = 'foo'::text AS t;

SELECT '192.168.100.128'::inet::citext = '192.168.100.128/32' AS t;
SELECT '192.168.100.128'::citext::inet = '192.168.100.128'::inet AS t;

SELECT '1999-01-08 04:05:06'::timestamp::citext = '1999-01-08 04:05:06'::timestamp::text AS t;
SELECT '1999-01-08 04:05:06'::citext::timestamp = '1999-01-08 04:05:06'::timestamp AS t;
SELECT '1999-01-08 04:05:06'::timestamptz::citext = '1999-01-08 04:05:06'::timestamptz::text AS t;
SELECT '1999-01-08 04:05:06'::citext::timestamptz = '1999-01-08 04:05:06'::timestamptz AS t;

SELECT '1 hour'::interval::citext = '1 hour'::interval::text AS t;
SELECT '1 hour'::citext::interval = '1 hour'::interval AS t;

SELECT '1999-01-08'::date::citext = '1999-01-08'::date::text AS t;
SELECT '1999-01-08'::citext::date = '1999-01-08'::date AS t;

SELECT '04:05:06'::time::citext = '04:05:06' AS t;
SELECT '04:05:06'::citext::time = '04:05:06'::time AS t;
SELECT '04:05:06'::timetz::citext = '04:05:06'::timetz::text AS t;
SELECT '04:05:06'::citext::timetz = '04:05:06'::timetz AS t;

SELECT '101'::bit::citext = '101'::bit::text AS t;
SELECT '101'::citext::bit = '101'::text::bit AS t;
SELECT '101'::bit varying::citext = '101'::bit varying::text AS t;
SELECT '101'::citext::bit varying = '101'::text::bit varying AS t;

SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid::citext = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS t;
SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::citext::uuid = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid AS t;

-- Assignment casts.
CREATE TABLE caster (
                        citext      citext,
                        text        text,
                        varchar     varchar,
                        bpchar      bpchar,
                        char        char,
                        chr         "char",
                        name        name,
                        bytea       bytea,
                        boolean     boolean,
                        float4      float4,
                        float8      float8,
                        numeric     numeric,
                        int8        int8,
                        int4        int4,
                        int2        int2,
                        inet        inet,
                        timestamp   timestamp,
                        timestamptz timestamptz,
                        interval    interval,
                        date        date,
                        time        time,
                        timetz      timetz,
                        bit         bit,
                        bitv        bit varying,
                        uuid        uuid
);

INSERT INTO caster (text)          VALUES ('foo'::citext);
INSERT INTO caster (citext)        VALUES ('foo'::text);

INSERT INTO caster (varchar)       VALUES ('foo'::text);
INSERT INTO caster (text)          VALUES ('foo'::varchar);
INSERT INTO caster (varchar)       VALUES ('foo'::citext);
INSERT INTO caster (citext)        VALUES ('foo'::varchar);

INSERT INTO caster (bpchar)        VALUES ('foo'::text);
INSERT INTO caster (text)          VALUES ('foo'::bpchar);
INSERT INTO caster (bpchar)        VALUES ('foo'::citext);
INSERT INTO caster (citext)        VALUES ('foo'::bpchar);

INSERT INTO caster (char)          VALUES ('f'::text);
INSERT INTO caster (text)          VALUES ('f'::char);
INSERT INTO caster (char)          VALUES ('f'::citext);
INSERT INTO caster (citext)        VALUES ('f'::char);

INSERT INTO caster (chr)           VALUES ('f'::text);
INSERT INTO caster (text)          VALUES ('f'::"char");
INSERT INTO caster (chr)           VALUES ('f'::citext);  -- requires cast
INSERT INTO caster (chr)           VALUES ('f'::citext::text);
INSERT INTO caster (citext)        VALUES ('f'::"char");

INSERT INTO caster (name)          VALUES ('foo'::text);
INSERT INTO caster (text)          VALUES ('foo'::name);
INSERT INTO caster (name)          VALUES ('foo'::citext);
INSERT INTO caster (citext)        VALUES ('foo'::name);

-- Cannot cast to bytea on assignment.
INSERT INTO caster (bytea)         VALUES ('foo'::text);
INSERT INTO caster (text)          VALUES ('foo'::bytea);
INSERT INTO caster (bytea)         VALUES ('foo'::citext);
INSERT INTO caster (citext)        VALUES ('foo'::bytea);

-- Cannot cast to boolean on assignment.
INSERT INTO caster (boolean)       VALUES ('t'::text);
INSERT INTO caster (text)          VALUES ('t'::boolean);
INSERT INTO caster (boolean)       VALUES ('t'::citext);
INSERT INTO caster (citext)        VALUES ('t'::boolean);

-- Cannot cast to float8 on assignment.
INSERT INTO caster (float8)        VALUES ('12.42'::text);
INSERT INTO caster (text)          VALUES ('12.42'::float8);
INSERT INTO caster (float8)        VALUES ('12.42'::citext);
INSERT INTO caster (citext)        VALUES ('12.42'::float8);

-- Cannot cast to float4 on assignment.
INSERT INTO caster (float4)        VALUES ('12.42'::text);
INSERT INTO caster (text)          VALUES ('12.42'::float4);
INSERT INTO caster (float4)        VALUES ('12.42'::citext);
INSERT INTO caster (citext)        VALUES ('12.42'::float4);

-- Cannot cast to numeric on assignment.
INSERT INTO caster (numeric)       VALUES ('12.42'::text);
INSERT INTO caster (text)          VALUES ('12.42'::numeric);
INSERT INTO caster (numeric)       VALUES ('12.42'::citext);
INSERT INTO caster (citext)        VALUES ('12.42'::numeric);

-- Cannot cast to int8 on assignment.
INSERT INTO caster (int8)          VALUES ('12'::text);
INSERT INTO caster (text)          VALUES ('12'::int8);
INSERT INTO caster (int8)          VALUES ('12'::citext);
INSERT INTO caster (citext)        VALUES ('12'::int8);

-- Cannot cast to int4 on assignment.
INSERT INTO caster (int4)          VALUES ('12'::text);
INSERT INTO caster (text)          VALUES ('12'::int4);
INSERT INTO caster (int4)          VALUES ('12'::citext);
INSERT INTO caster (citext)        VALUES ('12'::int4);

-- Cannot cast to int2 on assignment.
INSERT INTO caster (int2)          VALUES ('12'::text);
INSERT INTO caster (text)          VALUES ('12'::int2);
INSERT INTO caster (int2)          VALUES ('12'::citext);
INSERT INTO caster (citext)        VALUES ('12'::int2);


-- Cannot cast to inet on assignment.
INSERT INTO caster (inet)          VALUES ('192.168.100.128'::text);
INSERT INTO caster (text)          VALUES ('192.168.100.128'::inet);
INSERT INTO caster (inet)          VALUES ('192.168.100.128'::citext);
INSERT INTO caster (citext)        VALUES ('192.168.100.128'::inet);

-- Cannot cast to timestamp on assignment.
INSERT INTO caster (timestamp)     VALUES ('1999-01-08 04:05:06'::text);
INSERT INTO caster (text)          VALUES ('1999-01-08 04:05:06'::timestamp);
INSERT INTO caster (timestamp)     VALUES ('1999-01-08 04:05:06'::citext);
INSERT INTO caster (citext)        VALUES ('1999-01-08 04:05:06'::timestamp);

-- Cannot cast to timestamptz on assignment.
INSERT INTO caster (timestamptz)   VALUES ('1999-01-08 04:05:06'::text);
INSERT INTO caster (text)          VALUES ('1999-01-08 04:05:06'::timestamptz);
INSERT INTO caster (timestamptz)   VALUES ('1999-01-08 04:05:06'::citext);
INSERT INTO caster (citext)        VALUES ('1999-01-08 04:05:06'::timestamptz);

-- Cannot cast to interval on assignment.
INSERT INTO caster (interval)      VALUES ('1 hour'::text);
INSERT INTO caster (text)          VALUES ('1 hour'::interval);
INSERT INTO caster (interval)      VALUES ('1 hour'::citext);
INSERT INTO caster (citext)        VALUES ('1 hour'::interval);

-- Cannot cast to date on assignment.
INSERT INTO caster (date)          VALUES ('1999-01-08'::text);
INSERT INTO caster (text)          VALUES ('1999-01-08'::date);
INSERT INTO caster (date)          VALUES ('1999-01-08'::citext);
INSERT INTO caster (citext)        VALUES ('1999-01-08'::date);

-- Cannot cast to time on assignment.
INSERT INTO caster (time)          VALUES ('04:05:06'::text);
INSERT INTO caster (text)          VALUES ('04:05:06'::time);
INSERT INTO caster (time)          VALUES ('04:05:06'::citext);
INSERT INTO caster (citext)        VALUES ('04:05:06'::time);

-- Cannot cast to timetz on assignment.
INSERT INTO caster (timetz)        VALUES ('04:05:06'::text);
INSERT INTO caster (text)          VALUES ('04:05:06'::timetz);
INSERT INTO caster (timetz)        VALUES ('04:05:06'::citext);
INSERT INTO caster (citext)        VALUES ('04:05:06'::timetz);

-- Cannot cast to bit on assignment.
INSERT INTO caster (bit)           VALUES ('101'::text);
INSERT INTO caster (text)          VALUES ('101'::bit);
INSERT INTO caster (bit)           VALUES ('101'::citext);
INSERT INTO caster (citext)        VALUES ('101'::bit);

-- Cannot cast to bit varying on assignment.
INSERT INTO caster (bitv)          VALUES ('101'::text);
INSERT INTO caster (text)          VALUES ('101'::bit varying);
INSERT INTO caster (bitv)          VALUES ('101'::citext);
INSERT INTO caster (citext)        VALUES ('101'::bit varying);

-- Cannot cast to uuid on assignment.
INSERT INTO caster (uuid)          VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::text);
INSERT INTO caster (text)          VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid);
INSERT INTO caster (uuid)          VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::citext);
INSERT INTO caster (citext)        VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid);

-- Table 9-5. SQL String Functions and Operators
SELECT 'D'::citext || 'avid'::citext = 'David'::citext AS citext_concat;
SELECT 'Value: '::citext || 42 = 'Value: 42' AS text_concat;
SELECT  42 || ': value'::citext ='42: value' AS int_concat;
SELECT bit_length('jose'::citext) = 32 AS t;
SELECT bit_length( name ) = bit_length( name::text ) AS t FROM srt;
SELECT char_length( name ) = char_length( name::text ) AS t FROM srt;
SELECT lower( name ) = lower( name::text ) AS t FROM srt;
SELECT octet_length( name ) = octet_length( name::text ) AS t FROM srt;
SELECT overlay( name placing 'hom' from 2 for 4) = overlay( name::text placing 'hom' from 2 for 4) AS t FROM srt;
SELECT position( 'a' IN name ) = position( 'a' IN name::text ) AS t FROM srt;

SELECT substr('alphabet'::citext, 3)       = 'phabet' AS t;
SELECT substr('alphabet'::citext, 3, 2)    = 'ph' AS t;

SELECT substring('alphabet'::citext, 3)    = 'phabet' AS t;
SELECT substring('alphabet'::citext, 3, 2) = 'ph' AS t;
SELECT substring('Thomas'::citext from 2 for 3) = 'hom' AS t;
SELECT substring('Thomas'::citext from 2) = 'homas' AS t;
SELECT substring('Thomas'::citext from '...$') = 'mas' AS t;

SELECT trim('    trim    '::citext)               = 'trim' AS t;
SELECT trim('xxxxxtrimxxxx'::citext, 'x'::citext) = 'trim' AS t;
SELECT trim('xxxxxxtrimxxxx'::text,  'x'::citext) = 'trim' AS t;
SELECT trim('xxxxxtrimxxxx'::text,   'x'::citext) = 'trim' AS t;

SELECT upper( name ) = upper( name::text ) AS t FROM srt;

-- Table 9-6. Other String Functions.
SELECT ascii( name ) = ascii( name::text ) AS t FROM srt;

SELECT btrim('    trim'::citext                   ) = 'trim' AS t;
SELECT btrim('xxxxxtrimxxxx'::citext, 'x'::citext ) = 'trim' AS t;
SELECT btrim('xyxtrimyyx'::citext,    'xy'::citext) = 'trim' AS t;
SELECT btrim('xyxtrimyyx'::text,      'xy'::citext) = 'trim' AS t;
SELECT btrim('xyxtrimyyx'::citext,    'xy'::text  ) = 'trim' AS t;

-- chr() takes an int and returns text.
-- convert() and convert_from take bytea and return text.

SELECT decode('MTIzAAE='::citext, 'base64') = decode('MTIzAAE='::text, 'base64') AS t;
-- encode() takes bytea and returns text.
SELECT initcap('hi THOMAS'::citext) = initcap('hi THOMAS'::text) AS t;
SELECT length( name ) = length( name::text ) AS t FROM srt;

SELECT lpad('hi'::citext, 5              ) = '   hi' AS t;
SELECT lpad('hi'::citext, 5, 'xy'::citext) = 'xyxhi' AS t;
SELECT lpad('hi'::text,   5, 'xy'::citext) = 'xyxhi' AS t;
SELECT lpad('hi'::citext, 5, 'xy'::text  ) = 'xyxhi' AS t;

SELECT ltrim('    trim'::citext               ) = 'trim' AS t;
SELECT ltrim('zzzytrim'::citext, 'xyz'::citext) = 'trim' AS t;
SELECT ltrim('zzzytrim'::text,   'xyz'::citext) = 'trim' AS t;
SELECT ltrim('zzzytrim'::citext, 'xyz'::text  ) = 'trim' AS t;

-- pg_client_encoding() takes no args and returns name.
SELECT quote_ident( name ) = quote_ident( name::text ) AS t FROM srt;
SELECT quote_literal( name ) = quote_literal( name::text ) AS t FROM srt;

SELECT regexp_replace('Thomas'::citext, '.[mN]a.',         'M') = 'ThM' AS t;
SELECT regexp_replace('Thomas'::citext, '.[MN]A.',         'M') = 'ThM' AS t;
SELECT regexp_replace('Thomas',         '.[MN]A.'::citext, 'M') = 'ThM' AS t;
SELECT regexp_replace('Thomas'::citext, '.[MN]A.'::citext, 'M') = 'ThM' AS t;
-- c forces case-sensitive
SELECT regexp_replace('Thomas'::citext, '.[MN]A.'::citext, 'M', 'c') = 'Thomas' AS t;

SELECT repeat('Pg'::citext, 4) = 'PgPgPgPg' AS t;

SELECT replace('abcdefabcdef'::citext, 'cd', 'XX') = 'abXXefabXXef' AS t;
SELECT replace('abcdefabcdef'::citext, 'CD', 'XX') = 'abXXefabXXef' AS t;
SELECT replace('ab^is$abcdef'::citext, '^is$', 'XX') = 'abXXabcdef' AS t;
SELECT replace('abcdefabcdef', 'cd'::citext, 'XX') = 'abXXefabXXef' AS t;
SELECT replace('abcdefabcdef', 'CD'::citext, 'XX') = 'abXXefabXXef' AS t;
SELECT replace('ab^is$abcdef', '^is$'::citext, 'XX') = 'abXXabcdef' AS t;
SELECT replace('abcdefabcdef'::citext, 'cd'::citext, 'XX') = 'abXXefabXXef' AS t;
SELECT replace('abcdefabcdef'::citext, 'CD'::citext, 'XX') = 'abXXefabXXef' AS t;
SELECT replace('ab^is$abcdef'::citext, '^is$'::citext, 'XX') = 'abXXabcdef' AS t;

SELECT rpad('hi'::citext, 5              ) = 'hi   ' AS t;
SELECT rpad('hi'::citext, 5, 'xy'::citext) = 'hixyx' AS t;
SELECT rpad('hi'::text,   5, 'xy'::citext) = 'hixyx' AS t;
SELECT rpad('hi'::citext, 5, 'xy'::text  ) = 'hixyx' AS t;

SELECT rtrim('trim    '::citext             ) = 'trim' AS t;
SELECT rtrim('trimxxxx'::citext, 'x'::citext) = 'trim' AS t;
SELECT rtrim('trimxxxx'::text,   'x'::citext) = 'trim' AS t;
SELECT rtrim('trimxxxx'::text,   'x'::text  ) = 'trim' AS t;

SELECT split_part('abc~@~def~@~ghi'::citext, '~@~', 2) = 'def' AS t;
SELECT split_part('abcTdefTghi'::citext, 't', 2) = 'def' AS t;
SELECT split_part('abcTdefTghi'::citext, 't'::citext, 2) = 'def' AS t;
SELECT split_part('abcTdefTghi', 't'::citext, 2) = 'def' AS t;

SELECT strpos('high'::citext, 'gh'        ) = 3 AS t;
SELECT strpos('high',         'gh'::citext) = 3 AS t;
SELECT strpos('high'::citext, 'gh'::citext) = 3 AS t;
SELECT strpos('high'::citext, 'GH'        ) = 3 AS t;
SELECT strpos('high',         'GH'::citext) = 3 AS t;
SELECT strpos('high'::citext, 'GH'::citext) = 3 AS t;

-- to_ascii() does not support UTF-8.
-- to_hex() takes a numeric argument.
SELECT substr('alphabet', 3, 2) = 'ph' AS t;
SELECT translate('abcdefabcdef'::citext, 'cd',         'XX') = 'abXXefabXXef' AS t;
SELECT translate('abcdefabcdef'::citext, 'CD',         'XX') = 'abXXefabXXef' AS t;
SELECT translate('abcdefabcdef'::citext, 'CD'::citext, 'XX') = 'abXXefabXXef' AS t;
SELECT translate('abcdefabcdef',         'CD'::citext, 'XX') = 'abXXefabXXef' AS t;

-- Table 9-20. Formatting Functions
SELECT str_to_date('2000-01-01'::citext, '%Y-%m-%d'::citext)
           = str_to_date('2000-01-01',         '%Y-%m-%d') AS t;
SELECT str_to_date('2000-01-01'::citext, '%Y-%m-%d')
           = str_to_date('2000-01-01',         '%Y-%m-%d') AS t;
SELECT str_to_date('2000-01-01',         '%Y-%m-%d'::citext)
           = str_to_date('2000-01-01',         '%Y-%m-%d') AS t;

SELECT to_timestamp('2000-01-01 01:01:01.1'::citext, 'YYYY-MM-DD HH24:MI:SS.US'::citext)
           = to_timestamp('2000-01-01 01:01:01.1',         'YYYY-MM-DD HH24:MI:SS.US') AS t;
SELECT to_timestamp('2000-01-01 01:01:01.1'::citext, 'YYYY-MM-DD HH24:MI:SS.US')
           = to_timestamp('2000-01-01 01:01:01.1',         'YYYY-MM-DD HH24:MI:SS.US') AS t;
SELECT to_timestamp('2000-01-01 01:01:01.1',         'YYYY-MM-DD HH24:MI:SS.US'::citext)
           = to_timestamp('2000-01-01 01:01:01.1',         'YYYY-MM-DD HH24:MI:SS.US') AS t;

-- Try assigning function results to a column.
SELECT COUNT(*) = 8::bigint AS t FROM try;

INSERT INTO try
VALUES
    (lower('ABCde')),
    (upper('abcD')),
    (substring('abcdef', 1, 3)),
    (replace('abcabc', 'ab', 'XY')),
    (concat('D', 'avid')),
    (CAST(now() AS STRING)),
    (CAST(current_date AS STRING)),
    (CAST(125 AS STRING));

SELECT COUNT(*) = 16::bigint AS t FROM try;

SELECT like_escape( name, '' ) = like_escape( name::text, '' ) AS t FROM srt;
SELECT like_escape( name,name, '' ) = like_escape( name::text, name::text, '' ) AS t FROM srt;
SELECT like_escape( name::text, ''::citext ) = like_escape( name::text, '' ) AS t FROM srt;
SELECT like_escape( name::text,name::text, ''::citext ) = like_escape( name::text,name::text, '' ) AS t FROM srt;

-- Ensure correct behavior for citext with materialized views.
CREATE TABLE citext_table (
                              id serial primary key,
                              name citext
);
INSERT INTO citext_table (name)
VALUES ('one'), ('two'), ('three'), (NULL), (NULL);
CREATE MATERIALIZED VIEW citext_matview AS
SELECT * FROM citext_table;
CREATE UNIQUE INDEX citext_matview_id
    ON citext_matview (id);
SELECT m.name,t.name
FROM citext_matview m
         FULL JOIN citext_table t ON (t.name::text = m.name)
WHERE t.name IS NOT NULL and m.name IS NOT NULL;
UPDATE citext_table SET name = 'Two' WHERE name = 'TWO';
SELECT m.name,t.name
FROM citext_matview m
         FULL JOIN citext_table t ON (t.name::text = m.name)
WHERE t.name IS NOT NULL and m.name IS NOT NULL;
REFRESH MATERIALIZED VIEW citext_matview;
SELECT name FROM citext_matview ORDER BY id;


-- NOT IN: bare string should follow CITEXT
SELECT 'ABA'::citext NOT IN ('abc', 'abd') AS t;
SELECT 'ABA'::citext NOT IN ('ABA', 'abd') AS f;

-- NOT IN: typed plain string should win
SELECT 'ABA'::citext NOT IN ('ABA'::text) AS f;
SELECT 'aba'::text NOT IN (SELECT name FROM srt) AS t;

-- NULL semantics for tuple RHS
SELECT 'ABA'::citext NOT IN ('zzz', NULL::citext) IS NULL AS t;
SELECT 'ABA'::text   NOT IN ('zzz'::citext, NULL::citext) IS NULL AS t;

-- NULL semantics for subquery RHS
SELECT 'ABA'::citext NOT IN (SELECT name FROM citext_table) IS NULL AS t;
SELECT 'ABA'::text   NOT IN (SELECT name FROM citext_table) IS NULL AS t;

-- GROUP BY should fold case variants together
SELECT COUNT(*) = 3::bigint AS t
FROM (
         SELECT name
         FROM srt_distinct
         GROUP BY name
     ) g;

SELECT SUM(cnt) = 5::bigint AS t
FROM (
         SELECT COUNT(*) AS cnt
         FROM srt_distinct
         GROUP BY name
     ) g;

-- HAVING on grouped CITEXT key
SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT name
         FROM srt_distinct
         GROUP BY name
         HAVING name = 'ABA'
     ) g;

CREATE TABLE srt_join (
                          name CITEXT
);

INSERT INTO srt_join VALUES ('aba'), ('ABC');

-- CITEXT join: case-insensitive
SELECT COUNT(*) = 2::bigint AS t
FROM srt a
         JOIN srt_join b ON a.name = b.name;

-- typed plain string should win
SELECT COUNT(*) = 1::bigint AS t
FROM srt a
         JOIN srt_join b ON a.name = b.name::text;

SELECT 'ABA'::citext = ANY (ARRAY['aba', 'zzz']::citext[]) AS t;
SELECT 'aba'::text   = ANY (ARRAY['ABA', 'zzz']::citext[]) AS f;

SELECT 'ABA'::citext = ALL (ARRAY['aba', 'ABA']::citext[]) AS t;
SELECT 'ABA'::citext = SOME (ARRAY['zzz', 'aba']::citext[]) AS t;

-- LIKE / ILIKE mixed forms
SELECT 'ABC'::citext LIKE  'a%'::text AS f;
SELECT 'ABC'::citext ILIKE 'a%'::text AS t;

-- regexp mixed forms
SELECT 'ABC'::citext ~  '^a'::text AS f;  --fail
SELECT 'ABC'::citext ~* '^a'::text AS t;

-- SIMILAR TO mixed negative
SELECT 'ABC'::citext NOT SIMILAR TO 'z%'::text AS t;

SELECT 'ab'::citext  = any ('ab'::citext,'b') AS f;
SELECT 'AB'::citext  = any ('ab'::citext,'b') AS f;
SELECT 'AB'  = any ('ab'::citext,'b') AS f;
SELECT 'AB'::text  = any ('ab'::citext,'b') AS f;
SELECT 'AB'::text  = any ('ab'::citext,'b'::text) AS f;
SELECT 'AB'::citext  = any ('ab'::citext,'b'::text) AS f;

-- Test procedure
CREATE TABLE proc_citext_log (
                                 msg STRING
);
delimiter \\
CREATE PROCEDURE proc_citext_var_basic()
BEGIN
DECLARE
v CITEXT;
    set v = 'TeSt';
INSERT INTO proc_citext_log VALUES (v::STRING);
END \\
delimiter ;

CALL proc_citext_var_basic();
SELECT msg FROM proc_citext_log ORDER BY msg;
drop procedure proc_citext_var_basic;

delimiter \\
CREATE PROCEDURE proc_citext_assign_from_text()
BEGIN
DECLARE
t TEXT;
DECLARE
v CITEXT;
    set t = 'AbC';
    set v = t;
INSERT INTO proc_citext_log VALUES (v::STRING);
END \\
delimiter ;

CALL proc_citext_assign_from_text();
SELECT msg FROM proc_citext_log ORDER BY msg;
drop procedure proc_citext_assign_from_text;

delimiter \\
CREATE PROCEDURE proc_citext_assign_from_text2()
BEGIN
DECLARE
t TEXT;
DECLARE
v CITEXT;
    set v = 'abC';
    set t = v;
INSERT INTO proc_citext_log VALUES (v::STRING);
END \\
delimiter ;

CALL proc_citext_assign_from_text2();
SELECT msg FROM proc_citext_log ORDER BY msg;
drop procedure proc_citext_assign_from_text2;


CREATE TABLE proc_citext_src (
                                 id   INT PRIMARY KEY,
                                 name CITEXT
);

INSERT INTO proc_citext_src VALUES
                                (1, 'One'),
                                (2, 'TWO');

delimiter \\
CREATE PROCEDURE proc_citext_select_into()
BEGIN
DECLARE
v CITEXT;
SELECT name INTO v
FROM proc_citext_src
WHERE id = 2;

INSERT INTO proc_citext_log VALUES (v::STRING);
END \\
delimiter ;

CALL proc_citext_select_into();
SELECT msg FROM proc_citext_log ORDER BY msg;
drop procedure proc_citext_select_into;


delimiter \\
CREATE  PROCEDURE proc_citext_if_compare()
BEGIN
DECLARE
v CITEXT;
    set v = 'AbC';

    IF v = 'abc' THEN
        INSERT INTO proc_citext_log VALUES ('eq_true');
ELSE
        INSERT INTO proc_citext_log VALUES ('eq_false');
    ENDIF;
END \\
delimiter ;
CALL proc_citext_if_compare();
SELECT msg FROM proc_citext_log ORDER BY msg;
drop procedure proc_citext_if_compare;


delimiter \\
CREATE PROCEDURE proc_citext_if_compare_text()
BEGIN
DECLARE
v CITEXT;
DECLARE
t TEXT;
    set v = 'AbC';
    set t = 'abc';

    IF v = t THEN
        INSERT INTO proc_citext_log VALUES ('text_eq_true');
ELSE
        INSERT INTO proc_citext_log VALUES ('text_eq_false');
    ENDIF;
END \\
delimiter ;
CALL proc_citext_if_compare_text();
SELECT msg FROM proc_citext_log ORDER BY msg;
drop procedure proc_citext_if_compare_text;


delimiter \\
CREATE PROCEDURE proc_citext_param(p CITEXT)
BEGIN
    IF p = 'hello' THEN
        INSERT INTO proc_citext_log VALUES ('param_match');
ELSE
        INSERT INTO proc_citext_log VALUES ('param_no_match');
    ENDIF;
END \\
delimiter ;

CALL proc_citext_param('HeLLo');
SELECT msg FROM proc_citext_log ORDER BY msg;
drop procedure proc_citext_param;


CREATE TABLE proc_citext_dml (
                                 id   INT PRIMARY KEY,
                                 name CITEXT
);

delimiter \\
CREATE PROCEDURE proc_citext_insert_update()
BEGIN
DECLARE
v CITEXT;
    set v = 'TeSt';

INSERT INTO proc_citext_dml VALUES (1, v);

UPDATE proc_citext_dml
SET name = 'XyZ'
WHERE name = 'test';
END \\
delimiter ;

CALL proc_citext_insert_update();
SELECT id, name FROM proc_citext_dml ORDER BY id;
drop procedure proc_citext_insert_update;


delimiter \\
CREATE PROCEDURE proc_citext_null()
BEGIN
DECLARE
v CITEXT;
    set v = NULL;

    IF v IS NULL THEN
        INSERT INTO proc_citext_log VALUES ('null_ok');
ELSE
        INSERT INTO proc_citext_log VALUES ('null_bad');
    ENDIF;
END \\
delimiter ;

CALL proc_citext_null();
SELECT msg FROM proc_citext_log ORDER BY msg;
drop procedure proc_citext_null;


delimiter \\
CREATE PROCEDURE proc_citext_string_ops()
BEGIN
DECLARE
v CITEXT;
    set v = 'Alpha-Beta';

    IF split_part(v, '-', 1) = 'Alpha' THEN
        INSERT INTO proc_citext_log VALUES ('split_ok');
    ENDIF;

    IF v LIKE '%beta%' THEN
        INSERT INTO proc_citext_log VALUES ('like_ok');
    ENDIF;
END \\
delimiter ;

CALL proc_citext_string_ops();
SELECT msg FROM proc_citext_log ORDER BY msg;
drop procedure proc_citext_string_ops;

-- Test prepare
DROP TABLE IF EXISTS prep_citext_t;
CREATE TABLE prep_citext_t (
                               id   INT PRIMARY KEY,
                               name CITEXT
);

INSERT INTO prep_citext_t VALUES
                              (1, 'AbC'),
                              (2, 'TeSt'),
                              (3, 'XYZ');

PREPARE prep_eq_citext (CITEXT) AS
SELECT 'ABC'::CITEXT = $1;
EXECUTE prep_eq_citext('abc');


PREPARE prep_eq_text (TEXT) AS
SELECT 'ABC'::CITEXT = $1;
EXECUTE prep_eq_text('abc');

PREPARE prep_eq_infer AS
SELECT 'ABC'::CITEXT = $1;
EXECUTE prep_eq_infer('abc');

PREPARE prep_like_citext (CITEXT) AS
SELECT 'Alpha'::CITEXT LIKE $1;
EXECUTE prep_like_citext('a%');

PREPARE prep_like_text (TEXT) AS
SELECT 'Alpha'::CITEXT LIKE $1;
EXECUTE prep_like_text('a%');

PREPARE prep_ilike_text (TEXT) AS
SELECT 'Alpha'::CITEXT ILIKE $1;
EXECUTE prep_ilike_text('a%');

PREPARE prep_regex_citext (CITEXT) AS
SELECT 'Alpha'::CITEXT ~ $1;
EXECUTE prep_regex_citext('^a');

PREPARE prep_regex_text (TEXT) AS
SELECT 'Alpha'::CITEXT ~ $1;
EXECUTE prep_regex_text('^a');

PREPARE prep_where_citext (CITEXT) AS
SELECT id FROM prep_citext_t WHERE name = $1 ORDER BY id;
EXECUTE prep_where_citext('abc');

PREPARE prep_where_text (TEXT) AS
SELECT id FROM prep_citext_t WHERE name = $1 ORDER BY id;
EXECUTE prep_where_text('abc');

PREPARE prep_insert_text (INT, TEXT) AS
    INSERT INTO prep_citext_t VALUES ($1, $2);
EXECUTE prep_insert_text(10, 'MiXeD');

SELECT name FROM prep_citext_t WHERE id = 10;

PREPARE prep_update_text (TEXT, INT) AS
UPDATE prep_citext_t SET name = $1 WHERE id = $2;
EXECUTE prep_update_text('UpDaTeD', 10);

SELECT name FROM prep_citext_t WHERE id = 10;


PREPARE prep_split_citext (CITEXT) AS
SELECT split_part($1, '-', 1);
EXECUTE prep_split_citext('AbC-DeF');

PREPARE prep_replace_citext (CITEXT) AS
SELECT replace($1, 'abc', 'X');
EXECUTE prep_replace_citext('AbC-AbC');

PREPARE prep_in_citext (CITEXT) AS
SELECT $1 IN (SELECT name FROM prep_citext_t);
EXECUTE prep_in_citext('abc');

PREPARE prep_in_text (TEXT) AS
SELECT $1 IN (SELECT name FROM prep_citext_t);
EXECUTE prep_in_text('abc');

PREPARE prep_eq_null (CITEXT) AS
SELECT 'ABC'::CITEXT = $1;
EXECUTE prep_eq_null(NULL);

PREPARE t1 AS SELECT $1[2] LIKE 'b';
EXECUTE t1(array['a','b']);

PREPARE t2 AS SELECT $1[2] ILIKE 'B';
EXECUTE t2(array['a','B']);

PREPARE t3 AS SELECT $1[2] SIMILAR TO 'b';
EXECUTE t3(array['a','a']);

prepare query_type_delim_plan (int4) as
select e.typdelim from pg_catalog.pg_type t, pg_catalog.pg_type e where t.oid = $1 and t.typelem = e.oid;

DEALLOCATE prep_eq_citext;
DEALLOCATE prep_eq_text;
DEALLOCATE prep_eq_infer;
DEALLOCATE prep_like_citext;
DEALLOCATE prep_like_text;
DEALLOCATE prep_ilike_text;
DEALLOCATE prep_regex_citext;
DEALLOCATE prep_regex_text;
DEALLOCATE prep_where_citext;
DEALLOCATE prep_where_text;
DEALLOCATE prep_insert_text;
DEALLOCATE prep_update_text;
DEALLOCATE prep_split_citext;
DEALLOCATE prep_replace_citext;
DEALLOCATE prep_in_citext;
DEALLOCATE prep_in_text;
DEALLOCATE prep_in_infer;
DEALLOCATE prep_eq_null;
DEALLOCATE t1;
DEALLOCATE t2;
DEALLOCATE t3;
DEALLOCATE query_type_delim_plan;

DROP TABLE IF EXISTS citext_udv_t;
CREATE TABLE citext_udv_t (
                              id   INT PRIMARY KEY,
                              name CITEXT,
                              dept CITEXT
);

INSERT INTO citext_udv_t VALUES
                             (1, 'Alice', 'Sales'),
                             (2, 'ALICE', 'HR'),
                             (3, 'Bob',   'Sales'),
                             (4, 'Charlie', 'Engineering');

-- 1. basic set/show/select
SET @ci_name := 'AbC';
SELECT @ci_name;
SHOW @ci_name;

SET @CI_name := 'aBc';
SELECT @CI_name;
SHOW @CI_name;

SET @"CI_name" := 'ABC';
SELECT @"CI_name";
SHOW @"CI_name";

-- 2. explicit CITEXT cast into UDV
SET @ci_cast := 'AbC'::CITEXT;
SELECT @ci_cast;
SHOW @ci_cast;

SET @ci_cast2 := 'ABC'::CITEXT;
SELECT @ci_cast2 = 'abc'::CITEXT;

-- 3. SELECT INTO from CITEXT column
SET @first_ci := '';
SELECT name FROM citext_udv_t ORDER BY id LIMIT 1;
SELECT name INTO @first_ci FROM citext_udv_t ORDER BY id LIMIT 1;
SELECT @first_ci;
SHOW @first_ci;

SET @first_ci2 := ''::citext;
SELECT name::text FROM citext_udv_t ORDER BY id LIMIT 1;
SELECT name::text INTO @first_ci2 FROM citext_udv_t ORDER BY id LIMIT 1;
SELECT @first_ci2;
SHOW @first_ci2;

SET @first_dept := '';
SELECT dept INTO @first_dept FROM citext_udv_t ORDER BY id LIMIT 1;
SELECT @first_dept;

-- 4. UDV compared with CITEXT literals / columns
SET @ci_key := 'alice';
SELECT id, name FROM citext_udv_t WHERE name = @ci_key ORDER BY id;
SELECT id, name FROM citext_udv_t WHERE name = 'ALICE' ORDER BY id;

SET @ci_dept := 'sales';
SELECT id, name, dept FROM citext_udv_t WHERE dept = @ci_dept ORDER BY id;

-- 5. mixed typed string / CITEXT behavior
SET @plain_txt := 'alice';
SELECT 'Alice'::CITEXT = @plain_txt;
SELECT 'Alice'::CITEXT = @plain_txt::TEXT;
SELECT 'Alice'::CITEXT = @plain_txt::CITEXT;

SET @plain_txt2 := 'sales';
SELECT dept = @plain_txt2 FROM citext_udv_t WHERE id = 1;
SELECT dept = @plain_txt2::TEXT FROM citext_udv_t WHERE id = 1;
SELECT dept = @plain_txt2::CITEXT FROM citext_udv_t WHERE id = 1;

-- 6. LIKE / ILIKE / IN with UDV
SET @ci_pat := 'a%';
SELECT 'Alice'::CITEXT LIKE @ci_pat;
SELECT 'Alice'::CITEXT ILIKE @ci_pat;

SET @ci_in := 'alice';
SELECT @ci_in::CITEXT IN (SELECT name FROM citext_udv_t);
SELECT @ci_in::TEXT   IN (SELECT name FROM citext_udv_t);

-- 7. string functions with UDV carrying CITEXT values
SET @ci_func := 'AbC-DeF'::CITEXT;
SELECT split_part(@ci_func, '-', 1);
SELECT strpos(@ci_func, 'bc');
SELECT replace(@ci_func, 'abc', 'X');
SELECT translate(@ci_func, 'ad', 'xy');

-- 8. PREPARE / EXECUTE with UDV and CITEXT

SET @p_name := 'alice';
PREPARE p_ci_1 AS SELECT id, name FROM citext_udv_t WHERE name = $1 ORDER BY id;
EXECUTE p_ci_1(@p_name::CITEXT);
EXECUTE p_ci_1(@p_name::TEXT);

SET @p_pat := 'a%';
PREPARE p_ci_2 AS SELECT 'Alice'::CITEXT LIKE $1;
EXECUTE p_ci_2(@p_pat::CITEXT);
EXECUTE p_ci_2(@p_pat::TEXT);

SET @p_dept := 'sales';
PREPARE p_ci_3 AS SELECT id, name FROM citext_udv_t WHERE dept = $1 ORDER BY id;
EXECUTE p_ci_3(@p_dept::CITEXT);
EXECUTE p_ci_3(@p_dept::TEXT);

DEALLOCATE PREPARE p_ci_1;
DEALLOCATE PREPARE p_ci_2;
DEALLOCATE PREPARE p_ci_3;

-- 9. DML with UDV carrying CITEXT
SET @ins_name := 'DaVid'::CITEXT;
SET @ins_dept := 'Sales'::CITEXT;
INSERT INTO citext_udv_t VALUES (10, @ins_name, @ins_dept);
SELECT * FROM citext_udv_t WHERE id = 10;

SET @upd_name := 'david';
UPDATE citext_udv_t SET dept = 'HR' WHERE name = @upd_name::CITEXT;
SELECT * FROM citext_udv_t WHERE id = 10;

SET @del_name := 'DAVID';
DELETE FROM citext_udv_t WHERE name = @del_name::CITEXT;
SELECT * FROM citext_udv_t WHERE id = 10;

-- 10. NULL
SET @ci_null = NULL;
SELECT @ci_null;
SHOW @ci_null;
SELECT 'abc'::CITEXT = @ci_null;
SELECT @ci_null IS NULL;

DROP TABLE citext_udv_t;

-- INSERT INTO ... SELECT / CASE WHEN with CITEXT

CREATE TABLE citext_ins_src (
     id   INT PRIMARY KEY,
     name CITEXT,
     dept CITEXT
);

INSERT INTO citext_ins_src VALUES
     (1, 'Alice',   'Sales'),
     (2, 'ALICE',   'HR'),
     (3, 'Bob',     'Sales'),
     (4, 'Charlie', 'Engineering');

CREATE TABLE citext_ins_dst (
     id        INT PRIMARY KEY,
     name      CITEXT,
     dept      CITEXT,
     tag_name  STRING,
     dept_flag STRING
);

-- 1. INSERT INTO ... SELECT with CITEXT filter
INSERT INTO citext_ins_dst
SELECT id, name, dept, 'plain_insert_select', 'copied'
FROM citext_ins_src
WHERE name = 'alice'::CITEXT
ORDER BY id;

SELECT * FROM citext_ins_dst ORDER BY id;

-- 2. INSERT INTO ... SELECT: typed plain string should win
INSERT INTO citext_ins_dst
SELECT id, name, dept, 'text_insert_select', 'copied'
FROM citext_ins_src
WHERE name = 'alice'::TEXT
ORDER BY id;

SELECT * FROM citext_ins_dst ORDER BY id;

-- 3. INSERT INTO ... SELECT with CASE WHEN on CITEXT columns
INSERT INTO citext_ins_dst
SELECT
    id,
    name,
    dept,
    CASE
        WHEN name = 'alice'::CITEXT THEN 'match_ci'
        ELSE 'no_match_ci'
        END,
    CASE
        WHEN dept = 'sales'::CITEXT THEN 'sales_ci'
        ELSE 'other_ci'
        END
FROM citext_ins_src
ORDER BY id;

SELECT * FROM citext_ins_dst ORDER BY id;

-- 4. CASE WHEN with CITEXT literal comparison
SELECT
    id,
    name,
    CASE
        WHEN name = 'alice'::CITEXT THEN 'match_ci'
        ELSE 'no_match_ci'
        END AS case_result_ci
FROM citext_ins_src
ORDER BY id;

-- 5. CASE WHEN with typed plain string comparison
SELECT
    id,
    name,
    CASE
        WHEN name = 'alice'::TEXT THEN 'match_text'
        ELSE 'no_match_text'
        END AS case_result_text
FROM citext_ins_src
ORDER BY id;

-- 6. CASE WHEN with CITEXT in multiple branches
SELECT
    id,
    CASE
        WHEN name = 'alice'::CITEXT THEN 'name_ci'
        WHEN dept = 'sales'::CITEXT THEN 'dept_ci'
        ELSE 'other_ci'
        END AS multi_branch_case
FROM citext_ins_src
ORDER BY id;

-- 7. CASE WHEN returning CITEXT-compatible values
SELECT
    id,
    CASE
        WHEN dept = 'sales'::CITEXT THEN name
        ELSE 'fallback'::CITEXT
        END AS case_name_ci
FROM citext_ins_src
ORDER BY id;

DROP TABLE citext_ins_dst;
DROP TABLE citext_ins_src;

-- CITEXT[] array comparison tests
-- Same CITEXT[] comparison should be case-insensitive.
SELECT ARRAY['A', 'B']::CITEXT[] = ARRAY['a', 'b']::CITEXT[] AS t;
SELECT ARRAY['A', 'B']::CITEXT[] <> ARRAY['a', 'b']::CITEXT[] AS f;
SELECT ARRAY['A', 'B']::CITEXT[] = ARRAY['a', 'c']::CITEXT[] AS f;

-- Bare string ARRAY literal should follow CITEXT[].
SELECT ARRAY['A', 'B']::CITEXT[] = ARRAY['a', 'b'] AS t;
SELECT ARRAY['A', 'B'] = ARRAY['a', 'b']::CITEXT[] AS t;

-- Explicit plain string array should win.
-- These should be case-sensitive.
SELECT ARRAY['A', 'B']::CITEXT[] = ARRAY['a', 'b']::TEXT[] AS f;
SELECT ARRAY['A', 'B']::TEXT[] = ARRAY['a', 'b']::CITEXT[] AS f;

SELECT ARRAY['A', 'B']::CITEXT[] = ARRAY['a', 'b']::VARCHAR[] AS f;
SELECT ARRAY['A', 'B']::VARCHAR[] = ARRAY['a', 'b']::CITEXT[] AS f;

SELECT ARRAY['A', 'B']::CITEXT[] = ARRAY['A', 'B']::TEXT[] AS t;
SELECT ARRAY['A', 'B']::TEXT[] = ARRAY['A', 'B']::CITEXT[] AS t;

-- NULL array comparison.
SELECT NULL::CITEXT[] IS DISTINCT FROM NULL::CITEXT[] AS f;
SELECT NULL::CITEXT[] IS NOT DISTINCT FROM NULL::CITEXT[] AS t;

SELECT ARRAY['A', NULL]::CITEXT[] IS NOT DISTINCT FROM ARRAY['a', NULL]::CITEXT[] AS t;
SELECT ARRAY['A', NULL]::TEXT[] IS DISTINCT FROM ARRAY['a', NULL]::CITEXT[] AS t;

-- Array length/order should still matter.
SELECT ARRAY['A', 'B']::CITEXT[] = ARRAY['a']::CITEXT[] AS f;
SELECT ARRAY['A', 'B']::CITEXT[] = ARRAY['b', 'a']::CITEXT[] AS f;

-- CITEXT[] table column tests

CREATE TABLE citext_arr_t (
                              id   INT PRIMARY KEY,
                              name CITEXT[]
);

INSERT INTO citext_arr_t VALUES
    (1, ARRAY['Alpha', 'Beta']::CITEXT[]),
    (2, ARRAY['ALPHA', 'gamma']::CITEXT[]),
    (3, ARRAY['Delta']::CITEXT[]),
    (4, NULL);

SELECT id, name FROM citext_arr_t ORDER BY id;

-- Same CITEXT[] semantics.
SELECT id FROM citext_arr_t
WHERE name = ARRAY['alpha', 'beta']::CITEXT[]
ORDER BY id;

-- Bare string ARRAY literal should follow CITEXT[] column type.
SELECT id FROM citext_arr_t
WHERE name = ARRAY['alpha', 'beta']
ORDER BY id;

-- Explicit TEXT[] should win, so comparison is case-sensitive.
SELECT id FROM citext_arr_t
WHERE name = ARRAY['alpha', 'beta']::TEXT[]
ORDER BY id;

SELECT id FROM citext_arr_t
WHERE name = ARRAY['Alpha', 'Beta']::TEXT[]
ORDER BY id;

-- IS DISTINCT FROM / IS NOT DISTINCT FROM with CITEXT[] column.
SELECT id FROM citext_arr_t
WHERE name IS NOT DISTINCT FROM ARRAY['alpha', 'beta']::CITEXT[]
ORDER BY id;

SELECT id FROM citext_arr_t
WHERE name IS DISTINCT FROM ARRAY['alpha', 'beta']::CITEXT[]
ORDER BY id;

-- ANY / SOME / ALL with CITEXT and CITEXT[]
-- CITEXT left + CITEXT[] right: case-insensitive.
SELECT 'ALPHA'::CITEXT = ANY (ARRAY['alpha', 'beta']::CITEXT[]) AS t;
SELECT 'ALPHA'::CITEXT = SOME (ARRAY['zzz', 'alpha']::CITEXT[]) AS t;
SELECT 'ALPHA'::CITEXT = ALL (ARRAY['alpha', 'ALPHA']::CITEXT[]) AS t;
SELECT 'ALPHA'::CITEXT = ALL (ARRAY['alpha', 'beta']::CITEXT[]) AS f;

-- Bare string left should follow CITEXT[] right.
SELECT 'ALPHA' = ANY (ARRAY['alpha', 'beta']::CITEXT[]) AS t;
SELECT 'ALPHA' = SOME (ARRAY['zzz', 'alpha']::CITEXT[]) AS t;
SELECT 'ALPHA' = ALL (ARRAY['alpha', 'ALPHA']::CITEXT[]) AS t;

-- Explicit TEXT left should win, so comparison is case-sensitive.
SELECT 'ALPHA'::TEXT = ANY (ARRAY['alpha', 'beta']::CITEXT[]) AS f;
SELECT 'ALPHA'::TEXT = ANY (ARRAY['ALPHA', 'beta']::CITEXT[]) AS t;
SELECT 'ALPHA'::TEXT = ALL (ARRAY['alpha', 'ALPHA']::CITEXT[]) AS f;

-- CITEXT left + bare ARRAY literal: bare array should follow CITEXT.
SELECT 'ALPHA'::CITEXT = ANY (ARRAY['alpha', 'beta']) AS t;
SELECT 'ALPHA'::CITEXT = SOME (ARRAY['zzz', 'alpha']) AS t;
SELECT 'ALPHA'::CITEXT = ALL (ARRAY['alpha', 'ALPHA']) AS t;

-- CITEXT left + explicit TEXT[] right: TEXT[] wins, case-sensitive.
SELECT 'ALPHA'::CITEXT = ANY (ARRAY['alpha', 'beta']::TEXT[]) AS f;
SELECT 'ALPHA'::CITEXT = ANY (ARRAY['ALPHA', 'beta']::TEXT[]) AS t;
SELECT 'ALPHA'::CITEXT = ALL (ARRAY['alpha', 'ALPHA']::TEXT[]) AS f;

-- ANY/SOME/ALL against CITEXT[] table column.
SELECT id FROM citext_arr_t
WHERE 'alpha'::CITEXT = ANY (name)
ORDER BY id;

SELECT id FROM citext_arr_t
WHERE 'ALPHA' = ANY (name)
ORDER BY id;

SELECT id FROM citext_arr_t
WHERE 'ALPHA'::TEXT = ANY (name)
ORDER BY id;

SELECT id FROM citext_arr_t
WHERE 'alpha'::CITEXT = SOME (name)
ORDER BY id;

SELECT id FROM citext_arr_t
WHERE 'alpha'::CITEXT = ALL (name)
ORDER BY id;


-- ANY tuple-style RHS with CITEXT
SELECT 'AB'::CITEXT = ANY ('ab'::CITEXT, 'b') AS t;
SELECT 'AB'::CITEXT = ANY ('zz'::CITEXT, 'b') AS f;

-- Bare string left should follow CITEXT tuple element.
SELECT 'AB' = ANY ('ab'::CITEXT, 'b') AS t;

-- Explicit TEXT left should win.
SELECT 'AB'::TEXT = ANY ('ab'::CITEXT, 'b') AS f;
SELECT 'AB'::TEXT = ANY ('AB'::CITEXT, 'b') AS t;

-- CITEXT left + mixed typed tuple.
-- Explicit TEXT element should make typed string path visible.
SELECT 'AB'::CITEXT = ANY ('ab'::CITEXT, 'b'::TEXT) AS f;
SELECT 'AB'::CITEXT = ANY ('ab'::TEXT, 'b'::CITEXT) AS f;
SELECT 'AB'::CITEXT = ANY ('AB'::CITEXT, 'b'::TEXT) AS t;


-- CITEXT[] array containment: @> and <@
-- CITEXT[] contains should be case-insensitive when both sides are CITEXT[].
SELECT ARRAY['Alpha', 'Beta']::CITEXT[] @> ARRAY['alpha']::CITEXT[] AS t;
SELECT ARRAY['Alpha', 'Beta']::CITEXT[] @> ARRAY['BETA']::CITEXT[] AS t;
SELECT ARRAY['Alpha', 'Beta']::CITEXT[] @> ARRAY['gamma']::CITEXT[] AS f;

SELECT ARRAY['alpha']::CITEXT[] <@ ARRAY['Alpha', 'Beta']::CITEXT[] AS t;
SELECT ARRAY['GAMMA']::CITEXT[] <@ ARRAY['Alpha', 'Beta']::CITEXT[] AS f;

-- Bare string ARRAY literal should follow CITEXT[].
SELECT ARRAY['Alpha', 'Beta']::CITEXT[] @> ARRAY['alpha'] AS t;
SELECT ARRAY['alpha'] <@ ARRAY['Alpha', 'Beta']::CITEXT[] AS t;

-- Explicit TEXT[] should win if your type-check fallback supports @> mixed forms.
-- If @> only supports same array type in current code, these can be expected failures.
SELECT ARRAY['Alpha', 'Beta']::CITEXT[] @> ARRAY['alpha']::TEXT[] AS f;
SELECT ARRAY['Alpha', 'Beta']::TEXT[] @> ARRAY['alpha']::CITEXT[] AS f;

SELECT ARRAY['Alpha', 'Beta']::CITEXT[] @> ARRAY['Alpha']::TEXT[] AS t;
SELECT ARRAY['Alpha', 'Beta']::TEXT[] @> ARRAY['Alpha']::CITEXT[] AS t;

-- Table column @> tests.
SELECT id FROM citext_arr_t
WHERE name @> ARRAY['alpha']::CITEXT[]
ORDER BY id;

SELECT id FROM citext_arr_t
WHERE name @> ARRAY['ALPHA']::CITEXT[]
ORDER BY id;

-- Bare ARRAY literal follows name::CITEXT[].
SELECT id FROM citext_arr_t
WHERE name @> ARRAY['alpha']
ORDER BY id;

-- Explicit TEXT[] wins, so comparison should be case-sensitive if mixed @> is supported.
SELECT id FROM citext_arr_t
WHERE name @> ARRAY['alpha']::TEXT[]
ORDER BY id;

SELECT id FROM citext_arr_t
WHERE name @> ARRAY['Alpha']::TEXT[]
ORDER BY id;

-- Reverse containment.
SELECT id FROM citext_arr_t
WHERE ARRAY['alpha']::CITEXT[] <@ name
ORDER BY id;

SELECT id FROM citext_arr_t
WHERE ARRAY['ALPHA'] <@ name
ORDER BY id;

SELECT id FROM citext_arr_t
WHERE ARRAY['alpha']::TEXT[] <@ name
ORDER BY id;

-- UNION / INTERSECT / EXCEPT with CITEXT
CREATE TABLE citext_setop_unicode (
                                      key_col CITEXT
);

INSERT INTO citext_setop_unicode VALUES
                                     ('①混合Alpha中文'),
                                     ('②Greek中文数据'),
                                     ('②greek中文DATA'),
                                     ('③日本語データ'),
                                     ('③日本語データー');

-- UNION: bare string literal should be coerced to CITEXT.
-- '①混合Alpha中文' and '①混合alpha中文' are equal under CITEXT semantics.
SELECT COUNT(*) = 5::bigint AS t
FROM (
         SELECT key_col FROM citext_setop_unicode
         UNION
         SELECT '①混合alpha中文'
     ) u;

-- UNION: explicit TEXT should win.
-- The CITEXT column should be cast to TEXT, so Alpha and alpha are different.
SELECT COUNT(*) = 6::bigint AS t
FROM (
         SELECT key_col FROM citext_setop_unicode
         UNION
         SELECT '①混合alpha中文'::TEXT
     ) u;

-- UNION: explicit CITEXT should keep CITEXT semantics.
SELECT COUNT(*) = 5::bigint AS t
FROM (
         SELECT key_col FROM citext_setop_unicode
         UNION
         SELECT '①混合alpha中文'::CITEXT
     ) u;

-- INTERSECT: bare string literal follows CITEXT semantics.
SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT key_col FROM citext_setop_unicode
         INTERSECT
         SELECT '①混合alpha中文'
     ) u;

-- INTERSECT: explicit TEXT wins, so Alpha and alpha are different.
SELECT COUNT(*) = 0::bigint AS t
FROM (
         SELECT key_col FROM citext_setop_unicode
         INTERSECT
         SELECT '①混合alpha中文'::TEXT
     ) u;

-- INTERSECT: explicit TEXT with exact case should match.
SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT key_col FROM citext_setop_unicode
         INTERSECT
         SELECT '①混合Alpha中文'::TEXT
     ) u;

-- EXCEPT: bare string literal follows CITEXT semantics and removes the row.
SELECT COUNT(*) = 4::bigint AS t
FROM (
         SELECT key_col FROM citext_setop_unicode
         EXCEPT
         SELECT '①混合alpha中文'
     ) u;

-- EXCEPT: explicit TEXT wins, so Alpha is not removed by alpha.
SELECT COUNT(*) = 5::bigint AS t
FROM (
         SELECT key_col FROM citext_setop_unicode
         EXCEPT
         SELECT '①混合alpha中文'::TEXT
     ) u;

-- EXCEPT: explicit TEXT with exact case removes the row.
SELECT COUNT(*) = 4::bigint AS t
FROM (
         SELECT key_col FROM citext_setop_unicode
         EXCEPT
         SELECT '①混合Alpha中文'::TEXT
     ) u;


-- Basic scalar set-op tables.
CREATE TABLE citext_setop_ci (
                                 name CITEXT
);

CREATE TABLE citext_setop_ci2 (
                                  name CITEXT
);

CREATE TABLE citext_setop_text (
                                   name TEXT
);

CREATE TABLE citext_setop_varchar (
                                      name VARCHAR
);

INSERT INTO citext_setop_ci VALUES
                                ('Alpha'),
                                ('BETA'),
                                ('gamma');

INSERT INTO citext_setop_ci2 VALUES
                                 ('alpha'),
                                 ('beta'),
                                 ('delta');

INSERT INTO citext_setop_text VALUES
                                  ('alpha'),
                                  ('BETA'),
                                  ('delta');

INSERT INTO citext_setop_varchar VALUES
                                     ('alpha'),
                                     ('BETA'),
                                     ('epsilon');


-- CITEXT + CITEXT should use CITEXT semantics.
SELECT COUNT(*) = 4::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         UNION
         SELECT name FROM citext_setop_ci2
     ) u;

SELECT COUNT(*) = 2::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         INTERSECT
         SELECT name FROM citext_setop_ci2
     ) u;

SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         EXCEPT
         SELECT name FROM citext_setop_ci2
     ) u;


-- CITEXT + bare string literal should use CITEXT semantics.
SELECT COUNT(*) = 3::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         UNION
         SELECT 'alpha'
     ) u;

SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         INTERSECT
         SELECT 'alpha'
     ) u;

SELECT COUNT(*) = 2::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         EXCEPT
         SELECT 'alpha'
     ) u;


-- Reverse order: bare string literal + CITEXT should also use CITEXT semantics.
SELECT COUNT(*) = 3::bigint AS t
FROM (
         SELECT 'alpha'
         UNION
         SELECT name FROM citext_setop_ci
     ) u;

SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT 'alpha'
         INTERSECT
         SELECT name FROM citext_setop_ci
     ) u;

SELECT COUNT(*) = 0::bigint AS t
FROM (
         SELECT 'alpha'
         EXCEPT
         SELECT name FROM citext_setop_ci
     ) u;


-- CITEXT + explicit TEXT constant should use TEXT semantics.
SELECT COUNT(*) = 4::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         UNION
         SELECT 'alpha'::TEXT
     ) u;

SELECT COUNT(*) = 0::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         INTERSECT
         SELECT 'alpha'::TEXT
     ) u;

SELECT COUNT(*) = 3::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         EXCEPT
         SELECT 'alpha'::TEXT
     ) u;

SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         INTERSECT
         SELECT 'Alpha'::TEXT
     ) u;

SELECT COUNT(*) = 2::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         EXCEPT
         SELECT 'Alpha'::TEXT
     ) u;


-- Reverse order: explicit TEXT constant should still win.
SELECT COUNT(*) = 4::bigint AS t
FROM (
         SELECT 'alpha'::TEXT
         UNION
         SELECT name FROM citext_setop_ci
     ) u;

SELECT COUNT(*) = 0::bigint AS t
FROM (
         SELECT 'alpha'::TEXT
         INTERSECT
         SELECT name FROM citext_setop_ci
     ) u;

SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT 'alpha'::TEXT
         EXCEPT
         SELECT name FROM citext_setop_ci
     ) u;


-- CITEXT + explicit VARCHAR constant should use VARCHAR semantics.
SELECT COUNT(*) = 4::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         UNION
         SELECT 'alpha'::VARCHAR
     ) u;

SELECT COUNT(*) = 0::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         INTERSECT
         SELECT 'alpha'::VARCHAR
     ) u;

SELECT COUNT(*) = 3::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         EXCEPT
         SELECT 'alpha'::VARCHAR
     ) u;


-- CITEXT column + TEXT column should use TEXT semantics.
SELECT COUNT(*) = 5::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         UNION
         SELECT name FROM citext_setop_text
     ) u;

SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         INTERSECT
         SELECT name FROM citext_setop_text
     ) u;

SELECT COUNT(*) = 2::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         EXCEPT
         SELECT name FROM citext_setop_text
     ) u;

SELECT COUNT(*) = 2::bigint AS t
FROM (
         SELECT name FROM citext_setop_text
         EXCEPT
         SELECT name FROM citext_setop_ci
     ) u;


-- CITEXT column + VARCHAR column should use VARCHAR semantics.
SELECT COUNT(*) = 5::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         UNION
         SELECT name FROM citext_setop_varchar
     ) u;

SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         INTERSECT
         SELECT name FROM citext_setop_varchar
     ) u;

SELECT COUNT(*) = 2::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         EXCEPT
         SELECT name FROM citext_setop_varchar
     ) u;


-- UNION ALL / INTERSECT ALL / EXCEPT ALL smoke tests.
-- These mainly ensure mixed CITEXT/string type propagation does not fail.
SELECT COUNT(*) = 4::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         UNION ALL
         SELECT 'alpha'::TEXT
     ) u;

SELECT COUNT(*) = 0::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         INTERSECT ALL
         SELECT 'alpha'::TEXT
     ) u;

SELECT COUNT(*) = 3::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         EXCEPT ALL
         SELECT 'alpha'::TEXT
     ) u;


-- Explicit CITEXT constant should keep CITEXT semantics.
SELECT COUNT(*) = 3::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         UNION
         SELECT 'alpha'::CITEXT
     ) u;

SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         INTERSECT
         SELECT 'alpha'::CITEXT
     ) u;

SELECT COUNT(*) = 2::bigint AS t
FROM (
         SELECT name FROM citext_setop_ci
         EXCEPT
         SELECT 'alpha'::CITEXT
     ) u;


-- Constant-only set-op tests.
-- CITEXT + TEXT should use TEXT semantics.
SELECT COUNT(*) = 2::bigint AS t
FROM (
         SELECT 'Alpha'::CITEXT
         UNION
         SELECT 'alpha'::TEXT
     ) u;

SELECT COUNT(*) = 0::bigint AS t
FROM (
         SELECT 'Alpha'::CITEXT
         INTERSECT
         SELECT 'alpha'::TEXT
     ) u;

SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT 'Alpha'::CITEXT
         EXCEPT
         SELECT 'alpha'::TEXT
     ) u;

-- CITEXT + bare string should use CITEXT semantics.
SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT 'Alpha'::CITEXT
         UNION
         SELECT 'alpha'
     ) u;

SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT 'Alpha'::CITEXT
         INTERSECT
         SELECT 'alpha'
     ) u;

SELECT COUNT(*) = 0::bigint AS t
FROM (
         SELECT 'Alpha'::CITEXT
         EXCEPT
         SELECT 'alpha'
     ) u;


-- Multi-column set-op.
-- This catches the case where one column needs left->right propagation while
-- another column needs right->left propagation.
CREATE TABLE citext_setop_mixed_l (
                                      a CITEXT,
                                      b TEXT
);

CREATE TABLE citext_setop_mixed_r (
                                      a TEXT,
                                      b CITEXT
);

INSERT INTO citext_setop_mixed_l VALUES ('Alpha', 'BRAVO');
INSERT INTO citext_setop_mixed_r VALUES ('alpha', 'bravo');

-- Both columns should follow TEXT semantics in their own positions.
-- Therefore ('Alpha', 'BRAVO') and ('alpha', 'bravo') are different rows.
SELECT COUNT(*) = 2::bigint AS t
FROM (
         SELECT a, b FROM citext_setop_mixed_l
         UNION
         SELECT a, b FROM citext_setop_mixed_r
     ) u;

SELECT COUNT(*) = 2::bigint AS t
FROM (
         SELECT a, b FROM citext_setop_mixed_l
         UNION ALL
         SELECT a, b FROM citext_setop_mixed_r
     ) u;

SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT a, b::citext FROM citext_setop_mixed_l
         UNION
         SELECT a::citext, b FROM citext_setop_mixed_r
     ) u;

SELECT COUNT(*) = 2::bigint AS t
FROM (
         SELECT a, b::citext FROM citext_setop_mixed_l
         UNION all
         SELECT a::citext, b FROM citext_setop_mixed_r
     ) u;

SELECT COUNT(*) = 0::bigint AS t
FROM (
         SELECT a, b FROM citext_setop_mixed_l
         INTERSECT
         SELECT a, b FROM citext_setop_mixed_r
     ) u;

SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT a, b FROM citext_setop_mixed_l
         EXCEPT
         SELECT a, b FROM citext_setop_mixed_r
     ) u;

SELECT COUNT(*) = 1::bigint AS t
FROM (
         SELECT a, b FROM citext_setop_mixed_r
         EXCEPT
         SELECT a, b FROM citext_setop_mixed_l
     ) u;

-- ============================================================
-- CITEXT[] array concatenation tests
-- ============================================================

-- Same-type CITEXT[] || CITEXT[].
SELECT (ARRAY['a','b']::CITEXT[] || ARRAY['B']::CITEXT[])
           = ARRAY['A','B','b']::CITEXT[] AS t;

SELECT (ARRAY['a','b']::CITEXT[] || ARRAY['c']::CITEXT[])
           = ARRAY['a','b','c']::CITEXT[] AS t;

-- Bare ARRAY literal should follow CITEXT[].
SELECT (ARRAY['a','b']::CITEXT[] || ARRAY['B'])
           = ARRAY['A','B','b']::TEXT[] AS t;

SELECT (ARRAY['A'] || ARRAY['b','c']::CITEXT[])
           = ARRAY['a','B','C']::TEXT[] AS t;

-- Explicit TEXT[] wins over CITEXT[].
-- Result should behave as ordinary TEXT[] and compare case-sensitively.
SELECT (ARRAY['a','b']::CITEXT[] || ARRAY['B']::TEXT[])
           = ARRAY['a','b','B']::TEXT[] AS t;

SELECT (ARRAY['a','b']::CITEXT[] || ARRAY['B']::TEXT[])
           = ARRAY['a','b','b']::TEXT[] AS f;

SELECT (ARRAY['a','b']::TEXT[] || ARRAY['B']::CITEXT[])
           = ARRAY['a','b','B']::TEXT[] AS t;

SELECT (ARRAY['a','b']::TEXT[] || ARRAY['B']::CITEXT[])
           = ARRAY['a','b','b']::TEXT[] AS f;

-- Explicit VARCHAR[] also wins over CITEXT[].
SELECT (ARRAY['a','b']::CITEXT[] || ARRAY['B']::VARCHAR[])
           = ARRAY['a','b','B']::STRING[] AS t;

SELECT (ARRAY['a','b']::VARCHAR[] || ARRAY['B']::CITEXT[])
           = ARRAY['a','b','B']::STRING[] AS t;

-- CITEXT[] || bare string scalar.
SELECT (ARRAY['a','b']::CITEXT[] || 'B')
           = ARRAY['A','B','b']::TEXT[] AS t;

-- bare string scalar || CITEXT[].
SELECT ('A' || ARRAY['b','c']::CITEXT[])
           = ARRAY['a','B','C']::TEXT[] AS t;

-- Explicit TEXT scalar wins over CITEXT[].
SELECT (ARRAY['a','b']::CITEXT[] || 'B'::TEXT)
           = ARRAY['a','b','B']::TEXT[] AS t;

SELECT (ARRAY['a','b']::CITEXT[] || 'B'::TEXT)
           = ARRAY['a','b','b']::TEXT[] AS f;

SELECT ('A'::TEXT || ARRAY['b','c']::CITEXT[])
           = ARRAY['A','b','c']::TEXT[] AS t;

SELECT ('A'::TEXT || ARRAY['b','c']::CITEXT[])
           = ARRAY['a','b','c']::TEXT[] AS f;

-- Explicit VARCHAR scalar wins over CITEXT[].
SELECT (ARRAY['a','b']::CITEXT[] || 'B'::VARCHAR)
           = ARRAY['a','b','B']::STRING[] AS t;

SELECT ('A'::VARCHAR || ARRAY['b','c']::CITEXT[])
           = ARRAY['A','b','c']::STRING[] AS t;

-- NULL element handling.
SELECT (ARRAY['a', NULL]::CITEXT[] || 'B')
           = ARRAY['A', NULL, 'b']::TEXT[] AS t;

SELECT (ARRAY['a', NULL]::CITEXT[] || 'B'::TEXT)
           = ARRAY['a', NULL, 'B']::TEXT[] AS t;

-- NULL array handling should preserve normal array concat semantics.
SELECT (NULL::CITEXT[] || 'B'::CITEXT) IS NULL AS t;
SELECT ('B'::CITEXT || NULL::CITEXT[]) IS NULL AS t;
SELECT (NULL::CITEXT[] || ARRAY['B']::CITEXT[]) IS NULL AS t;

-- Table-column concat tests.
CREATE TABLE citext_arr_concat_t (
                                     id   INT PRIMARY KEY,
                                     name CITEXT[]
);

INSERT INTO citext_arr_concat_t VALUES
                                    (1, ARRAY['Alpha', 'Beta']::CITEXT[]),
                                    (2, ARRAY['ALPHA', 'gamma']::CITEXT[]),
                                    (3, NULL);

-- CITEXT[] column || bare scalar.
SELECT id
FROM citext_arr_concat_t
WHERE name || 'X' = ARRAY['alpha', 'beta', 'x']::TEXT[]
ORDER BY id;

-- bare scalar || CITEXT[] column.
SELECT id
FROM citext_arr_concat_t
WHERE 'X' || name = ARRAY['x', 'alpha', 'beta']::TEXT[]
ORDER BY id;

-- CITEXT[] column || explicit TEXT scalar: TEXT wins, case-sensitive.
SELECT id
FROM citext_arr_concat_t
WHERE name || 'X'::TEXT = ARRAY['Alpha', 'Beta', 'X']::TEXT[]
ORDER BY id;

SELECT id
FROM citext_arr_concat_t
WHERE name || 'X'::TEXT = ARRAY['alpha', 'beta', 'X']::TEXT[]
ORDER BY id;

-- CITEXT[] column || bare ARRAY literal.
SELECT id
FROM citext_arr_concat_t
WHERE name || ARRAY['X'] = ARRAY['alpha', 'beta', 'x']::TEXT[]
ORDER BY id;

-- CITEXT[] column || explicit TEXT[]: TEXT[] wins, case-sensitive.
SELECT id
FROM citext_arr_concat_t
WHERE name || ARRAY['X']::TEXT[] = ARRAY['Alpha', 'Beta', 'X']::TEXT[]
ORDER BY id;

SELECT id
FROM citext_arr_concat_t
WHERE name || ARRAY['X']::TEXT[] = ARRAY['alpha', 'beta', 'X']::TEXT[]
ORDER BY id;

DROP TABLE citext_arr_concat_t;

-- scalar CITEXT, not to CITEXT[].
SELECT ARRAY['a','b']::CITEXT || ARRAY['b'];
SELECT ARRAY['a','b']::CITEXT || ARRAY['b']::text[];
SELECT ARRAY['b']::string[] || ARRAY['a','b']::CITEXT;
SELECT ARRAY['b'] || ARRAY['a','b']::CITEXT;

-- Correct spelling is:
SELECT ARRAY['a','b']::CITEXT[] || ARRAY['b']::CITEXT[];



-- Timeseries engine: CITEXT is not supported
CREATE TS DATABASE ts_citext_regression;
USE ts_citext_regression;
CREATE TABLE ts_citext_col (
                               k_timestamp TIMESTAMP NOT NULL,
                               c1 CITEXT
) TAGS (
    size INT NOT NULL
) PRIMARY TAGS (size);

CREATE TABLE ts_citext_tag (
                               k_timestamp TIMESTAMP NOT NULL,
                               c1 VARCHAR(10)
) TAGS (
    tag_name CITEXT NOT NULL
) PRIMARY TAGS (tag_name);

CREATE TABLE ts_citext_multi (
                                 k_timestamp TIMESTAMP NOT NULL,
                                 c1 CITEXT,
                                 c2 CITEXT
) TAGS (
    size INT NOT NULL
) PRIMARY TAGS (size);

CREATE TABLE ts_alter_base (
                               k_timestamp TIMESTAMP NOT NULL,
                               c1 VARCHAR(10)
) TAGS (
    size INT NOT NULL
) PRIMARY TAGS (size);

ALTER TABLE ts_alter_base ADD COLUMN c2 CITEXT;

ALTER TABLE ts_alter_base ALTER COLUMN c1 TYPE CITEXT;

drop database ts_citext_regression;
drop database citext_regression cascade;