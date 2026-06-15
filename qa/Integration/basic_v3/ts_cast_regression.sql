SET TIME ZONE 'UTC';
DROP DATABASE IF EXISTS ts_cast_regression CASCADE;
CREATE TS DATABASE ts_cast_regression;
CREATE TABLE ts_cast_regression.t (
  k_timestamp TIMESTAMP NOT NULL,
  f FLOAT8,
  g FLOAT8,
  s VARCHAR(40)
) TAGS (
  id INT8 NOT NULL
) PRIMARY TAGS(id);

INSERT INTO ts_cast_regression.t VALUES
  ('2026-01-01 00:00:00', 1.9, -1.9, 'True', 1),
  ('2026-01-01 00:01:00', 0.5, -0.5, 'FALSE', 2),
  ('2026-01-01 00:02:00', 1.0, 0.0, 't', 3),
  ('2026-01-01 00:03:00', 1e20, 0.0, '2026-01-01 07:00:00', 4);

SELECT
  id,
  CAST(f AS INT8) AS f_int,
  CAST(g AS INT8) AS g_int
FROM ts_cast_regression.t
WHERE id = 1;

SELECT CAST(f AS INT8) AS overflow_int
FROM ts_cast_regression.t
WHERE id = 4;

SELECT
  id,
  CAST(f AS BOOL) AS f_bool,
  CAST(g AS BOOL) AS g_bool
FROM ts_cast_regression.t
WHERE id IN (2, 3)
ORDER BY id;

SELECT
  id,
  s,
  CAST(s AS BOOL) AS s_bool
FROM ts_cast_regression.t
WHERE id IN (1, 2, 3)
ORDER BY id;

SELECT
  CAST(s AS TIMESTAMPTZ) AS s_timestamptz,
  CAST(s AS TIMESTAMP) AS s_timestamp
FROM ts_cast_regression.t
WHERE id = 4;

DROP DATABASE ts_cast_regression CASCADE;
