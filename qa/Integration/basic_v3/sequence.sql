-- ALTER SEQUENCE: verify START not reset when altering non-direction options
CREATE SEQUENCE s_start5 START 5;
SHOW CREATE SEQUENCE s_start5;
ALTER SEQUENCE s_start5 MAXVALUE 100;
SHOW CREATE SEQUENCE s_start5;
SELECT nextval('s_start5') AS next_after_alter_max;
DROP SEQUENCE s_start5;

-- ALTER SEQUENCE: change INCREMENT positive → negative, verify defaults recalculated
CREATE SEQUENCE s_asc_to_desc;
SHOW CREATE SEQUENCE s_asc_to_desc;
ALTER SEQUENCE s_asc_to_desc INCREMENT -1;
SHOW CREATE SEQUENCE s_asc_to_desc;
SELECT nextval('s_asc_to_desc') AS next_after_alter_neg_inc;
DROP SEQUENCE s_asc_to_desc;

-- ALTER SEQUENCE: change INCREMENT negative → positive, verify defaults recalculated
CREATE SEQUENCE s_desc_to_asc INCREMENT -1;
SHOW CREATE SEQUENCE s_desc_to_asc;
ALTER SEQUENCE s_desc_to_asc INCREMENT 1;
SHOW CREATE SEQUENCE s_desc_to_asc;
SELECT nextval('s_desc_to_asc') AS next_after_alter_pos_inc;
DROP SEQUENCE s_desc_to_asc;

-- ALTER SEQUENCE: change direction + explicit MINVALUE, verify user value preserved
CREATE SEQUENCE s_dir_custom_min;
ALTER SEQUENCE s_dir_custom_min INCREMENT -1 MINVALUE -100;
SHOW CREATE SEQUENCE s_dir_custom_min;
SELECT nextval('s_dir_custom_min') AS next_custom_min;
DROP SEQUENCE s_dir_custom_min;

-- ALTER SEQUENCE: change direction + explicit MAXVALUE, verify user value preserved
CREATE SEQUENCE s_dir_custom_max;
ALTER SEQUENCE s_dir_custom_max INCREMENT -1 MAXVALUE 0;
SHOW CREATE SEQUENCE s_dir_custom_max;
SELECT nextval('s_dir_custom_max') AS next_custom_max;
DROP SEQUENCE s_dir_custom_max;

-- ALTER SEQUENCE: change direction + NO MINVALUE, verify new direction default applied
CREATE SEQUENCE s_no_min;
ALTER SEQUENCE s_no_min INCREMENT -1 NO MINVALUE;
SHOW CREATE SEQUENCE s_no_min;
SELECT nextval('s_no_min') AS next_no_min;
DROP SEQUENCE s_no_min;

-- ALTER SEQUENCE: change direction + NO MAXVALUE, verify new direction default applied
CREATE SEQUENCE s_no_max;
ALTER SEQUENCE s_no_max INCREMENT -1 NO MAXVALUE;
SHOW CREATE SEQUENCE s_no_max;
SELECT nextval('s_no_max') AS next_no_max;
DROP SEQUENCE s_no_max;

-- ALTER SEQUENCE: change direction + NO MINVALUE NO MAXVALUE together
CREATE SEQUENCE s_no_both;
ALTER SEQUENCE s_no_both INCREMENT -1 NO MINVALUE NO MAXVALUE;
SHOW CREATE SEQUENCE s_no_both;
SELECT nextval('s_no_both') AS next_no_both;
DROP SEQUENCE s_no_both;

-- ALTER SEQUENCE: change only CACHE, verify START and other values unchanged
CREATE SEQUENCE s_cache START 10;
SHOW CREATE SEQUENCE s_cache;
ALTER SEQUENCE s_cache CACHE 1;
SHOW CREATE SEQUENCE s_cache;
DROP SEQUENCE s_cache;
