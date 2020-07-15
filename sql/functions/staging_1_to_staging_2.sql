CREATE OR REPLACE FUNCTION core.staging1_to_staging2()
RETURNS void AS $$
BEGIN

--Clear staging_2
DELETE FROM "staging_2"."prcl_bldgcom";
DELETE FROM "staging_2"."prcl_bldgres";
DELETE FROM "staging_2"."prcl_bldgsect";
DELETE FROM "staging_2"."prcl_prcl";

--Move data from staging_1 to staging_2
INSERT INTO "staging_2"."prcl_prcl"
SELECT * FROM "staging_1"."prcl_prcl";

INSERT INTO "staging_2"."prcl_bldgcom"
SELECT * FROM "staging_1"."prcl_bldgcom";

INSERT INTO "staging_2"."prcl_bldgres"
SELECT * FROM "staging_1"."prcl_bldgres";

INSERT INTO "staging_2"."prcl_bldgsect"
SELECT * FROM "staging_1"."prcl_bldgsect";

--Clear staging_1
DELETE FROM "staging_1"."prcl_bldgcom";
DELETE FROM "staging_1"."prcl_bldgres";
DELETE FROM "staging_1"."prcl_bldgsect";
DELETE FROM "staging_1"."prcl_prcl";

END;
$$
LANGUAGE plpgsql;