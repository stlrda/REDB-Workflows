CREATE OR REPLACE FUNCTION core.insert_week2_to_staging1()
RETURNS void AS $$
BEGIN

INSERT INTO "staging_1"."prcl_prcl"
SELECT * FROM "staging_1"."week_2_prcl";

INSERT INTO "staging_1"."prcl_bldgcom"
SELECT * FROM "staging_1"."week_2_bldgcom";

INSERT INTO "staging_1"."prcl_bldgres"
SELECT * FROM "staging_1"."week_2_bldgres";

INSERT INTO "staging_1"."prcl_bldgsect"
SELECT * FROM "staging_1"."week_2_bldgsect";

END;
$$
LANGUAGE plpgsql;