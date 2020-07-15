CREATE OR REPLACE FUNCTION core.insert_week1_to_staging1()
RETURNS void AS $$
BEGIN

INSERT INTO "staging_1"."prcl_prcl"
SELECT * FROM "staging_1"."week_1_prcl";

INSERT INTO "staging_1"."prcl_bldgcom"
SELECT * FROM "staging_1"."week_1_bldgcom";

INSERT INTO "staging_1"."prcl_bldgres"
SELECT * FROM "staging_1"."week_1_bldgres";

INSERT INTO "staging_1"."prcl_bldgsect"
SELECT * FROM "staging_1"."week_1_bldgsect";

END;
$$
LANGUAGE plpgsql;