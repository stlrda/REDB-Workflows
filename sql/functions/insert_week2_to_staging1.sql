CREATE OR REPLACE FUNCTION core.insert_week2_to_staging1()
RETURNS void AS $$
BEGIN

INSERT INTO "staging_1"."prcl_prcl"
SELECT * FROM "staging_1"."week_2_prcl";

INSERT INTO "staging_1"."prcl_bldgall"
SELECT * FROM "staging_1"."week_2_bldgall";

INSERT INTO "staging_1"."prcl_bldgsect"
SELECT * FROM "staging_1"."week_2_bldgsect";

END;
$$
LANGUAGE plpgsql;