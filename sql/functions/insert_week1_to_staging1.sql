CREATE OR REPLACE FUNCTION core.insert_week1_to_staging1()
RETURNS void AS $$
BEGIN

CREATE TABLE "staging_1"."prcl_prcl"
AS TABLE "staging_1"."week_1_prcl_prcl";

CREATE TABLE "staging_1"."prcl_bldgall"
AS TABLE "staging_1"."week_1_prcl_bldgall";

CREATE TABLE "staging_1"."prcl_bldgsect"
AS TABLE "staging_1"."week_1_prcl_bldgsect";

END;
$$
LANGUAGE plpgsql;