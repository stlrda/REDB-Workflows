-------------------------Insert new Neighborhoods-------------------------
CREATE OR REPLACE FUNCTION core.new_neighborhoods()
RETURNS void AS $$
BEGIN

WITH NEW_NEIGHBORHOODS AS
	(
	SELECT DISTINCT CURRENT_WEEK."Nbrhd"
	FROM "staging_1"."prcl_prcl" AS CURRENT_WEEK
	LEFT JOIN "staging_2"."prcl_prcl" AS PREVIOUS_WEEK
	ON CURRENT_WEEK."Nbrhd" = PREVIOUS_WEEK."Nbrhd"
	WHERE PREVIOUS_WEEK."Nbrhd" IS NULL
	ORDER BY CURRENT_WEEK."Nbrhd"
	)
INSERT INTO "core"."neighborhood"("neighborhood_name"
	, "county_id"
	, "create_date"
	, "current_flag"
	, "removed_flag"
	--, "etl_job"
	, "update_date"
	)
SELECT "Nbrhd"
	, '10001'
	, CURRENT_DATE
	, TRUE
	, FALSE
	, CURRENT_DATE
FROM NEW_NEIGHBORHOODS

END;
$$
LANGUAGE plpgsql;

-------------------------Flag Dead Neighborhoods-------------------------
CREATE OR REPLACE FUNCTION core.dead_neighborhoods()
RETURNS void AS $$
BEGIN

WITH DEAD_NEIGHBORHOODS AS
	(
	SELECT DISTINCT PREVIOUS_WEEK."Nbrhd"
	FROM "staging_2"."prcl_prcl" AS PREVIOUS_WEEK
	LEFT JOIN "staging_1"."prcl_prcl" AS CURRENT_WEEK
	ON CURRENT_WEEK."Nbrhd" = PREVIOUS_WEEK."Nbrhd"
	WHERE CURRENT_WEEK."Nbrhd" IS NULL
	)
UPDATE "core"."neighborhood"
SET  "current_flag" = FALSE
	, "removed_flag" = TRUE
	, "update_date" = CURRENT_DATE
FROM DEAD_NEIGHBORHOODS
WHERE "neighborhood"."neighborhood_name" = DEAD_NEIGHBORHOODS."Nbrhd"

END;
$$
LANGUAGE plpgsql;