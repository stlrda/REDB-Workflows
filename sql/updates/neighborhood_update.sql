-------------------------Insert new Neighborhoods-------------------------
WITH NEW_NEIGHBORHOODS AS
	(
	SELECT DISTINCT "prcl_test"."Nbrhd"
	FROM "staging_1"."prcl_test"
	LEFT JOIN core."neighborhood"
		ON "prcl_test"."Nbrhd" = "neighborhood"."neighborhood_name"
	WHERE "neighborhood"."neighborhood_id" IS NULL
	ORDER BY "prcl_test"."Nbrhd"
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

-------------------------Flag Dead Neighborhoods-------------------------
WITH DEAD_NEIGHBORHOODS AS
	(
	SELECT DISTINCT "neighborhood"."neighborhood_name"
	FROM core."neighborhood"
	LEFT JOIN "staging_1"."prcl_test"
		ON "prcl_test"."Nbrhd" = "neighborhood"."neighborhood_name"
	WHERE "prcl_test"."Nbrhd" IS NULL
	)
UPDATE "core"."neighborhood"
SET  "current_flag" = FALSE
	, "removed_flag" = TRUE
	, "update_date" = CURRENT_DATE
FROM DEAD_NEIGHBORHOODS
WHERE "neighborhood"."neighborhood_name" = DEAD_NEIGHBORHOODS."neighborhood_name"