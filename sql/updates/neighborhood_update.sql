-------------------------Insert new Neighborhoods-------------------------
WITH NEW_NEIGHBORHOODS AS
	(
	SELECT DISTINCT "staging_1"."prcl_prcl"."Nbrhd"
	FROM "staging_1"."prcl_prcl"
	LEFT JOIN "staging_2"."prcl_prcl"
	ON "staging_1"."prcl_prcl"."Nbrhd" = "staging_2"."prcl_prcl"."Nbrhd"
	WHERE "staging_2"."prcl_prcl"."Nbrhd" IS NULL
	ORDER BY "staging_1"."prcl_prcl"."Nbrhd"
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
	SELECT DISTINCT "staging_2"."prcl_prcl"."Nbrhd"
	FROM "staging_2"."prcl_prcl"
	LEFT JOIN "staging_1"."prcl_prcl"
	ON "staging_1"."prcl_prcl"."Nbrhd" = "staging_2"."prcl_prcl"."Nbrhd"
	WHERE "staging_1"."prcl_prcl"."Nbrhd" IS NULL
	)
UPDATE "core"."neighborhood"
SET  "current_flag" = FALSE
	, "removed_flag" = TRUE
	, "update_date" = CURRENT_DATE
FROM DEAD_NEIGHBORHOODS
WHERE "neighborhood"."neighborhood_name" = DEAD_NEIGHBORHOODS."Nbrhd"