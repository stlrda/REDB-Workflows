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
WHERE "neighborhood"."neighborhood_name" = DEAD_NEIGHBORHOODS."Nbrhd";

END;
$$
LANGUAGE plpgsql;
-------------------------
SELECT core.dead_neighborhoods();