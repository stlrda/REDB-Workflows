<<<<<<< HEAD
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
ON CONFLICT ON CONSTRAINT UC_Neighborhood DO UPDATE
SET "current_flag" = TRUE
	, "removed_flag" = FALSE
	, "update_date" = CURRENT_DATE;

END;
$$
LANGUAGE plpgsql;
-------------------------
SELECT core.new_neighborhoods();
=======
INSERT INTO history.neighborhood
    (
     neighborhood_name
     ,county_id
    )
    SELECT DISTINCT
                    pp."Nbrhd"
    ,(SELECT county_id FROM history.county WHERE county.county_name = 'Saint Louis City County')
    FROM "staging_1"."prcl_prcl" pp
    WHERE NOT EXISTS(
        SELECT
               neighborhood_name
        FROM history.neighborhood
        WHERE pp."Nbrhd" = "neighborhood_name"
        )
    RETURNING *;
>>>>>>> theo/development
