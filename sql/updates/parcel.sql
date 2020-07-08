----------------------------------FLAG DEAD PARCELS (DEPENDANT ON MAPPING_TABLE BEING UPDATED FIRST)------------------------------------
CREATE OR REPLACE FUNCTION core.dead_parcel()
RETURNS void AS $$
BEGIN

WITH REDB_PARCEL_IDS AS
	(
	WITH DEAD_PARCEL_IDS AS
		(
		SELECT PREVIOUS_WEEK."ParcelId"
		FROM "staging_2"."prcl_prcl" AS PREVIOUS_WEEK
		LEFT JOIN "staging_1"."prcl_prcl" AS CURRENT_WEEK
		ON PREVIOUS_WEEK."ParcelId" = CURRENT_WEEK."ParcelId"
		WHERE CURRENT_WEEK."ParcelId" IS NULL
		)
	SELECT DISTINCT SUBSTRING("county_id_mapping_table"."parcel_id" FROM 1 FOR 14) AS redb_county_id
	FROM "core"."county_id_mapping_table"
	JOIN DEAD_PARCEL_IDS
	ON DEAD_PARCEL_IDS."ParcelId" = "county_id_mapping_table"."county_parcel_id"
	)
UPDATE "core"."parcel" 
SET "removed_flag" = TRUE,
	"current_flag" = FALSE,
	"update_date" = CURRENT_DATE
FROM REDB_PARCEL_IDS
WHERE "redb_county_id" = SUBSTRING("parcel"."parcel_id" FROM 1 FOR 14);

END;
$$
LANGUAGE plpgsql;
-------------------------
SELECT core.dead_parcel();