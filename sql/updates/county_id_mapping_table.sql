---------------Flag dead parcels in mapping table------------
CREATE OR REPLACE FUNCTION core.dead_county_id()
RETURNS void AS $$
BEGIN

WITH DEAD_PARCELS AS
	(
	SELECT PREVIOUS_WEEK."ParcelId"
	FROM staging_2.prcl_prcl AS PREVIOUS_WEEK
	LEFT JOIN staging_1."prcl_prcl" AS CURRENT_WEEK
	ON PREVIOUS_WEEK."ParcelId" = CURRENT_WEEK."ParcelId"
	WHERE CURRENT_WEEK."ParcelId" IS NULL
	)
UPDATE "core"."county_id_mapping_table" 
	SET "removed_flag" = TRUE,
		"current_flag" = FALSE,
		"update_date" = CURRENT_DATE
FROM DEAD_PARCELS
WHERE DEAD_PARCELS."ParcelId" = "county_id_mapping_table"."county_parcel_id";

END;
$$
LANGUAGE plpgsql;
-------------------------
SELECT core.dead_county_id();