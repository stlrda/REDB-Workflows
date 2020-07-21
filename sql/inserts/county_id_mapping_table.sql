---------------------insert new parcels into mapping table--------------------
CREATE OR REPLACE FUNCTION core.new_county_id()
RETURNS void AS $$
BEGIN

WITH NEW_PARCELS AS 
	(
	SELECT DISTINCT CURRENT_WEEK."ParcelId"
	FROM staging_1.prcl_prcl AS CURRENT_WEEK
	LEFT JOIN staging_2."prcl_prcl" AS PREVIOUS_WEEK
	ON CURRENT_WEEK."ParcelId" = PREVIOUS_WEEK."ParcelId"
	WHERE PREVIOUS_WEEK."ParcelId" IS NULL
	)
INSERT INTO "core"."county_id_mapping_table"("county_id"
	, "county_parcel_id"
	, "parcel_id"
	, "county_parcel_id_type"
	, "create_date"
	, "current_flag"
	, "removed_flag"
	, "update_date"
	)
SELECT '10001'
	, "ParcelId"
	, CONCAT('10001', '.', nextval('core.id_mapping'), '.000.0000')
	, 'parcel_11'
	, CURRENT_DATE
	, TRUE
	, FALSE
	, CURRENT_DATE
FROM NEW_PARCELS
ON CONFLICT ON CONSTRAINT UC_Mapping DO UPDATE
SET "current_flag" = TRUE
	, "removed_flag" = FALSE
	, "update_date" = CURRENT_DATE;

END;
$$
LANGUAGE plpgsql;
-------------------------
SELECT core.new_county_id();