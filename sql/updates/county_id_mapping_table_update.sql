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
FROM NEW_PARCELS;

END;
$$
LANGUAGE plpgsql;

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