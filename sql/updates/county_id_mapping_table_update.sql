---------------------insert new parcels into mapping table--------------------
WITH NEW_PARCELS AS 
	(
	SELECT DISTINCT staging_1.prcl_prcl."ParcelId"
	FROM staging_1.prcl_prcl
	LEFT JOIN staging_2."prcl_prcl"
	ON staging_1.prcl_prcl."ParcelId" = staging_2."prcl_prcl"."ParcelId"
	WHERE staging_2."prcl_prcl"."ParcelId" IS NULL
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

---------------Flag dead parcels in mapping table------------
WITH DEAD_PARCELS AS
	(
	SELECT staging_2.prcl_prcl."ParcelId"
	FROM staging_2.prcl_prcl
	LEFT JOIN staging_1."prcl_prcl"
	ON staging_2.prcl_prcl."ParcelId" = staging_1."prcl_prcl"."ParcelId"
	WHERE staging_1."prcl_prcl"."ParcelId" IS NULL
	)
UPDATE "core"."county_id_mapping_table" 
	SET "removed_flag" = TRUE,
		"current_flag" = FALSE,
		"update_date" = CURRENT_DATE
FROM DEAD_PARCELS
WHERE DEAD_PARCELS."ParcelId" = "county_id_mapping_table"."county_parcel_id";