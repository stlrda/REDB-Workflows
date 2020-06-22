---------------------insert new parcels--------------------
WITH NEW_PARCELS AS 
	(
	SELECT DISTINCT "prcl_test"."ParcelId"
	FROM staging_1.prcl_test
	LEFT JOIN core."county_id_mapping_table"
		ON "prcl_test"."ParcelId" = "county_id_mapping_table"."county_parcel_id"
	WHERE "county_id_mapping_table"."county_parcel_id" IS NULL
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

---------------flag dead parcels is part of a trigger in another file------------