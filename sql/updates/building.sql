-----------------DEAD BUILDINGS WITH BldgNum INCLUDED-------------------
CREATE OR REPLACE FUNCTION core.dead_building()
RETURNS void AS $$
BEGIN

WITH DEAD_BUILDINGS AS --Compares Past(staging_2) to Current(staging_1) and returns the ParcelID of DEAD building records.
	(
	WITH BUILDING_TABLE AS --joins results of BUILDING_RECORD on the bldgcom and bldgres tables further limiting scope
		(	
		WITH BUILDING_RECORD AS --Selects records from prcl_prcl that meet our building criteria and returns NbrOfApts along with ParcelId
			(
			SELECT "prcl_prcl"."ParcelId"
				, "NbrOfApts" 
			FROM "staging_2"."prcl_prcl"
			WHERE "Parcel" != "GisParcel" AND "OwnerCode" != '8'
			)
		SELECT BUILDING_RECORD."ParcelId"
			, "BldgNum"
			, "BldgUse"
			, "NbrOfApts"
		FROM BUILDING_RECORD
		JOIN "staging_2"."prcl_bldgall"
		ON (SELECT core.format_parcelId(prcl_bldgall."CityBlock", prcl_bldgall."Parcel", prcl_bldgall."OwnerCode")) = BUILDING_RECORD."ParcelId"
		)		
	SELECT "county_id_mapping_table"."parcel_id"
		, BUILDING_TABLE."ParcelId"
		, BUILDING_TABLE."BldgNum"
		, BUILDING_TABLE."BldgUse"
		, BUILDING_TABLE."NbrOfApts"
	FROM BUILDING_TABLE
	LEFT JOIN "staging_1"."prcl_bldgall"
	ON "prcl_bldgall"."ParcelId" = BUILDING_TABLE."ParcelId" 
		AND "prcl_bldgall"."BldgNum" = BUILDING_TABLE."BldgNum" 
		AND COALESCE("prcl_bldgall"."BldgUse", 'NULL') = COALESCE(BUILDING_TABLE."BldgUse", 'NULL') --sometimes BldgUse is null for some reason
	JOIN "core"."county_id_mapping_table"
	ON BUILDING_TABLE."ParcelId" = county_id_mapping_table."county_parcel_id"
	WHERE "prcl_bldgall"."ParcelId" IS NULL
	)
UPDATE "core"."building"
SET "removed_flag" = TRUE,
	"current_flag" = FALSE,
	"update_date" = CURRENT_DATE
FROM DEAD_BUILDINGS
WHERE CONCAT(SUBSTRING(DEAD_BUILDINGS."parcel_id" FROM 1 FOR 14), '.', CAST("BldgNum" AS INT) + 100) = SUBSTRING("building"."building_id" FROM 1 FOR 18);

END;
$$
LANGUAGE plpgsql;
-------------------------
SELECT core.dead_building();