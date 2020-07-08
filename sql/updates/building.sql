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
			SELECT "prcl_prcl"."ParcelId", "NbrOfApts" 
			FROM "staging_2"."prcl_prcl"
			WHERE "Parcel" != "GisParcel" AND "OwnerCode" != '8'
			)
		SELECT "ParcelId", "BldgNum", "NbrOfApts"
		FROM BUILDING_RECORD
		JOIN "staging_2"."prcl_bldgcom"
		ON (SELECT core.format_parcelId(prcl_bldgcom."CityBlock", prcl_bldgcom."Parcel", prcl_bldgcom."OwnerCode")) = BUILDING_RECORD."ParcelId"
		UNION ALL
		SELECT "ParcelId", "BldgNum", "NbrOfApts" 
		FROM BUILDING_RECORD
		JOIN "staging_2"."prcl_bldgres"
		ON (SELECT core.format_parcelId(prcl_bldgres."CityBlock", prcl_bldgres."Parcel", prcl_bldgres."OwnerCode")) = BUILDING_RECORD."ParcelId"
		)		
	SELECT "county_id_mapping_table"."parcel_id", BUILDING_TABLE."ParcelId", BUILDING_TABLE."BldgNum", BUILDING_TABLE."NbrOfApts"
	FROM BUILDING_TABLE
	LEFT JOIN (SELECT core.format_parcelId(prcl_bldgcom."CityBlock", prcl_bldgcom."Parcel", prcl_bldgcom."OwnerCode") AS "ParcelId", "prcl_bldgcom"."BldgNum"
			FROM "staging_1"."prcl_bldgcom"
			UNION ALL
			SELECT core.format_parcelId(prcl_bldgres."CityBlock", prcl_bldgres."Parcel", prcl_bldgres."OwnerCode") AS "ParcelId", "prcl_bldgres"."BldgNum"
			FROM "staging_1"."prcl_bldgres") UNION_BLDGS
	ON UNION_BLDGS."ParcelId" = BUILDING_TABLE."ParcelId" AND UNION_BLDGS."BldgNum" = BUILDING_TABLE."BldgNum"
	JOIN "core"."county_id_mapping_table"
	ON BUILDING_TABLE."ParcelId" = county_id_mapping_table."county_parcel_id"
	WHERE UNION_BLDGS."ParcelId" IS NULL
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