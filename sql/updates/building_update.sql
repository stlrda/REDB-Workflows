-- TODO look into wether or not BuildingTable should be a view if it is used in multiple places
-- Joins prcl_bcom & prcl_bres on our inner query to further narrow down the building list
-- Join criteria is the ParcelId field which is constructed from CityBlock, Parcel and OwnerCode on the bcom & bres tables  
---------INSERT NEW BUILDINGS INTO BUILDING TABLE----------------------
WITH NEW_BUILDINGS AS --Compares Current(staging_1) to past(staging_2) and selects returns the ParcelID of NEW building records.
	(
	WITH BUILDING_TABLE AS --joins results of BUILDING_RECORD on the bldgcom and bldgres tables further limiting scope
		(
		WITH BUILDING_RECORD AS --Selects records from prcl_prcl that meet our building criteria and returns NbrOfApts along with ParcelId
			(
			SELECT "prcl_prcl"."ParcelId", "NbrOfApts" 
			FROM "staging_1"."prcl_prcl"
			WHERE "Parcel" != "GisParcel" AND "OwnerCode" != '8'
			)
		SELECT "ParcelId", "BldgNum", "NbrOfApts"
		FROM BUILDING_RECORD
		JOIN "staging_1"."prcl_bldgcom"
		ON (SELECT core.format_parcelId(prcl_bldgcom."CityBlock", prcl_bldgcom."Parcel", prcl_bldgcom."OwnerCode")) = BUILDING_RECORD."ParcelId"
		UNION ALL
		SELECT "ParcelId", "BldgNum", "NbrOfApts" 
		FROM BUILDING_RECORD
		JOIN "staging_1"."prcl_bldgres"
		ON (SELECT core.format_parcelId(prcl_bldgres."CityBlock", prcl_bldgres."Parcel", prcl_bldgres."OwnerCode")) = BUILDING_RECORD."ParcelId"
		)
	SELECT BUILDING_TABLE."ParcelId", "BldgNum", "NbrOfApts"
	FROM BUILDING_TABLE
	LEFT JOIN (SELECT (SELECT core.format_parcelId(prcl_bldgcom."CityBlock", prcl_bldgcom."Parcel", prcl_bldgcom."OwnerCode")) AS "ParcelId"
				FROM "staging_2"."prcl_bldgcom"
				UNION ALL
				SELECT (SELECT core.format_parcelId(prcl_bldgres."CityBlock", prcl_bldgres."Parcel", prcl_bldgres."OwnerCode")) AS "ParcelID"
                FROM "staging_2"."prcl_bldgres") UNION_BLDGS
	ON UNION_BLDGS."ParcelId" = BUILDING_TABLE."ParcelId"
	WHERE UNION_BLDGS."ParcelId" IS NULL
	)
INSERT INTO "core"."building" ("parcel_id"
	, "building_id"
	, "owner_id"
	, "description"
	, "apartment_count"
	, "create_date"
	, "current_flag"
	, "removed_flag"
	--, "etl_job"
	, "update_date"
	)
	(SELECT "parcel"."parcel_id"
		, CONCAT(SUBSTRING("parcel"."parcel_id" FROM 1 FOR 15), (CAST(NEW_BUILDINGS."BldgNum" AS INT) + 100),'.0000') -- Counts each row associated with a parcel_id starting at 101 and incorporates the building_id into the parcel_id
		, "owner_id"
		, "description"
		, CAST("NbrOfApts" AS INT)
		, CURRENT_DATE
		, TRUE
		, FALSE
		, CURRENT_DATE
	FROM "core"."county_id_mapping_table"
	JOIN NEW_BUILDINGS
	ON NEW_BUILDINGS."ParcelId" = "county_id_mapping_table"."county_parcel_id"
	JOIN "core"."parcel"
	ON CONCAT("parcel"."county_id", '.', "parcel"."parcel_number") = SUBSTRING("county_id_mapping_table"."parcel_id" FROM 1 FOR 14)
	ORDER BY 1, 2
	);

-----------------DEAD BUILDINGS WITH BldgNum INCLUDED-------------------
WITH DEAD_BUILDINGS AS --Compares Current(staging_1) to past(staging_2) and selects returns the ParcelID of NEW building records.
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
	LEFT JOIN (SELECT (SELECT core.format_parcelId(prcl_bldgcom."CityBlock", prcl_bldgcom."Parcel", prcl_bldgcom."OwnerCode")) AS "ParcelId"
				FROM "staging_1"."prcl_bldgcom"
				UNION ALL
				SELECT (SELECT core.format_parcelId(prcl_bldgres."CityBlock", prcl_bldgres."Parcel", prcl_bldgres."OwnerCode")) AS "ParcelID"
                FROM "staging_1"."prcl_bldgres") UNION_BLDGS
	ON UNION_BLDGS."ParcelId" = BUILDING_TABLE."ParcelId"
	JOIN "core"."county_id_mapping_table"
	ON BUILDING_TABLE."ParcelId" = county_id_mapping_table."county_parcel_id"
	WHERE UNION_BLDGS."ParcelId" IS NULL
	)
UPDATE "core"."building"
SET "removed_flag" = TRUE,
	"current_flag" = FALSE,
	"update_date" = CURRENT_DATE
FROM DEAD_BUILDINGS
WHERE CONCAT(SUBSTRING(DEAD_BUILDINGS."parcel_id" FROM 1 FOR 14), '.', CAST("BldgNum" AS INT) + 100) = SUBSTRING("building"."building_id" FROM 1 FOR 18)