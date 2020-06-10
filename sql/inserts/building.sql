-- Creates the table for assigning unique Building IDs.
CREATE TABLE IF NOT EXISTS "core"."building" (
    "parcel_id" varchar
	, "building_id" varchar PRIMARY KEY -- CCCCCC.PPPPPPPP.BBB.0000 (county_id.parcel_number.building_number.unit_number)
	, "owner_id" varchar
	, "description" varchar
	, "apartment_count" int
	, "create_date" date
	, "current_flag" boolean
	, "removed_flag" boolean
	, "etl_job" varchar
	, "update_date" date
);

-- Joins prcl_bcom & prcl_bres on our inner query to further narrow down the building list
-- Join criteria is the ParcelId field which is constructed from CityBlock, Parcel and OwnerCode on the bcom & bres tables  
WITH BuildingTable AS 
	(
	WITH BuildingRecord AS --Selects records from prcl_prcl that meet our building criteria and returns NbrOfApts along with ParcelId
		(
		SELECT "prcl_prcl"."ParcelId", "NbrOfApts" 
		FROM "staging_2"."prcl_prcl"
		WHERE "Parcel" != "GisParcel" AND "OwnerCode" != '8'
		)
	SELECT "ParcelId", "NbrOfApts"
	FROM BuildingRecord
	JOIN "staging_2"."prcl_bldgcom"
	ON replace(replace(concat(to_char(prcl_bldgcom."CityBlock"::float8,'0000.00'),to_char(prcl_bldgcom."Parcel"::int8,'0000'),prcl_bldgcom."OwnerCode"),'.',''),' ','') = BuildingRecord."ParcelId"
	UNION ALL
	SELECT "ParcelId", "NbrOfApts" 
	FROM BuildingRecord
	JOIN "staging_2"."prcl_bldgres"
	ON replace(replace(concat(to_char(prcl_bldgres."CityBlock"::float8,'0000.00'),to_char(prcl_bldgres."Parcel"::int8,'0000'),prcl_bldgres."OwnerCode"),'.',''),' ','') = BuildingRecord."ParcelId"
	)
-- Unique Building IDs are constructed via partitioning by parcel_id and concatinating the row_number +100 onto the end of the parcel_id 
-- EG (CCCCCC.PPPPPPPP.BBB.0000)
INSERT INTO "core"."building" ("parcel_id"
	, "building_id"
	, "owner_id"
	, "description"
	, "apartment_count"
	--, "create_date"
	--, "current_flag"
	--, "removed_flag"
	--, "etl_job"
	--, "update_date"
	)
	(SELECT "parcel"."parcel_id"
		, CONCAT(SUBSTRING("parcel"."parcel_id" FROM 1 FOR 15),(ROW_NUMBER () OVER(PARTITION BY "parcel"."parcel_id") + 100),'.0000') -- Counts each row associated with a parcel_id starting at 101 and incorporates the building_id into the parcel_id
		, "owner_id"
		, "description"
		, CAST("NbrOfApts" AS INT)
	FROM "core"."county_id_mapping_table"
	JOIN BuildingTable
	ON BuildingTable."ParcelId" = "county_id_mapping_table"."county_parcel_id"
	JOIN "core"."parcel"
	ON CONCAT("parcel"."county_id", '.', "parcel"."parcel_number") = SUBSTRING("county_id_mapping_table"."parcel_id" FROM 1 FOR 14)
	ORDER BY "parcel"."parcel_id"
	);