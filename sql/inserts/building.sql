CREATE TABLE "core"."buildingtest" (
    "parcel_id" varchar,
	"building_id" varchar,
	"parcel_id_new" varchar PRIMARY KEY,
    "owner_id" varchar,
    "description" varchar,
    "apartment_count" int,
    "create_date" date,
    "current_flag" boolean,
    "removed_flag" boolean,
    "etl_job" varchar,
    "update_date" date
);

-- CREATE SEQUENCE IF NOT EXISTS core.buildingID 
-- INCREMENT BY 1 
-- START 101
-- OWNED BY core.buildingtest.building_id;

-----------------------------With ID Lookup Table---------------------------------------------------------------------------------------------
WITH BuildingTable AS --Joins BCom & BRes on our inner query to narrow down the building list
	(
	WITH BuildingRecord AS --Selects records from prcl that meet our building criteria and returns NbrOfApts along with ParcelId
		(
		SELECT "prcl_prcl"."ParcelId", "NbrOfApts" 
		FROM "core"."parcel_id"
		JOIN "staging_1"."prcl_prcl"
		ON "prcl_prcl"."ParcelId" = "parcel_id"."ParcelId"
		JOIN "core"."parceltest"
		ON "parceltest"."parcel_number" = "parcel_id"."parcel_number"
		WHERE "Parcel" != "GisParcel" AND "prcl_prcl"."OwnerCode" != '8'
		)
	SELECT "ParcelId", "NbrOfApts"
	FROM BuildingRecord
	JOIN "staging_1"."prcl_bldgcom"
	ON replace(replace(concat(to_char(prcl_bldgcom."CityBlock"::float8,'0000.00'),to_char(prcl_bldgcom."Parcel"::int8,'0000'),prcl_bldgcom."OwnerCode"),'.',''),' ','') = BuildingRecord."ParcelId"
	UNION ALL
	SELECT "ParcelId", "NbrOfApts" 
	FROM BuildingRecord
	JOIN "staging_1"."prcl_bldgres"
	ON replace(replace(concat(to_char(prcl_bldgres."CityBlock"::float8,'0000.00'),to_char(prcl_bldgres."Parcel"::int8,'0000'),prcl_bldgres."OwnerCode"),'.',''),' ','') = BuildingRecord."ParcelId"
	)
INSERT INTO "core"."buildingtest" ("parcel_id"
						, "building_id"
						, "parcel_id_new"
						, "owner_id"
						, "description"
						, "apartment_count"
						--, "create_date"
						--, "current_flag"
						--, "removed_flag"
						--, "etl_job"
						--, "update_date"
						)
	(SELECT "parceltest"."parcel_id"
			, (ROW_NUMBER () OVER(PARTITION BY "parcel_id") + 100) --Counts each row associated with a parcel_id starting at 101
			, CONCAT(SUBSTRING("parceltest"."parcel_id" FROM 1 FOR 15),(ROW_NUMBER () OVER(PARTITION BY "parcel_id") + 100),'.0000') --incorporates the building_id into the parcel_id
			, "owner_id"
			, "description"
			, CAST("NbrOfApts" AS INT)
	FROM "core"."parcel_id"
	JOIN BuildingTable
	ON BuildingTable."ParcelId" = "parcel_id"."ParcelId"
	JOIN "core"."parceltest"
	ON "parceltest"."parcel_number" = "parcel_id"."parcel_number"
	ORDER BY "parceltest"."parcel_id"
	)