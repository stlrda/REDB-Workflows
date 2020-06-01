CREATE TABLE IF NOT EXISTS "core"."building" (
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

WITH BuildingTable AS --Joins BCom & BRes on our inner query to narrow down the building list
	(
	WITH BuildingRecord AS --Selects records from prcl that meet our building criteria and returns NbrOfApts along with ParcelId
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
INSERT INTO "core"."building" ("parcel_id"
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
	(SELECT "parcel"."parcel_id"
			, (ROW_NUMBER () OVER(PARTITION BY "parcel"."parcel_id") + 100) --Counts each row associated with a parcel_id starting at 101
			, CONCAT(SUBSTRING("parcel"."parcel_id" FROM 1 FOR 15),(ROW_NUMBER () OVER(PARTITION BY "parcel"."parcel_id") + 100),'.0000') --incorporates the building_id into the parcel_id
			, "owner_id"
			, "description"
			, CAST("NbrOfApts" AS INT)
	FROM "core"."county_id_mapping_table"
	JOIN BuildingTable
	ON BuildingTable."ParcelId" = "county_id_mapping_table"."county_parcel_id"
	JOIN "core"."parcel"
	ON "parcel"."parcel_number" = "county_id_mapping_table"."parcel_id"
	ORDER BY "parcel"."parcel_id"
	)