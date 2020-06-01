CREATE TABLE IF NOT EXISTS "core"."unit" (
    "unit_id" varchar PRIMARY KEY
	, "building_id" varchar
	, "description" varchar
	, "condominium" boolean
	, "create_date" date
	, "current_flag" boolean
	, "removed_flag" boolean
	, "etl_job" varchar
	, "update_date" date
);

WITH UnitQry AS -- joins to our ID Lookup table to relate prcl_11 ID to parcelNumber found within the REDB unique ID 
	(
	WITH UnitTable AS -- joins on constructed prcl_11 ID to further narrow down to matching records from BldgSect table
		(
		WITH BuildingTable AS -- joins on constructed prcl_11 ID to matching records from BCom & BRes and unions them into one query
			(
			WITH BuildingRecord AS -- Pulls in potential unit records from prcl along with the condominium field to be used in final query
				(
				SELECT "prcl_prcl"."ParcelId"
					, CAST("Condominium" AS BOOLEAN) AS "Condominium"
				FROM "staging_2"."prcl_prcl"
				WHERE "Parcel" != "GisParcel" AND "OwnerCode" != '8'
				)
			SELECT "ParcelId"
				, "Condominium"
				, "prcl_bldgcom"."BldgNum" AS "BldgNum"
			FROM BuildingRecord
			JOIN "staging_2"."prcl_bldgcom"
			ON replace(replace(concat(to_char(prcl_bldgcom."CityBlock"::float8,'0000.00'),to_char(prcl_bldgcom."Parcel"::int8,'0000'),prcl_bldgcom."OwnerCode"),'.',''),' ','') = BuildingRecord."ParcelId"
			UNION ALL
			SELECT "ParcelId"
				, "Condominium"
				, "prcl_bldgres"."BldgNum" AS "BldgNum" 
			FROM BuildingRecord
			JOIN "staging_2"."prcl_bldgres"
			ON replace(replace(concat(to_char(prcl_bldgres."CityBlock"::float8,'0000.00'),to_char(prcl_bldgres."Parcel"::int8,'0000'),prcl_bldgres."OwnerCode"),'.',''),' ','') = BuildingRecord."ParcelId"
			)
		SELECT replace(replace(concat(to_char(prcl_bldgsect."CityBlock"::float8,'0000.00'),to_char(prcl_bldgsect."Parcel"::int8,'0000'),prcl_bldgsect."OwnerCode"),'.',''),' ','') AS "ParcelId"
			, "prcl_bldgsect"."BldgNum"
			, "Condominium"
		FROM "staging_2"."prcl_bldgsect"
		JOIN BuildingTable
		ON replace(replace(concat(to_char(prcl_bldgsect."CityBlock"::float8,'0000.00'),to_char(prcl_bldgsect."Parcel"::int8,'0000'),prcl_bldgsect."OwnerCode"),'.',''),' ','') = BuildingTable."ParcelId" AND BuildingTable."BldgNum" = "prcl_bldgsect"."BldgNum"
		WHERE "prcl_bldgsect"."BldgNum" IS NOT NULL
		ORDER BY replace(replace(concat(to_char(prcl_bldgsect."CityBlock"::float8,'0000.00'),to_char(prcl_bldgsect."Parcel"::int8,'0000'),prcl_bldgsect."OwnerCode"),'.',''),' ','')
			, CAST("prcl_bldgsect"."BldgNum" AS INT)
		)
	SELECT "county_id_mapping_table"."parcel_id"
		, "BldgNum"
		, "Condominium"
	FROM UnitTable
	JOIN "core"."county_id_mapping_table"
	ON "county_id_mapping_table"."county_parcel_id" = UnitTable."ParcelId"
	)
INSERT INTO	"core"."unit"(
	"unit_id"
	, "building_id"
	, "description"
	, "condominium"
	--, "create_date"
	--, "current_flag"
	--, "removed_flag"
	--, "etl_job"
	--, "update_date"
	)
	(SELECT	CONCAT(SUBSTRING("parcel_id_new" FROM 1 FOR 19), (ROW_NUMBER () OVER(PARTITION BY "parcel_id_new") + 1000)) --Counts each row associated with a building_id starting at 1001 and incorporates the Unit_id into the building_id
		, "building"."parcel_id_new"
		, "description" 
		, UnitQry."Condominium"
	FROM UnitQry
	JOIN "core"."building"
	ON SUBSTRING("building"."parcel_id_new" FROM 7 FOR 8) = UnitQry."parcel_id" AND CAST(SUBSTRING("building"."parcel_id_new" FROM 16 FOR 3) AS INT) = (CAST(UnitQry."BldgNum" AS INT) + 100)
	ORDER BY "parcel_id_new"
	)