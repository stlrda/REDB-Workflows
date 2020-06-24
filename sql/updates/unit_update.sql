--------------------------------------INSERT NEW UNITS INTO CORE-------------------------------------------------
WITH NEW_UNITS AS -- joins to our ID Lookup table to relate prcl_11 ID to parcelNumber found within the REDB unique ID 
	(
	WITH UNIT_TABLE AS -- joins on constructed prcl_11 ID to further narrow down to matching records from BldgSect table
		(
		WITH BUILDING_TABLE AS -- joins on constructed prcl_11 ID to matching records from BCom & BRes and unions them into one query
			(
			WITH BUILDING_RECORD AS -- Pulls in potential unit records from prcl along with the condominium field to be used in final query
				(
				SELECT "prcl_prcl"."ParcelId"
					, CAST("Condominium" AS BOOLEAN) AS "Condominium"
				FROM "staging_1"."prcl_prcl"
				WHERE "Parcel" != "GisParcel" AND "OwnerCode" != '8'
				)
			SELECT "ParcelId"
				, "Condominium"
				, "prcl_bldgcom"."BldgNum" AS "BldgNum"
			FROM BUILDING_RECORD
			JOIN "staging_1"."prcl_bldgcom"
			ON replace(replace(concat(to_char(prcl_bldgcom."CityBlock"::float8,'0000.00'),to_char(prcl_bldgcom."Parcel"::int8,'0000'),prcl_bldgcom."OwnerCode"),'.',''),' ','') = BUILDING_RECORD."ParcelId"
			UNION ALL
			SELECT "ParcelId"
				, "Condominium"
				, "prcl_bldgres"."BldgNum" AS "BldgNum" 
			FROM BUILDING_RECORD
			JOIN "staging_1"."prcl_bldgres"
			ON replace(replace(concat(to_char(prcl_bldgres."CityBlock"::float8,'0000.00'),to_char(prcl_bldgres."Parcel"::int8,'0000'),prcl_bldgres."OwnerCode"),'.',''),' ','') = BUILDING_RECORD."ParcelId"
			)
		SELECT replace(replace(concat(to_char(prcl_bldgsect."CityBlock"::float8,'0000.00'),to_char(prcl_bldgsect."Parcel"::int8,'0000'),prcl_bldgsect."OwnerCode"),'.',''),' ','') AS "ParcelId"
			, "prcl_bldgsect"."BldgNum", "prcl_bldgsect"."SectNum" -- NectNum = Unit Number
			, "Condominium"
		FROM "staging_1"."prcl_bldgsect"
		JOIN BUILDING_TABLE
		ON replace(replace(concat(to_char(prcl_bldgsect."CityBlock"::float8,'0000.00'),to_char(prcl_bldgsect."Parcel"::int8,'0000'),prcl_bldgsect."OwnerCode"),'.',''),' ','') = BUILDING_TABLE."ParcelId" AND BUILDING_TABLE."BldgNum" = "prcl_bldgsect"."BldgNum"
		WHERE "prcl_bldgsect"."BldgNum" IS NOT NULL
		ORDER BY replace(replace(concat(to_char(prcl_bldgsect."CityBlock"::float8,'0000.00'),to_char(prcl_bldgsect."Parcel"::int8,'0000'),prcl_bldgsect."OwnerCode"),'.',''),' ','')
			, CAST("prcl_bldgsect"."BldgNum" AS INT)
		)
	SELECT "county_id_mapping_table"."parcel_id"
		, UNIT_TABLE."BldgNum"
		, UNIT_TABLE."SectNum"
		, UNIT_TABLE."Condominium"
	FROM UNIT_TABLE
	LEFT JOIN (SELECT UNION_BLDGS."ParcelId"
				, UNION_BLDGS."BldgNum"
				, "prcl_bldgsect"."SectNum"
				FROM (SELECT replace(replace(concat(to_char(prcl_bldgcom."CityBlock"::float8,'0000.00'),to_char(prcl_bldgcom."Parcel"::int8,'0000'),prcl_bldgcom."OwnerCode"),'.',''),' ','') AS "ParcelId"
					, "BldgNum"
					FROM "staging_2"."prcl_bldgcom"
					UNION ALL
					SELECT replace(replace(concat(to_char(prcl_bldgres."CityBlock"::float8,'0000.00'),to_char(prcl_bldgres."Parcel"::int8,'0000'),prcl_bldgres."OwnerCode"),'.',''),' ','') AS "ParcelID"
					, "BldgNum"
					FROM "staging_2"."prcl_bldgres"
					) UNION_BLDGS	
				JOIN "staging_2"."prcl_bldgsect"
				ON replace(replace(concat(to_char(prcl_bldgsect."CityBlock"::float8,'0000.00'),to_char(prcl_bldgsect."Parcel"::int8,'0000'),prcl_bldgsect."OwnerCode"),'.',''),' ','') = UNION_BLDGS."ParcelId" AND UNION_BLDGS."BldgNum" = "prcl_bldgsect"."BldgNum"
				) UNION_UNITS
	ON UNION_UNITS."ParcelId" = UNIT_TABLE."ParcelId" AND UNION_UNITS."BldgNum" = UNIT_TABLE."BldgNum" AND UNION_UNITS."SectNum" = UNIT_TABLE."SectNum"
	JOIN "core"."county_id_mapping_table"
	ON "county_parcel_id" = UNIT_TABLE."ParcelId"
	WHERE UNION_UNITS."ParcelId" IS NULL
	)
INSERT INTO "core"."unit"(
	"unit_id"
	, "building_id"
	, "description"
	, "condominium"
	, "create_date"
	, "current_flag"
	, "removed_flag"
	--, "etl_job"
	, "update_date"
	)
(SELECT CONCAT(SUBSTRING(NEW_UNITS."parcel_id" FROM 1 FOR 15), (CAST (NEW_UNITS."BldgNum" AS INT) + 100), '.', (CAST (NEW_UNITS."SectNum" AS INT) + 1000))
	, "building"."building_id"
	, "building"."description"
	, NEW_UNITS."Condominium"
	, CURRENT_DATE
	, TRUE
	, FALSE
	, CURRENT_DATE
FROM NEW_UNITS
JOIN "core"."building"
ON "building"."building_id" = CONCAT(SUBSTRING(NEW_UNITS."parcel_id" FROM 1 FOR 15), (CAST (NEW_UNITS."BldgNum" AS INT) + 100), '.0000')
);

----------------------------------------MARK DEAD UNITS AS SUCH------------------------------------------------------------
WITH DEAD_UNITS AS
	(
	WITH UNIT_TABLE AS -- joins on constructed prcl_11 ID to further narrow down to matching records from BldgSect table
		(
		WITH BUILDING_TABLE AS -- joins on constructed prcl_11 ID to matching records from BCom & BRes and unions them into one query
			(
			WITH BUILDING_RECORD AS -- Pulls in potential unit records from prcl along with the condominium field to be used in final query
				(
				SELECT "prcl_prcl"."ParcelId"
					, CAST("Condominium" AS BOOLEAN) AS "Condominium"
				FROM "staging_2"."prcl_prcl"
				WHERE "Parcel" != "GisParcel" AND "OwnerCode" != '8'
				)
			SELECT "ParcelId"
				, "Condominium"
				, "prcl_bldgcom"."BldgNum" AS "BldgNum"
			FROM BUILDING_RECORD
			JOIN "staging_2"."prcl_bldgcom"
			ON replace(replace(concat(to_char(prcl_bldgcom."CityBlock"::float8,'0000.00'),to_char(prcl_bldgcom."Parcel"::int8,'0000'),prcl_bldgcom."OwnerCode"),'.',''),' ','') = BUILDING_RECORD."ParcelId"
			UNION ALL
			SELECT "ParcelId"
				, "Condominium"
				, "prcl_bldgres"."BldgNum" AS "BldgNum" 
			FROM BUILDING_RECORD
			JOIN "staging_2"."prcl_bldgres"
			ON replace(replace(concat(to_char(prcl_bldgres."CityBlock"::float8,'0000.00'),to_char(prcl_bldgres."Parcel"::int8,'0000'),prcl_bldgres."OwnerCode"),'.',''),' ','') = BUILDING_RECORD."ParcelId"
			)
		SELECT replace(replace(concat(to_char(prcl_bldgsect."CityBlock"::float8,'0000.00'),to_char(prcl_bldgsect."Parcel"::int8,'0000'),prcl_bldgsect."OwnerCode"),'.',''),' ','') AS "ParcelId"
			, "prcl_bldgsect"."BldgNum", "prcl_bldgsect"."SectNum" -- SectNum = Unit Number
			, "Condominium"
		FROM "staging_2"."prcl_bldgsect"
		JOIN BUILDING_TABLE
		ON replace(replace(concat(to_char(prcl_bldgsect."CityBlock"::float8,'0000.00'),to_char(prcl_bldgsect."Parcel"::int8,'0000'),prcl_bldgsect."OwnerCode"),'.',''),' ','') = BUILDING_TABLE."ParcelId" AND BUILDING_TABLE."BldgNum" = "prcl_bldgsect"."BldgNum"
		WHERE "prcl_bldgsect"."BldgNum" IS NOT NULL
		ORDER BY replace(replace(concat(to_char(prcl_bldgsect."CityBlock"::float8,'0000.00'),to_char(prcl_bldgsect."Parcel"::int8,'0000'),prcl_bldgsect."OwnerCode"),'.',''),' ','')
			, CAST("prcl_bldgsect"."BldgNum" AS INT)
		)
	SELECT "county_id_mapping_table"."parcel_id", UNIT_TABLE."BldgNum", UNIT_TABLE."SectNum", UNIT_TABLE."Condominium"
	FROM UNIT_TABLE
	LEFT JOIN (SELECT UNION_BLDGS."ParcelId"
					, UNION_BLDGS."BldgNum"
					, "prcl_bldgsect"."SectNum"
					FROM (SELECT replace(replace(concat(to_char(prcl_bldgcom."CityBlock"::float8,'0000.00'),to_char(prcl_bldgcom."Parcel"::int8,'0000'),prcl_bldgcom."OwnerCode"),'.',''),' ','') AS "ParcelId"
						, "BldgNum"
						FROM "staging_1"."prcl_bldgcom"
						UNION ALL
						SELECT replace(replace(concat(to_char(prcl_bldgres."CityBlock"::float8,'0000.00'),to_char(prcl_bldgres."Parcel"::int8,'0000'),prcl_bldgres."OwnerCode"),'.',''),' ','') AS "ParcelID"
						, "BldgNum"
						FROM "staging_1"."prcl_bldgres"
						) UNION_BLDGS	
					JOIN "staging_1"."prcl_bldgsect"
					ON replace(replace(concat(to_char(prcl_bldgsect."CityBlock"::float8,'0000.00'),to_char(prcl_bldgsect."Parcel"::int8,'0000'),prcl_bldgsect."OwnerCode"),'.',''),' ','') = UNION_BLDGS."ParcelId" AND UNION_BLDGS."BldgNum" = "prcl_bldgsect"."BldgNum"
					) UNION_UNITS
	ON UNION_UNITS."ParcelId" = UNIT_TABLE."ParcelId" AND UNION_UNITS."BldgNum" = UNIT_TABLE."BldgNum" AND UNION_UNITS."SectNum" = UNIT_TABLE."SectNum"
	JOIN "core"."county_id_mapping_table"
	ON UNIT_TABLE."ParcelId" = county_id_mapping_table."county_parcel_id"
	WHERE UNION_UNITS."ParcelId" IS NULL
	)
UPDATE "core"."unit"
SET "current_flag" = FALSE,
	"removed_flag" = TRUE,
	"update_date" = CURRENT_DATE
FROM DEAD_UNITS
WHERE "unit_id" = CONCAT(SUBSTRING(DEAD_UNITS."parcel_id" FROM 1 FOR 15), (CAST(DEAD_UNITS."BldgNum" AS INT) + 100), '.' , (CAST(DEAD_UNITS."SectNum" AS INT) + 1000))