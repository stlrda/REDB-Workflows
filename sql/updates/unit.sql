----------------------------------------MARK DEAD UNITS AS SUCH------------------------------------------------------------
CREATE OR REPLACE FUNCTION core.dead_unit()
RETURNS void AS $$
BEGIN

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
			SELECT BUILDING_RECORD."ParcelId"
				, BUILDING_RECORD."Condominium"
				, "prcl_bldgall"."BldgNum"
			FROM BUILDING_RECORD
			JOIN "staging_2"."prcl_bldgall"
			ON "prcl_bldgall"."ParcelId" = BUILDING_RECORD."ParcelId"
			)
		SELECT (SELECT core.format_parcelId(prcl_bldgsect."CityBlock", prcl_bldgsect."Parcel", prcl_bldgsect."OwnerCode")) AS "ParcelId"
			, "prcl_bldgsect"."BldgNum"
			, "prcl_bldgsect"."SectNum" -- SectNum = Unit Number
			, "Condominium"
		FROM "staging_2"."prcl_bldgsect"
		JOIN BUILDING_TABLE
		ON (SELECT core.format_parcelId(prcl_bldgsect."CityBlock", prcl_bldgsect."Parcel", prcl_bldgsect."OwnerCode")) = BUILDING_TABLE."ParcelId" 
			AND BUILDING_TABLE."BldgNum" = "prcl_bldgsect"."BldgNum"
		WHERE "prcl_bldgsect"."BldgNum" IS NOT NULL
		ORDER BY (SELECT core.format_parcelId(prcl_bldgsect."CityBlock", prcl_bldgsect."Parcel", prcl_bldgsect."OwnerCode"))
			, CAST("prcl_bldgsect"."BldgNum" AS INT)
		)
	SELECT DISTINCT "county_id_mapping_table"."parcel_id", UNIT_TABLE."BldgNum", UNIT_TABLE."SectNum", UNIT_TABLE."Condominium"
	FROM UNIT_TABLE
	LEFT JOIN (SELECT "prcl_bldgall"."ParcelId"
					, "prcl_bldgall"."BldgNum"
					, "prcl_bldgsect"."SectNum"
					FROM "staging_1"."prcl_bldgall"
					JOIN "staging_1"."prcl_bldgsect"
					ON (SELECT core.format_parcelId(prcl_bldgsect."CityBlock", prcl_bldgsect."Parcel", prcl_bldgsect."OwnerCode")) = "prcl_bldgall"."ParcelId" 
						AND "prcl_bldgsect"."BldgNum" = "prcl_bldgall"."BldgNum"
					) UNION_UNITS
	ON UNION_UNITS."ParcelId" = UNIT_TABLE."ParcelId" 
		AND UNION_UNITS."BldgNum" = UNIT_TABLE."BldgNum" 
		AND UNION_UNITS."SectNum" = UNIT_TABLE."SectNum"
	JOIN "core"."county_id_mapping_table"
	ON UNIT_TABLE."ParcelId" = county_id_mapping_table."county_parcel_id"
	WHERE UNION_UNITS."ParcelId" IS NULL
	)
UPDATE "core"."unit"
SET "current_flag" = FALSE,
	"update_date" = CURRENT_DATE
FROM DEAD_UNITS
WHERE "unit_id" = CONCAT(SUBSTRING(DEAD_UNITS."parcel_id" FROM 1 FOR 15), (CAST(DEAD_UNITS."BldgNum" AS INT) + 100), '.' , (CAST(DEAD_UNITS."SectNum" AS INT) + 1000));

-- Update removed flag of Unit in core that are present in staging 1 to FALSE
UPDATE "core"."unit"
SET "removed_flag" = FALSE 
	WHERE "unit"."unit_id" 
	IN (SELECT "unit"."unit_id"
		FROM "core"."unit"
		JOIN "core"."county_id_mapping_table"
		ON "county_id_mapping_table"."parcel_id" = CONCAT(SUBSTRING("unit"."unit_id" FROM 1 FOR 15), '000.0000')
		JOIN "staging_1"."prcl_bldgsect"
		ON "county_id_mapping_table"."county_parcel_id" = (SELECT core.format_parcelId(prcl_bldgsect."CityBlock", prcl_bldgsect."Parcel", prcl_bldgsect."OwnerCode"))
	   		AND (CAST(SUBSTRING("unit_id" FROM 16 FOR 3) AS INT) - 100) = CAST("prcl_bldgsect"."BldgNum" AS INT)
			AND (CAST(SUBSTRING("unit_id" FROM 20 FOR 4) AS INT) - 1000) = CAST("prcl_bldgsect"."SectNum" AS INT));

-- Update removed flag of Unit in core that are NOT present in staging 1 to TRUE
UPDATE "core"."unit"
SET "removed_flag" = TRUE 
	WHERE "unit"."unit_id" 
	IN (SELECT "unit"."unit_id"
		FROM "core"."unit"
		JOIN "core"."county_id_mapping_table"
		ON "county_id_mapping_table"."parcel_id" = CONCAT(SUBSTRING("unit"."unit_id" FROM 1 FOR 15), '000.0000')
		LEFT JOIN "staging_1"."prcl_bldgsect"
		ON "county_id_mapping_table"."county_parcel_id" = (SELECT core.format_parcelId(prcl_bldgsect."CityBlock", prcl_bldgsect."Parcel", prcl_bldgsect."OwnerCode"))
	   		AND (CAST(SUBSTRING("unit_id" FROM 16 FOR 3) AS INT) - 100) = CAST("prcl_bldgsect"."BldgNum" AS INT)
			AND (CAST(SUBSTRING("unit_id" FROM 20 FOR 4) AS INT) - 1000) = CAST("prcl_bldgsect"."SectNum" AS INT)
		WHERE "prcl_bldgsect"."Parcel" IS NULL);
		
END;
$$
LANGUAGE plpgsql;
-------------------------
SELECT core.dead_unit();
