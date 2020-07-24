--------------------------------------INSERT NEW UNITS INTO CORE-------------------------------------------------
CREATE OR REPLACE FUNCTION core.new_unit()
RETURNS void AS $$
BEGIN

CREATE OR REPLACE VIEW "staging_1".NEW_OR_CHANGED_UNITS AS -- joins to our ID Lookup table to relate prcl_11 ID to parcelNumber found within the REDB unique ID 
	(
	WITH UNIT_TABLE AS --selects the records from the CURRENT bldg_sect table that fit our UNIT criteria
		(
		WITH BUILDING_TABLE AS --selects the records from the CURRENT bldg_com & bldg_res tables that fit our BUILDING_RECORD criteria
			(
			WITH BUILDING_RECORD AS --Selects records from CURRENT.prcl_prcl that meet our building criteria and returns Condominium and description along with ParcelId (Need them to compare CURRENT to PREVIOUS)
				(
				SELECT "prcl_prcl"."ParcelId"
					, CAST("Condominium" AS BOOLEAN) AS "Condominium"
					, CONCAT("LegalDesc1",' ',"LegalDesc2",' ',"LegalDesc3",' ',"LegalDesc4",' ',"LegalDesc5") AS description
				FROM "staging_1"."prcl_prcl"
				WHERE "Parcel" != "GisParcel" AND "OwnerCode" != '8'
				)
			SELECT BUILDING_RECORD."ParcelId", "prcl_bldgall"."BldgNum", "description", "Condominium"
			FROM BUILDING_RECORD
			JOIN "staging_1"."prcl_bldgall"
			ON "prcl_bldgall"."ParcelId" = BUILDING_RECORD."ParcelId"
			)
		SELECT (SELECT core.format_parcelId(prcl_bldgsect."CityBlock", prcl_bldgsect."Parcel", prcl_bldgsect."OwnerCode")) AS "ParcelId"
			, "prcl_bldgsect"."BldgNum", "prcl_bldgsect"."SectNum" -- SectNum = Unit Number
			, "description"
			, "Condominium"
		FROM "staging_1"."prcl_bldgsect"
		JOIN BUILDING_TABLE
		ON (SELECT core.format_parcelId(prcl_bldgsect."CityBlock", prcl_bldgsect."Parcel", prcl_bldgsect."OwnerCode")) = BUILDING_TABLE."ParcelId"
			AND BUILDING_TABLE."BldgNum" = "prcl_bldgsect"."BldgNum"
		WHERE "prcl_bldgsect"."BldgNum" IS NOT NULL
		ORDER BY (SELECT core.format_parcelId(prcl_bldgsect."CityBlock", prcl_bldgsect."Parcel", prcl_bldgsect."OwnerCode"))
			, CAST("prcl_bldgsect"."BldgNum" AS INT)
		) -- UNIT TABLE = ALL THE STUFF IN CURRENT
	SELECT DISTINCT UNIT_TABLE."ParcelId", UNIT_TABLE."BldgNum", UNIT_TABLE."SectNum", UNIT_TABLE."description", UNIT_TABLE."Condominium"
	FROM UNIT_TABLE
	LEFT JOIN (
				SELECT DISTINCT UNION_BLDGS."ParcelId"
				, UNION_BLDGS."BldgNum"
				, "prcl_bldgsect"."SectNum"
				, UNION_BLDGS."description"
				, CAST(UNION_BLDGS."Condominium" AS BOOLEAN) AS "Condominium"
				FROM (SELECT PB."ParcelId"
					, PB."BldgNum"
					, PP."Condominium"
					, CONCAT("LegalDesc1",' ',"LegalDesc2",' ',"LegalDesc3",' ',"LegalDesc4",' ',"LegalDesc5") AS description
					FROM "staging_2"."prcl_bldgall" AS PB
					JOIN "staging_2"."prcl_prcl" AS PP
					ON PB."ParcelId" = PP."ParcelId"
					) UNION_BLDGS	
				JOIN "staging_2"."prcl_bldgsect"
				ON (SELECT core.format_parcelId(prcl_bldgsect."CityBlock", prcl_bldgsect."Parcel", prcl_bldgsect."OwnerCode")) = UNION_BLDGS."ParcelId" 
					AND UNION_BLDGS."BldgNum" = "prcl_bldgsect"."BldgNum"
				) UNION_UNITS --UNION UNITS GRABS EVERYTHING IN STAGING 2 PRCL_BLDGSECT AND ADDS THE DESCRIPTION AND CONDO TAG FROM PRCL_PRCL ONTO IT
	ON UNION_UNITS."ParcelId" = UNIT_TABLE."ParcelId"
		AND UNION_UNITS."BldgNum" = UNIT_TABLE."BldgNum"
 		AND UNION_UNITS."SectNum" = UNIT_TABLE."SectNum"
 		AND UNION_UNITS."description" = UNIT_TABLE."description"
 		AND UNION_UNITS."Condominium" = UNIT_TABLE."Condominium"
	WHERE UNION_UNITS."ParcelId" IS NULL
	);
---------------------------------------------------
UPDATE "core"."unit"
SET "update_date" = (CASE
                        WHEN "unit"."current_flag" = TRUE 
                        THEN CURRENT_DATE
                        ELSE "unit"."update_date"
                    END)
    , "current_flag" = FALSE
FROM staging_1.NEW_OR_CHANGED_UNITS, "core"."county_id_mapping_table"
WHERE "unit"."unit_id" = CONCAT(SUBSTRING("county_id_mapping_table"."parcel_id" FROM 1 FOR 15), (CAST(NEW_OR_CHANGED_UNITS."BldgNum" AS INT) + 100), '.', (CAST(NEW_OR_CHANGED_UNITS."SectNum" AS INT) + 1000))
    AND "county_id_mapping_table"."county_parcel_id" = NEW_OR_CHANGED_UNITS."ParcelId";
---------------------------------------------------
INSERT INTO "core"."unit" ("unit_id"
    , "building_id"
    , "description"
    , "condominium"
    , "create_date"
    , "current_flag"
    , "removed_flag"
    --, "etl_job"
    , "update_date")
(SELECT DISTINCT CONCAT(SUBSTRING("county_id_mapping_table"."parcel_id" FROM 1 FOR 15), (CAST(NEW_OR_CHANGED_UNITS."BldgNum" AS INT) + 100), '.', (CAST(NEW_OR_CHANGED_UNITS."SectNum" AS INT) + 1000)) AS unit_id
    , CONCAT(SUBSTRING("county_id_mapping_table"."parcel_id" FROM 1 FOR 15), (CAST(NEW_OR_CHANGED_UNITS."BldgNum" AS INT) + 100),'.0000') AS building_id
    , NEW_OR_CHANGED_UNITS."description"
    , NEW_OR_CHANGED_UNITS."Condominium"
    , CURRENT_DATE
    , TRUE
    , FALSE
    , CURRENT_DATE
FROM staging_1.NEW_OR_CHANGED_UNITS
JOIN "core"."county_id_mapping_table" 
ON "county_id_mapping_table"."county_parcel_id" = NEW_OR_CHANGED_UNITS."ParcelId"
JOIN "core"."parcel"
ON "parcel"."parcel_id" = "county_id_mapping_table"."parcel_id" 
WHERE "parcel"."current_flag" = True
)
ON CONFLICT (COALESCE("unit_id", 'NULL')
    , COALESCE("building_id", 'NULL')
    , COALESCE("description", 'NULL')
    , "condominium")
    DO UPDATE
SET "current_flag" = TRUE
    , "removed_flag" = FALSE
    , "update_date" = CURRENT_DATE;
	
END;
$$
LANGUAGE plpgsql;
-------------------------
SELECT core.new_unit();