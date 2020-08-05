---------INSERT NEW BUILDINGS INTO BUILDING TABLE----------------------
CREATE OR REPLACE FUNCTION core.new_building()
RETURNS void AS $$
BEGIN

CREATE OR REPLACE VIEW staging_1.UNION_BLDGS AS	
	(
	WITH JOIN_LEGAL_ENTITY_TO_BLDGS_UNION AS
		(
		WITH JOIN_ADDRESS_TO_BLDGS_UNION AS 
			(
			WITH UNION_PREVIOUS_BUILDINGS AS
				(
				SELECT "prcl_bldgall"."ParcelId"
					, "prcl_bldgall"."BldgNum"
					, "NbrOfApts"
					, CONCAT("LegalDesc1",' ',"LegalDesc2",' ',"LegalDesc3",' ',"LegalDesc4",' ',"LegalDesc5") AS description
					, "prcl_bldgall"."BldgUse"
				FROM "staging_2"."prcl_bldgall"
				JOIN "staging_2"."prcl_prcl"
				ON "prcl_bldgall"."ParcelId" = "prcl_prcl"."ParcelId"
				)
			SELECT UPB."ParcelId"
				, UPB."BldgNum"
				, UPB."NbrOfApts"
				, UPB."description"
				, UPB."BldgUse"
				, "OwnerName"
				, "OwnerName2"
				, "OwnerAddr"
				, "OwnerCity"
				, "OwnerState"
				, "OwnerCountry"
				, "OwnerZIP" 
			FROM "staging_2"."prcl_prcl"
			JOIN UNION_PREVIOUS_BUILDINGS AS UPB 
			ON "prcl_prcl"."ParcelId" = UPB."ParcelId"
			)
		SELECT DISTINCT "ParcelId"
			, "BldgNum"
			, "NbrOfApts"
			, "description"
			, "BldgUse"
			, "OwnerName"
			, "OwnerName2"
			, "OwnerAddr"
			, "address"."address_id"
		FROM JOIN_ADDRESS_TO_BLDGS_UNION AS JBU
		JOIN "core"."address"
		ON CONCAT(JBU."OwnerAddr", JBU."OwnerCity", JBU."OwnerState", JBU."OwnerCountry", JBU."OwnerZIP") 
			= CONCAT("street_address", "city", "state", "country", "zip")
		)
	SELECT DISTINCT "ParcelId"
		, "legal_entity_id" AS owner_id
		, "BldgNum"
		, "NbrOfApts"
		, "description"
		, "BldgUse"
	FROM JOIN_LEGAL_ENTITY_TO_BLDGS_UNION AS JLE
	JOIN "core"."legal_entity" CLE
	ON CONCAT(JLE."OwnerAddr", JLE."OwnerName", JLE."OwnerName2", JLE."address_id") 
			= CONCAT(CLE."legal_entity_address", CLE."legal_entity_name", CLE."legal_entity_secondary_name", CLE."address_id")
	);

CREATE OR REPLACE VIEW staging_1.NEW_OR_CHANGED_BUILDINGS AS --Compares CURRENT(staging_1) to PREVIOUS(staging_2) and returns the ParcelID, BldgNum, NbrOfApts & description of building records that are NEW or have had a change in NbrOfApts or Description.
	(
    WITH BUILDING_TABLE AS --selects the records from the CURRENT bldg_com & bldg_res tables that fit our BUILDING_RECORD criteria
		(
		WITH ADD_OWNER_ID AS -- adds owner_id from the core.parcel to track if it has changed in core.building
			(			
			WITH BUILDING_RECORD AS --Selects records from CURRENT.prcl_prcl that meet our building criteria and returns NbrOfApts and description along with ParcelId (Need them to compare CURRENT to PREVIOUS)
				(
				SELECT "ParcelId"
					, "NbrOfApts"
					, CONCAT("LegalDesc1",' ',"LegalDesc2",' ',"LegalDesc3",' ',"LegalDesc4",' ',"LegalDesc5") AS description
				FROM "staging_1"."prcl_prcl"
				WHERE "Parcel" != "GisParcel" AND "OwnerCode" != '8'
				)				
			SELECT BUILDING_RECORD."ParcelId"
				, "BldgNum"
				, "NbrOfApts"
				, "description"
				, "BldgUse"
			FROM BUILDING_RECORD
			JOIN "staging_1"."prcl_bldgall"
			ON "prcl_bldgall"."ParcelId" = BUILDING_RECORD."ParcelId"
			)
		SELECT DISTINCT ADD_OWNER_ID."ParcelId"
			, "parcel"."owner_id"
			, ADD_OWNER_ID."BldgNum"
			, ADD_OWNER_ID."NbrOfApts"
			, ADD_OWNER_ID."description"
			, ADD_OWNER_ID."BldgUse"
		FROM ADD_OWNER_ID
		JOIN "core"."county_id_mapping_table" 
		ON "county_id_mapping_table"."county_parcel_id" = ADD_OWNER_ID."ParcelId"
		JOIN "core"."parcel"
		ON "parcel"."parcel_id" = "county_id_mapping_table"."parcel_id" 
		WHERE "parcel"."current_flag" = True
	)
	SELECT DISTINCT BUILDING_TABLE."ParcelId"
		, BUILDING_TABLE."owner_id"
		, BUILDING_TABLE."BldgNum"
		, BUILDING_TABLE."NbrOfApts"
		, BUILDING_TABLE."description"
		, BUILDING_TABLE."BldgUse"
    FROM BUILDING_TABLE
    LEFT JOIN staging_1.UNION_BLDGS
    ON UNION_BLDGS."ParcelId" = BUILDING_TABLE."ParcelId"
		AND UNION_BLDGS."owner_id" = CAST(BUILDING_TABLE."owner_id" AS INT)
		AND UNION_BLDGS."BldgNum" = BUILDING_TABLE."BldgNum"
        AND UNION_BLDGS."NbrOfApts" = BUILDING_TABLE."NbrOfApts"
        AND UNION_BLDGS."description" = BUILDING_TABLE."description"
		AND COALESCE(UNION_BLDGS."BldgUse", 'NULL') = COALESCE(BUILDING_TABLE."BldgUse", 'NULL')
	WHERE UNION_BLDGS."ParcelId" IS NULL
	);
	
---------------------------------------------
UPDATE "core"."building"
SET "update_date" = (CASE
                        WHEN "building"."current_flag" = TRUE 
                        THEN CURRENT_DATE
                        ELSE "building"."update_date"
                    END)
    , "current_flag" = FALSE
FROM staging_1.NEW_OR_CHANGED_BUILDINGS, "core"."county_id_mapping_table"
WHERE "building"."building_id" = CONCAT(SUBSTRING("county_id_mapping_table"."parcel_id" FROM 1 FOR 15), (CAST(NEW_OR_CHANGED_BUILDINGS."BldgNum" AS INT) + 100),'.0000')
    AND "county_id_mapping_table"."county_parcel_id" = NEW_OR_CHANGED_BUILDINGS."ParcelId";

----------------------------------------------
INSERT INTO "core"."building" ("parcel_id"
    , "building_id"
    , "owner_id"
    , "description"
	, "building_use"
    , "apartment_count"
    , "create_date"
    , "current_flag"
    --, "etl_job"
    , "update_date")
(SELECT DISTINCT "county_id_mapping_table"."parcel_id"
    , CONCAT(SUBSTRING("county_id_mapping_table"."parcel_id" FROM 1 FOR 15), (CAST(NEW_OR_CHANGED_BUILDINGS."BldgNum" AS INT) + 100),'.0000') AS building_id
    , "parcel"."owner_id"
    , NEW_OR_CHANGED_BUILDINGS."description"
	, NEW_OR_CHANGED_BUILDINGS."BldgUse"
    , CAST("NbrOfApts" AS INT)
    , CURRENT_DATE
    , TRUE
    , CURRENT_DATE
FROM staging_1.NEW_OR_CHANGED_BUILDINGS
JOIN "core"."county_id_mapping_table" 
ON "county_id_mapping_table"."county_parcel_id" = NEW_OR_CHANGED_BUILDINGS."ParcelId"
JOIN "core"."parcel"
ON "parcel"."parcel_id" = "county_id_mapping_table"."parcel_id" 
WHERE "parcel"."current_flag" = True
)
ON CONFLICT (COALESCE("parcel_id", 'NULL')
    , COALESCE("building_id", 'NULL')
    , COALESCE("owner_id", 'NULL')
    , COALESCE("description", 'NULL')
	, COALESCE("building_use", 'NULL')
	, COALESCE("apartment_count", '777'))
DO UPDATE
SET "current_flag" = TRUE
    , "update_date" = CURRENT_DATE;

END;
$$
LANGUAGE plpgsql;
-------------------------
SELECT core.new_building();