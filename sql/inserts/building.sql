-- TODO look into wether or not BuildingTable should be a view if it is used in multiple places
-- Joins prcl_bcom & prcl_bres on our inner query to further narrow down the building list
-- Join criteria is the ParcelId field which is constructed from CityBlock, Parcel and OwnerCode on the bcom & bres tables  
---------INSERT NEW BUILDINGS INTO BUILDING TABLE----------------------
CREATE OR REPLACE FUNCTION core.new_building()
RETURNS void AS $$
BEGIN

CREATE OR REPLACE VIEW staging_1.NEW_OR_CHANGED_BUILDINGS AS --Compares CURRENT(staging_1) to PREVIOUS(staging_2) and returns the ParcelID, BldgNum, NbrOfApts & description of building records that are NEW or have had a change in NbrOfApts or Description.
		(
		WITH BUILDING_TABLE AS --selects the records from the CURRENT bldg_com & bldg_res tables that fit our BUILDING_RECORD criteria
			(
			WITH BUILDING_RECORD AS --Selects records from CURRENT.prcl_prcl that meet our building criteria and returns NbrOfApts and description along with ParcelId (Need them to compare CURRENT to PREVIOUS)
				(
				SELECT "ParcelId", "NbrOfApts", CONCAT("LegalDesc1",' ',"LegalDesc2",' ',"LegalDesc3",' ',"LegalDesc4",' ',"LegalDesc5") AS description
				FROM "staging_1"."prcl_prcl"
				WHERE "Parcel" != "GisParcel" AND "OwnerCode" != '8'
				)
			SELECT "ParcelId", "BldgNum", "NbrOfApts", "description"
			FROM BUILDING_RECORD
			JOIN "staging_1"."prcl_bldgcom"
			ON (SELECT core.format_parcelId(prcl_bldgcom."CityBlock", prcl_bldgcom."Parcel", prcl_bldgcom."OwnerCode")) = BUILDING_RECORD."ParcelId"
			UNION ALL
			SELECT "ParcelId", "BldgNum", "NbrOfApts", "description"
			FROM BUILDING_RECORD
			JOIN "staging_1"."prcl_bldgres"
			ON (SELECT core.format_parcelId(prcl_bldgres."CityBlock", prcl_bldgres."Parcel", prcl_bldgres."OwnerCode")) = BUILDING_RECORD."ParcelId"
			)
		SELECT DISTINCT BUILDING_TABLE."ParcelId", BUILDING_TABLE."BldgNum", BUILDING_TABLE."NbrOfApts", BUILDING_TABLE."description"
		FROM BUILDING_TABLE
		LEFT JOIN (SELECT core.format_parcelId(prcl_bldgcom."CityBlock", prcl_bldgcom."Parcel", prcl_bldgcom."OwnerCode") AS "ParcelId"
					, "prcl_bldgcom"."BldgNum"
					, "NbrOfApts"
					, CONCAT("LegalDesc1",' ',"LegalDesc2",' ',"LegalDesc3",' ',"LegalDesc4",' ',"LegalDesc5") AS description
					FROM "staging_2"."prcl_bldgcom"
					JOIN "staging_2"."prcl_prcl"
					ON (SELECT core.format_parcelId(prcl_bldgcom."CityBlock", prcl_bldgcom."Parcel", prcl_bldgcom."OwnerCode")) = "prcl_prcl"."ParcelId"
					UNION ALL
					SELECT core.format_parcelId(prcl_bldgres."CityBlock", prcl_bldgres."Parcel", prcl_bldgres."OwnerCode") AS "ParcelId"
					, "prcl_bldgres"."BldgNum"
					, "NbrOfApts"
					, CONCAT("LegalDesc1",' ',"LegalDesc2",' ',"LegalDesc3",' ',"LegalDesc4",' ',"LegalDesc5") AS description
					FROM "staging_2"."prcl_bldgres"
					JOIN "staging_2"."prcl_prcl"
					ON (SELECT core.format_parcelId(prcl_bldgres."CityBlock", prcl_bldgres."Parcel", prcl_bldgres."OwnerCode")) = "prcl_prcl"."ParcelId"
					) UNION_BLDGS
		ON UNION_BLDGS."ParcelId" = BUILDING_TABLE."ParcelId" 
			AND UNION_BLDGS."BldgNum" = BUILDING_TABLE."BldgNum"
			AND UNION_BLDGS."NbrOfApts" = BUILDING_TABLE."NbrOfApts"
			AND UNION_BLDGS."description" = BUILDING_TABLE."description"
		WHERE UNION_BLDGS."ParcelId" IS NULL
		);
----------------------------------------------
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
	, "apartment_count"
	, "create_date"
	, "current_flag"
	, "removed_flag"
	--, "etl_job"
	, "update_date")
(SELECT "county_id_mapping_table"."parcel_id"
	, CONCAT(SUBSTRING("county_id_mapping_table"."parcel_id" FROM 1 FOR 15), (CAST(NEW_OR_CHANGED_BUILDINGS."BldgNum" AS INT) + 100),'.0000') AS building_id
	, "parcel"."owner_id"
	, NEW_OR_CHANGED_BUILDINGS."description"
	, CAST("NbrOfApts" AS INT)
	, CURRENT_DATE
	, TRUE
	, FALSE
	, CURRENT_DATE
FROM staging_1.NEW_OR_CHANGED_BUILDINGS
JOIN "core"."county_id_mapping_table" 
ON "county_id_mapping_table"."county_parcel_id" = NEW_OR_CHANGED_BUILDINGS."ParcelId"
JOIN "core"."parcel"
ON "parcel"."description" = NEW_OR_CHANGED_BUILDINGS."description"
WHERE "parcel"."current_flag" = True
)
ON CONFLICT (COALESCE("parcel_id", 'NULL')
	, COALESCE("building_id", 'NULL')
	, COALESCE("owner_id", 'NULL')
	, COALESCE("description", 'NULL')
	, COALESCE("apartment_count", '777'))
	DO UPDATE
SET "current_flag" = TRUE
	, "removed_flag" = FALSE
	, "update_date" = CURRENT_DATE;

END;
$$
LANGUAGE plpgsql;
-------------------------
SELECT core.new_building();