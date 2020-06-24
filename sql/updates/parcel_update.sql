------------------------------VIEW NECESSARY FOR INSERTING NEW PARCELS (potentially used in more places) --------------------------------
CREATE VIEW staging_1.ID_TABLE_VIEW AS
	(
	SELECT "ParcelId", "legal_entity_id"
	FROM (
		SELECT "ParcelId"
            , "OwnerName"
            , "OwnerName2"
            , "address_id"
            , "OwnerAddr"
            , "OwnerCity"
            , "OwnerState"
            , "OwnerCountry"
            , "OwnerZIP" 
		FROM "staging_1"."prcl_test" AS P --TODO this table will need to be changed for prod
		LEFT JOIN "core"."address" AS A
			ON COALESCE("OwnerAddr", ' ') = COALESCE("street_address", ' ')
			AND COALESCE("OwnerCity", ' ') = COALESCE("city", ' ') 
			AND COALESCE("OwnerState", ' ') = COALESCE("state", ' ')
			AND COALESCE("OwnerCountry", ' ') = COALESCE("country", ' ') 
			AND COALESCE("OwnerZIP", ' ') = COALESCE("zip", ' ')
		) qry
	LEFT JOIN "core"."legal_entity"
        ON COALESCE("OwnerName", ' ') = COALESCE("legal_entity_name", ' ')
        AND COALESCE("OwnerName2", ' ') = COALESCE("legal_entity_secondary_name", ' ')
        AND ("legal_entity"."address_id" = "qry"."address_id")
	)
---------------NEW PARCELS (DEPENDANT ON THE ABOVE ID_TABLE_VIEW, COUNTY_ID_MAPPING_TABLE, LEGAL_ENTITY, ADDRESS, & NEIGHBORHOOD BEING UPDATED FIRST)-------------
--TODO references to parcel_test will need to be updated for production
WITH NEW_REDB_IDS AS
	(
	WITH NEW_PARCEL_IDS AS
		(
		SELECT staging_1.prcl_test."ParcelId"
		FROM staging_1.prcl_test
		LEFT JOIN staging_2."prcl_prcl"
			ON staging_1.prcl_test."ParcelId" = staging_2."prcl_prcl"."ParcelId"
		WHERE staging_2."prcl_prcl"."ParcelId" IS NULL
		)
	SELECT DISTINCT "county_id", "parcel_id", "county_parcel_id", "create_date", "current_flag", "removed_flag", "etl_job", "update_date"
	FROM "core"."county_id_mapping_table"
	JOIN NEW_PARCEL_IDS
	ON NEW_PARCEL_IDS."ParcelId" = "county_id_mapping_table"."county_parcel_id"
	)
INSERT INTO "core"."parcel" ("parcel_id"
    , "county_id"
    , "city_block_number"
    , "parcel_number"
    , "owner_id"
    , "description"
    , "frontage_to_street"
    , "land_area"
    , "zoning_class"
    , "ward"
    , "voting_precinct"
    , "inspection_area"
    , "neighborhood_id"
    , "police_district"
    , "census_tract"
    , "asr_neighborhood"
    , "special_parcel_type_code"
    , "sub_parcel_type_code"
    , "gis_city_block"
    , "gis_parcel"
    , "gis_owner_code"
    , "create_date"
	, "current_flag"
    , "removed_flag"
    , "etl_job"
    , "update_date"
    )
(SELECT NEW_REDB_IDS."parcel_id"
    , NEW_REDB_IDS."county_id"
    , "CityBlock"
    , SUBSTRING(NEW_REDB_IDS."parcel_id" FROM 7 FOR 8)
    , ID_TABLE_VIEW."legal_entity_id"
    , CONCAT("LegalDesc1",' ',"LegalDesc2",' ',"LegalDesc3",' ',"LegalDesc4",' ',"LegalDesc5")
    , CAST("Frontage" AS FLOAT)
    , CAST("LandArea" AS INT)
    , "Zoning"
    , "Ward10"
    , "Precinct10"
    , "InspArea10"
    , "neighborhood"."neighborhood_id"
    , "PoliceDist"
    , "CensTract10"
    , "AsrNbrhd"
    , "SpecParcelType"
    , "SubParcelType"
    , "GisCityBLock"
    , "GisParcel"
    , "GisOwnerCode"
    , NEW_REDB_IDS."create_date"
    , NEW_REDB_IDS."current_flag"
    , NEW_REDB_IDS."removed_flag"
    , NEW_REDB_IDS."etl_job"
    , NEW_REDB_IDS."update_date"
FROM "staging_1"."prcl_test"
JOIN staging_1.ID_TABLE_VIEW
ON "prcl_test"."ParcelId" = staging_1.ID_TABLE_VIEW."ParcelId"
JOIN NEW_REDB_IDS
ON "prcl_test"."ParcelId" = NEW_REDB_IDS."county_parcel_id"
JOIN "core"."neighborhood"
ON "prcl_test"."Nbrhd" = "neighborhood"."neighborhood_name"
)

----------------------------------FLAG DEAD PARCELS (DEPENDANT ON MAPPING_TABLE BEING UPDATED FIRST)------------------------------------
WITH REDB_PARCEL_IDS AS
	(
	WITH DEAD_PARCEL_IDS AS
		(
		SELECT staging_2.prcl_prcl."ParcelId"
		FROM staging_2.prcl_prcl
		LEFT JOIN staging_1."prcl_test"
			ON staging_2.prcl_prcl."ParcelId" = staging_1."prcl_test"."ParcelId"
		WHERE staging_1."prcl_test"."ParcelId" IS NULL
		)
	SELECT DISTINCT SUBSTRING("county_id_mapping_table"."parcel_id" FROM 1 FOR 14) AS redb_county_id
	FROM "core"."county_id_mapping_table"
	JOIN DEAD_PARCEL_IDS
	ON DEAD_PARCEL_IDS."ParcelId" = "county_id_mapping_table"."county_parcel_id"
	)
UPDATE "core"."parcel" 
SET "removed_flag" = TRUE,
	"current_flag" = FALSE,
	"update_date" = CURRENT_DATE
FROM REDB_PARCEL_IDS
WHERE "redb_county_id" = SUBSTRING("parcel"."parcel_id" FROM 1 FOR 14);