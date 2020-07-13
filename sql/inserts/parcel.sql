CREATE OR REPLACE FUNCTION core.new_parcel()
RETURNS void AS $$
BEGIN

------------------------------VIEW NECESSARY FOR INSERTING NEW PARCELS--------------------------------
CREATE OR REPLACE VIEW staging_1.ID_TABLE_VIEW AS
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
		FROM "staging_1"."prcl_prcl" AS P
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
	);

----------------------ADDITIONAL VIEW NECESSARY FOR INSERTING NEW PARCELS AND DETECTING CHANGES IN EXISTING ONES--------------------------------
CREATE OR REPLACE VIEW staging_1.NEW_REDB_IDS AS
    (
    WITH NEW_PARCEL_IDS AS
        (
            SELECT CURRENT_WEEK."ParcelId"
            FROM "staging_1"."prcl_prcl" AS CURRENT_WEEK
            LEFT JOIN "staging_2"."prcl_prcl" AS PREVIOUS_WEEK
            ON (CURRENT_WEEK."ParcelId" = PREVIOUS_WEEK."ParcelId"
                --Used for legal_entity Table
                AND COALESCE(CURRENT_WEEK."OwnerName", 'NULL') = COALESCE(PREVIOUS_WEEK."OwnerName", 'NULL')
                AND COALESCE(CURRENT_WEEK."OwnerName2", 'NULL') = COALESCE(PREVIOUS_WEEK."OwnerName2", 'NULL')
                --Used for address Table
                AND COALESCE(CURRENT_WEEK."OwnerAddr", 'NULL') = COALESCE(PREVIOUS_WEEK."OwnerAddr", 'NULL')
                AND COALESCE(CURRENT_WEEK."OwnerCity", 'NULL') = COALESCE(PREVIOUS_WEEK."OwnerCity", 'NULL')
                AND COALESCE(CURRENT_WEEK."OwnerCity", 'NULL') = COALESCE(PREVIOUS_WEEK."OwnerCity", 'NULL')
                AND COALESCE(CURRENT_WEEK."OwnerState", 'NULL') = COALESCE(PREVIOUS_WEEK."OwnerState", 'NULL')
                AND COALESCE(CURRENT_WEEK."OwnerCountry", 'NULL') = COALESCE(PREVIOUS_WEEK."OwnerCountry", 'NULL')
                AND COALESCE(CURRENT_WEEK."OwnerZIP", 'NULL') = COALESCE(PREVIOUS_WEEK."OwnerZIP", 'NULL')
                ---- Parcel Addresses
                AND COALESCE(CURRENT_WEEK."LowAddrNum", 'NULL') = COALESCE(PREVIOUS_WEEK."LowAddrNum", 'NULL')
                AND COALESCE(CURRENT_WEEK."HighAddrNum", 'NULL') = COALESCE(PREVIOUS_WEEK."HighAddrNum", 'NULL')
                AND COALESCE(CURRENT_WEEK."StPreDir", 'NULL') = COALESCE(PREVIOUS_WEEK."StPreDir", 'NULL')
                AND COALESCE(CURRENT_WEEK."StName", 'NULL') = COALESCE(PREVIOUS_WEEK."StName", 'NULL')
                AND COALESCE(CURRENT_WEEK."StType", 'NULL') = COALESCE(PREVIOUS_WEEK."StType", 'NULL')
                AND COALESCE(CURRENT_WEEK."LowAddrSuf", 'NULL') = COALESCE(PREVIOUS_WEEK."LowAddrSuf", 'NULL')
                AND COALESCE(CURRENT_WEEK."HighAddrSuf", 'NULL') = COALESCE(PREVIOUS_WEEK."HighAddrSuf", 'NULL')
                AND COALESCE(CURRENT_WEEK."ZIP", 'NULL') = COALESCE(PREVIOUS_WEEK."ZIP", 'NULL')
                --Used for parcel Table
                AND COALESCE(CURRENT_WEEK."CityBlock", 'NULL') = COALESCE(PREVIOUS_WEEK."CityBlock", 'NULL')
                AND COALESCE(CURRENT_WEEK."LegalDesc1", 'NULL') = COALESCE(PREVIOUS_WEEK."LegalDesc1", 'NULL')
                AND COALESCE(CURRENT_WEEK."LegalDesc2", 'NULL') = COALESCE(PREVIOUS_WEEK."LegalDesc2", 'NULL') 
                AND COALESCE(CURRENT_WEEK."LegalDesc3", 'NULL') = COALESCE(PREVIOUS_WEEK."LegalDesc3", 'NULL') 
                AND COALESCE(CURRENT_WEEK."LegalDesc4", 'NULL') = COALESCE(PREVIOUS_WEEK."LegalDesc4", 'NULL')  
                AND COALESCE(CURRENT_WEEK."LegalDesc5", 'NULL') = COALESCE(PREVIOUS_WEEK."LegalDesc5", 'NULL')  
                AND COALESCE(CURRENT_WEEK."Frontage", 'NULL') = COALESCE(PREVIOUS_WEEK."Frontage", 'NULL')
                AND COALESCE(CURRENT_WEEK."LandArea", 'NULL') = COALESCE(PREVIOUS_WEEK."LandArea", 'NULL')
                AND COALESCE(CURRENT_WEEK."Zoning", 'NULL') = COALESCE(PREVIOUS_WEEK."Zoning", 'NULL')
                AND COALESCE(CURRENT_WEEK."Ward10", 'NULL') = COALESCE(PREVIOUS_WEEK."Ward10", 'NULL')
                AND COALESCE(CURRENT_WEEK."Precinct10", 'NULL') = COALESCE(PREVIOUS_WEEK."Precinct10", 'NULL')
                AND COALESCE(CURRENT_WEEK."InspArea10", 'NULL') = COALESCE(PREVIOUS_WEEK."InspArea10", 'NULL')
                AND COALESCE(CURRENT_WEEK."PoliceDist", 'NULL') = COALESCE(PREVIOUS_WEEK."PoliceDist", 'NULL')
                AND COALESCE(CURRENT_WEEK."CensTract10", 'NULL') = COALESCE(PREVIOUS_WEEK."CensTract10", 'NULL')
                AND COALESCE(CURRENT_WEEK."AsrNbrhd", 'NULL') = COALESCE(PREVIOUS_WEEK."AsrNbrhd", 'NULL')
                AND COALESCE(CURRENT_WEEK."SpecParcelType", 'NULL') = COALESCE(PREVIOUS_WEEK."SpecParcelType", 'NULL')
                AND COALESCE(CURRENT_WEEK."SubParcelType", 'NULL') = COALESCE(PREVIOUS_WEEK."SubParcelType", 'NULL')
                AND COALESCE(CURRENT_WEEK."GisCityBLock", 'NULL') = COALESCE(PREVIOUS_WEEK."GisCityBLock", 'NULL')
                AND COALESCE(CURRENT_WEEK."GisParcel", 'NULL') = COALESCE(PREVIOUS_WEEK."GisParcel", 'NULL')
                AND COALESCE(CURRENT_WEEK."GisOwnerCode", 'NULL') = COALESCE(PREVIOUS_WEEK."GisOwnerCode", 'NULL')
                )
            WHERE PREVIOUS_WEEK."ParcelId" IS NULL
        )
    SELECT DISTINCT "county_id", "parcel_id", "county_parcel_id", "create_date", "current_flag", "removed_flag", "etl_job", "update_date"
    FROM "core"."county_id_mapping_table"
    JOIN NEW_PARCEL_IDS
    ON NEW_PARCEL_IDS."ParcelId" = "county_id_mapping_table"."county_parcel_id"
    );

CREATE OR REPLACE VIEW staging_1.parcel_addresses AS
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
        FROM "staging_1"."prcl_prcl" AS P
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
    );

--Update Current_flag & Update date on existing parcels if a new version is detected------------
UPDATE "core"."parcel"
SET "update_date" = (CASE
                        WHEN "parcel"."current_flag" = TRUE 
                        THEN CURRENT_DATE
                        ELSE "parcel"."update_date"
                    END)
    , "current_flag" = FALSE
FROM staging_1.NEW_REDB_IDS
WHERE "parcel"."parcel_id" = NEW_REDB_IDS."parcel_id";

--Insert New parcels and Updated versions of existing parcels-----------------------------------
INSERT INTO "core"."parcel" ("parcel_id"
    , "county_id"
    , "address_id"
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
(SELECT DISTINCT NEW_REDB_IDS."parcel_id"
    , NEW_REDB_IDS."county_id"
    , ca."address_id"
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
    , TRUE
    , NEW_REDB_IDS."removed_flag"
    , NEW_REDB_IDS."etl_job"
    , NEW_REDB_IDS."update_date"
FROM "staging_1"."prcl_prcl" pp
JOIN staging_1.ID_TABLE_VIEW
ON pp."ParcelId" = staging_1.ID_TABLE_VIEW."ParcelId"
JOIN staging_1.NEW_REDB_IDS
ON pp."ParcelId" = staging_1.NEW_REDB_IDS."county_parcel_id"
JOIN "core"."neighborhood"
ON pp."Nbrhd" = "neighborhood"."neighborhood_name"
JOIN "core"."address" ca
ON (SELECT  * FROM core.format_parcel_address(pp)) = ca."street_address"
    AND ca.zip  = pp."ZIP"
    AND ca.city = ''
    AND ca.state = 'MO'
    AND ca.country = 'USA'
)
ON CONFLICT (COALESCE("parcel_id", 'NULL')
    , COALESCE("county_id", 'NULL')
    , COALESCE("city_block_number", 'NULL')
    , COALESCE("parcel_number", 'NULL')
    , COALESCE("owner_id", 'NULL')
    , COALESCE("description", 'NULL')
    , COALESCE("frontage_to_street", 666)
    , COALESCE("land_area", 666)
    , COALESCE("zoning_class", 'NULL')
    , COALESCE("ward", 'NULL')
    , COALESCE("voting_precinct", 'NULL')
    , COALESCE("inspection_area", 'NULL')
    , COALESCE("neighborhood_id", 'NULL')
    , COALESCE("police_district", 'NULL')
    , COALESCE("census_tract", 'NULL')
    , COALESCE("asr_neighborhood", 'NULL')
    , COALESCE("special_parcel_type_code", 'NULL')
    , COALESCE("sub_parcel_type_code", 'NULL')
    , COALESCE("gis_city_block", 'NULL')
    , COALESCE("gis_parcel", 'NULL')
    , COALESCE("gis_owner_code", 'NULL'))
    DO UPDATE
SET "current_flag" = TRUE
    , "removed_flag" = FALSE
    , "update_date" = CURRENT_DATE;

END;
$$
LANGUAGE plpgsql;
-------------------------
SELECT core.new_parcel();