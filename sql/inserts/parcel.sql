CREATE TABLE IF NOT EXISTS "core"."parcel" (
    "parcel_id" varchar PRIMARY KEY -- CCCCCC.PPPPPPPP.000.0000 (county_id.parcel_number.building_number.unit_number)[Building & unit will always be .000.0000 on this table] 
    , "county_id" varchar -- County_Id 10001 because all the data is coming from one county at the moment but this needs to be more sophisticated down the line
    , "city_block_number" varchar -- prcl.CityBlock
    , "parcel_number" varchar -- generated with a sequence starting at 10000001
    , "parcel_taxing_status" varchar -- May be coming from a different table don't know for now.
    --, "primary_addresses_count" varchar -- DROP
    , "owner_id" varchar -- core.legal_entity.legal_entity_id
    , "description" varchar -- CONCAT(prcl.LegalDesc1,' ',prcl.LegalDesc2,' ',prcl.LegalDesc3,' ',prcl.LegalDesc4,' ',prcl.LegalDesc5)
    , "frontage_to_street" int -- prcl.Frontage
    , "land_area" int -- prcl.LandArea
    , "zoning_class" varchar -- prcl.Zoning
    , "ward" varchar -- prcl.Ward10
    , "voting_precinct" varchar -- prcl.Precinct10
    , "inspection_area" varchar -- prcl.InspArea10
    , "neighborhood_id" varchar -- core.neighborhood.neighborhood_id
    , "police_district" varchar -- prcl.PoliceDist
    , "census_tract" varchar -- prcl.CensTract10
    --, "census_block" varchar -- DROP
    , "asr_neighborhood" varchar -- prcl.AsrNbrhd
    , "special_parcel_type_code" varchar -- prcl.SpecParcelType
    , "sub_parcel_type_code" varchar -- prcl.SubParcelType
    , "gis_city_block" varchar -- prcl.GisCityBLock (That's Capital BL in BLock because the city data sucks)
    , "gis_parcel" varchar --prcl.GisParcel
    , "gis_owner_code" varchar --prcl.GisOwnerCode
    , "create_date" date  -- NYI
    , "current_flag" boolean -- NYI
    , "removed_flag" boolean -- NYI
    , "etl_job" varchar -- NYI
    , "update_date" date -- NYI
);

----------------------------New Version that uses the lookup table instead of sequence to get the ID------------------------
WITH ID_Table AS
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
		FROM "staging_2"."prcl_prcl" AS P
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
INSERT INTO "core"."parcel" ("parcel_id"
    , "county_id"
    , "city_block_number"
    , "parcel_number"
    --, "parcel_taxing_status"
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
    --, "create_date"
    --, "create_flag"
    --, "removed_flag"
    --, "etl_job"
    --, "update_date"
    )
(SELECT CONCAT('10001.',"county_id_mapping_table"."parcel_id",'.000.0000')
    , '10001'
    , "CityBlock"
    , "county_id_mapping_table"."parcel_id"
    --, "parcel_taxing_status"
    , ID_Table."legal_entity_id"
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
    --, "create_date"
    --, "create_flag"
    --, "removed_flag"
    --, "etl_job"
    --, "update_date"
FROM "staging_2"."prcl_prcl"
JOIN ID_Table 
ON "prcl_prcl"."ParcelId" = ID_Table."ParcelId"
JOIN "core"."county_id_mapping_table"
ON "prcl_prcl"."ParcelId" = "county_id_mapping_table"."county_parcel_id"
JOIN "core"."neighborhood"
ON "prcl_prcl"."Nbrhd" = "neighborhood"."neighborhood_name"
);