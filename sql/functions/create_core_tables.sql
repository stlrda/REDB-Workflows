----Function for creating all the tables used in core and any constraints/indexes/sequences necessary for them to work------------
CREATE OR REPLACE FUNCTION core.create_core_tables()
RETURNS void AS $$
BEGIN

--Creates table for county IDs----------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.county (
    county_id varchar PRIMARY KEY
    , county_name varchar
    , county_state varchar
    );

--Creates table for neighborhood IDs----------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.neighborhood (
    neighborhood_id SERIAL PRIMARY KEY
    , neighborhood_name varchar CONSTRAINT UC_Neighborhood UNIQUE
    , county_id varchar
    , create_date date
    , current_flag boolean
    , removed_flag boolean
    , etl_job varchar
    , update_date date
    );

--Creates table for address ids which are uniquely assigned via the serial Primary Key "address_id"-------------------------------
CREATE TABLE IF NOT EXISTS core.address (
    address_id SERIAL PRIMARY KEY
    , street_address varchar
    , county_id varchar
    , city varchar
    , state varchar
    , country varchar
    , zip varchar
    , create_date date
    , current_flag boolean
    , removed_flag boolean
    , etl_job varchar
    , update_date date
    );
	
-- Unique Index is necessary to account for potential nulls in address fields.
CREATE UNIQUE INDEX UI_OwnerAddress ON "core"."address"(COALESCE("street_address", 'NULL')
	, COALESCE("city", 'NULL')
    , COALESCE("state", 'NULL')
    , COALESCE("country", 'NULL')
    , COALESCE("zip", 'NULL')
	);

--Creates the table and constraint for Mapping Parcel11 IDs to REDb IDs-----------------------------------------------------------
CREATE TABLE IF NOT EXISTS "core"."county_id_mapping_table" (
	county_id varchar -- county_id
	, parcel_id varchar PRIMARY KEY -- REDB identifier
	, county_parcel_id varchar CONSTRAINT UC_Mapping UNIQUE -- The identifier the county uses
	, county_parcel_id_type varchar -- The name the county uses to refer to their identifier EG:'parcel_11'
	, create_date date
	, current_flag boolean
	, removed_flag boolean
	, etl_job varchar
	, update_date date
	);

CREATE SEQUENCE IF NOT EXISTS core.id_mapping
INCREMENT BY 1 
START 10000001
OWNED BY core.county_id_mapping_table."county_parcel_id";

--Creates the table and index for assigning unique Legal_Entity IDs.-------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.legal_entity (
    legal_entity_id SERIAL PRIMARY KEY
    , legal_entity_address varchar
    , legal_entity_name varchar
    , legal_entity_secondary_name varchar
    , address_id integer
    , create_date date
    , current_flag boolean
    , removed_flag boolean
    , etl_job varchar
    , update_date date
    );

--index necessary to account for potential nulls in fields needed to create legal entity
CREATE UNIQUE INDEX UI_Legal_Entity ON "core"."legal_entity" (COALESCE("legal_entity_address", 'NULL_ADDRESS')
, COALESCE("legal_entity_name", 'NULL_NAME_1')
, COALESCE("legal_entity_secondary_name", 'NULL_NAME_2')
, "address_id");

--Creates the table for assigning unique Parcel IDs.----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "core"."parcel" (
    "parcel_id" varchar -- CCCCCC.PPPPPPPP.000.0000 (county_id.parcel_number.building_number.unit_number)
    , "county_id" varchar -- County_Id 10001 because all the data is coming from one county at the moment but this needs to be more sophisticated down the line
	, "address_id" BIGINT
    , "city_block_number" varchar -- prcl.CityBlock
    , "parcel_number" varchar -- generated with a sequence starting at 10000001
    --, "parcel_taxing_status" varchar -- May be coming from a different table don't know for now.
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

CREATE UNIQUE INDEX UI_Active_Parcel ON "core"."parcel"(parcel_id, current_flag) WHERE current_flag = TRUE;

CREATE UNIQUE INDEX UI_Parcel ON "core"."parcel"(COALESCE("parcel_id", 'NULL')
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
    , COALESCE("gis_owner_code", 'NULL')
    );

--Creates the table for assigning unique Building IDs.--------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "core"."building" (
    "parcel_id" varchar
	, "building_id" varchar -- CCCCCC.PPPPPPPP.BBB.0000 (county_id.parcel_number.building_number.unit_number)
	, "owner_id" varchar
	, "description" varchar
	, "apartment_count" int
	, "create_date" date
	, "current_flag" boolean
	, "removed_flag" boolean
	, "etl_job" varchar
	, "update_date" date
);

CREATE UNIQUE INDEX UI_Active_Building ON "core"."building"(building_id, current_flag) WHERE current_flag = TRUE;

CREATE UNIQUE INDEX UI_Building ON "core"."building"(COALESCE("parcel_id", 'NULL')
	, COALESCE("building_id", 'NULL')
	, COALESCE("owner_id", 'NULL')
	, COALESCE("description", 'NULL')
	, COALESCE("apartment_count", '777')
	);
	
--Creates the table for assigning unique Unit IDs.------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "core"."unit" (
    "unit_id" varchar -- CCCCCC.PPPPPPPP.BBB.UUUU (county_id.parcel_number.building_number.unit_number)
	, "building_id" varchar
	, "description" varchar
	, "condominium" boolean
	, "create_date" date
	, "current_flag" boolean
	, "removed_flag" boolean
	, "etl_job" varchar
	, "update_date" date
);

CREATE UNIQUE INDEX UI_Active_Unit ON "core"."unit"(unit_id, current_flag) WHERE current_flag = TRUE;

CREATE UNIQUE INDEX UI_Unit ON "core"."unit"(COALESCE("unit_id", 'NULL')
	, COALESCE("building_id", 'NULL')
	, COALESCE("description", 'NULL')
	, "condominium"
	);
	
END;
$$
LANGUAGE plpgsql;

SELECT core.create_core_tables()