CREATE TABLE IF NOT EXISTS core.county (
    county_id varchar PRIMARY KEY
    , county_name varchar
    , county_state varchar
    );

CREATE TABLE IF NOT EXISTS core.neighborhood (
    neighborhood_id SERIAL PRIMARY KEY
    , neighborhood_name varchar UNIQUE
    , county_id varchar
    , create_date date
    , current_flag boolean
    , removed_flag boolean
    , etl_job varchar
    , update_date date
    );

-- Creates table for address ids which are uniquely assigned via the serial Primary Key "address_id"
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

CREATE TABLE IF NOT EXISTS "core"."county_id_mapping_table" (
	county_id varchar -- county_id
	, parcel_id varchar PRIMARY KEY -- REDB identifier
	, county_parcel_id varchar unique -- The identifier the county uses
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
OWNED BY core.county_id_mapping_table."county_parcel_id"; --Owned By line just causes the sequence to be dropped if the column is dropped 

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

CREATE TABLE IF NOT EXISTS "core"."parcel" (
    "parcel_id" varchar PRIMARY KEY -- CCCCCC.PPPPPPPP.000.0000 (county_id.parcel_number.building_number.unit_number)
    , "county_id" varchar -- County_Id 10001 because all the data is coming from one county at the moment but this needs to be more sophisticated down the line
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

-- Creates the table for assigning unique Building IDs.
CREATE TABLE IF NOT EXISTS "core"."building" (
    "parcel_id" varchar
	, "building_id" varchar PRIMARY KEY -- CCCCCC.PPPPPPPP.BBB.0000 (county_id.parcel_number.building_number.unit_number)
	, "owner_id" varchar
	, "description" varchar
	, "apartment_count" int
	, "create_date" date
	, "current_flag" boolean
	, "removed_flag" boolean
	, "etl_job" varchar
	, "update_date" date
);

CREATE TABLE IF NOT EXISTS "core"."unit" (
    "unit_id" varchar PRIMARY KEY -- CCCCCC.PPPPPPPP.BBB.UUUU (county_id.parcel_number.building_number.unit_number)
	, "building_id" varchar
	, "description" varchar
	, "condominium" boolean
	, "create_date" date
	, "current_flag" boolean
	, "removed_flag" boolean
	, "etl_job" varchar
	, "update_date" date
);