CREATE TABLE IF NOT EXISTS "core"."parcel" (
    "parcel_id" PRIMARY KEY,
    "county_id" varchar,
    "city_block_number" varchar,
    "parcel_number" varchar,
    "parcel_taxing_status" varchar,
    "primary_addresses_count" varchar,
    "owner_id" varchar,
    "description" varchar,
    "frontage_to_street" int,
    "land_area" int,
    "zoning_class" varchar,
    "ward" varchar,
    "voting_precinct" varchar,
    "inspection_area" varchar,
    "neighborhood_id" varchar,
    "police_district" varchar,
    "census_tract" varchar,
    "census_block" varchar,
    "asr_neighborhood" varchar,
    "special_parcel_type_code" varchar,
    "sub_parcel_type_code" varchar,
    "gis_city_block" varchar,
    "gis_parcel" varchar,
    "gis_owner_code" varchar,
    "create_date" date,
    "current_flag" boolean,
    "removed_flag" boolean,
    "etl_job" varchar,
    "update_date" date
);

CREATE SEQUENCE IF NOT EXISTS core.parcelID 
INCREMENT BY 1 
START 10000001
OWNED BY core.parceltest.parcel_id;

ALTER SEQUENCE core.parcelid RESTART;

INSERT INTO "core"."parceltest" ("parcel_id", "county_id", "parcel_number") 
SELECT (nextval('core.parcelid')), '10001', "prcl_Prcl"."Parcel" 
FROM "staging_1"."prcl_Prcl";

SELECT "CityBlock", "Parcel", "OwnerCode", "ParcelId", "OwnerName", "OwnerName2", "address_id", "OwnerAddr", "OwnerCity", "OwnerState", "OwnerCountry", "OwnerZIP" 
FROM "staging_1"."prcl_Prcl" AS P
LEFT JOIN "core"."addresstest" AS A
	ON COALESCE("OwnerAddr", ' ') = COALESCE("street_address",' ')
	AND COALESCE("OwnerCity", ' ') = COALESCE("city",' ') 
	AND COALESCE("OwnerState", ' ') = COALESCE("state",' ')
	AND COALESCE("OwnerCountry", ' ') = COALESCE("country",' ') 
	AND COALESCE("OwnerZIP", ' ') = COALESCE("zip",' ')
	ORDER BY address_id;