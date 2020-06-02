CREATE TABLE "parcel" (
  "parcel_id" varchar PRIMARY KEY,
  "county_id" varchar,
  "city_block_number" varchar,
  "parcel_number" varchar,
  "parcelt_taxing_status" varchar,
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

CREATE TABLE "building" (
  "building_id" varchar PRIMARY KEY,
  "parcel_id" varchar,
  "owner_id" varchar,
  "description" varchar,
  "apartment_count" int,
  "create_date" date,
  "current_flag" boolean,
  "removed_flag" boolean,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "unit" (
  "unit_id" varchar PRIMARY KEY,
  "building_id" varchar,
  "description" varchar,
  "condominium" boolean,
  "create_date" date,
  "current_flag" boolean,
  "removed_flag" boolean,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "legal_entity" (
  "legal_entity_id" varchar PRIMARY KEY,
  "legal_entity_name" varchar,
  "legal_entity_secondary_name" varchar,
  "address_id" varchar,
  "create_date" date,
  "current_flag" boolean,
  "removed_flag" boolean,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "address" (
  "address_id" varchar PRIMARY KEY,
  "street_number" varchar,
  "street_name_prefix" varchar,
  "street_name" varchar,
  "street_name_suffix" varchar,
  "secondary_designator" varchar,
  "county_id" varchar,
  "city" varchar,
  "state" varchar,
  "country" varchar,
  "zip" varchar,
  "create_date" date,
  "current_flag" boolean,
  "removed_flag" boolean,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "county_id_mapping_table" (
  "county_id" varchar,
  "parcel_id" varchar PRIMARY KEY,
  "county_parcel_id" varchar,
  "county_parcel_id_type" varchar,
  "create_date" date,
  "current_flag" boolean,
  "removed_flag" boolean,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "county" (
  "county_id" varchar PRIMARY KEY,
  "county_name" varchar,
  "county_state" varchar,
  "create_date" date,
  "current_flag" boolean,
  "removed_flag" boolean,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "neighborhood" (
  "neighborhood_id" varchar PRIMARY KEY,
  "neighborhood_name" varchar,
  "county_id" varchar,
  "create_date" date,
  "current_flag" boolean,
  "removed_flag" boolean,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "sub_parcel_type" (
  "sub_parcel_type_code" varchar PRIMARY KEY,
  "sub_parcel_type" varchar,
  "create_date" date,
  "current_flag" boolean,
  "removed_flag" boolean,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "special_parcel_type" (
  "special_parcel_type_code" varchar PRIMARY KEY,
  "special_parcel_type" varchar,
  "create_date" date,
  "current_flag" boolean,
  "removed_flag" boolean,
  "etl_job" varchar,
  "update_date" date
);

ALTER TABLE "parcel" ADD FOREIGN KEY ("county_id") REFERENCES "county" ("county_id");

ALTER TABLE "parcel" ADD FOREIGN KEY ("owner_id") REFERENCES "legal_entity" ("legal_entity_id");

ALTER TABLE "parcel" ADD FOREIGN KEY ("neighborhood_id") REFERENCES "neighborhood" ("neighborhood_id");

ALTER TABLE "parcel" ADD FOREIGN KEY ("special_parcel_type_code") REFERENCES "special_parcel_type" ("special_parcel_type_code");

ALTER TABLE "parcel" ADD FOREIGN KEY ("sub_parcel_type_code") REFERENCES "sub_parcel_type" ("sub_parcel_type_code");

ALTER TABLE "building" ADD FOREIGN KEY ("parcel_id") REFERENCES "parcel" ("parcel_id");

ALTER TABLE "building" ADD FOREIGN KEY ("owner_id") REFERENCES "legal_entity" ("legal_entity_id");

ALTER TABLE "unit" ADD FOREIGN KEY ("building_id") REFERENCES "building" ("building_id");

ALTER TABLE "legal_entity" ADD FOREIGN KEY ("address_id") REFERENCES "address" ("address_id");

ALTER TABLE "address" ADD FOREIGN KEY ("county_id") REFERENCES "county" ("county_id");

ALTER TABLE "county_id_mapping_table" ADD FOREIGN KEY ("county_id") REFERENCES "county" ("county_id");

ALTER TABLE "county_id_mapping_table" ADD FOREIGN KEY ("parcel_id") REFERENCES "parcel" ("parcel_id");

ALTER TABLE "neighborhood" ADD FOREIGN KEY ("county_id") REFERENCES "county" ("county_id");

COMMENT ON COLUMN "parcel"."parcel_id" IS 'REDB identifier, not municipal identifier. CCCCC.PPPPPPPP.000.0000';

COMMENT ON COLUMN "parcel"."parcelt_taxing_status" IS 'maps to Saint Louis City Owner Code';

COMMENT ON COLUMN "parcel"."frontage_to_street" IS 'number of feet that face the street';

COMMENT ON COLUMN "parcel"."land_area" IS 'sq footage';

COMMENT ON COLUMN "building"."building_id" IS 'REDB identifier, not municipal identifier. CCCCC.PPPPPPPP.BBB.0000';

COMMENT ON COLUMN "unit"."unit_id" IS 'REDB identifier, not municipal identifier. CCCCC.PPPPPPPP.BBB.UUUU';

COMMENT ON COLUMN "county_id_mapping_table"."parcel_id" IS 'REDB identifier';

COMMENT ON COLUMN "county_id_mapping_table"."county_parcel_id" IS 'This is the identifier the county uses';

COMMENT ON COLUMN "county_id_mapping_table"."county_parcel_id_type" IS 'The name the county uses to refer to their identifier';