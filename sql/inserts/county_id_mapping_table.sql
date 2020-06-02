CREATE TABLE IF NOT EXISTS "core"."county_id_mapping_table" (
	county_id varchar -- county_id
	, parcel_id varchar -- REDB identifier
	, county_parcel_id varchar -- The identifier the county uses
	, county_parcel_id_type varchar -- The name the county uses to refer to their identifier EG:'parcel_11'
	, create_date date
	, current_flag boolean
	, removed_flag boolean
	, etl_job varchar
	, update_date date
)

-- sequence for creating a unique REDB identifier based on county identifier
CREATE SEQUENCE IF NOT EXISTS core.id_mapping
INCREMENT BY 1 
START 10000001
OWNED BY core.county_id_mapping_table."county_parcel_id";

INSERT INTO "core"."county_id_mapping_table" ("county_id", "county_parcel_id", "parcel_id", "county_parcel_id_type")
	SELECT '10001', "ParcelId", CONCAT("county_id", '.', nextval('core.id_mapping'), '.000.0000'), 'parcel_11'
	FROM "staging_2"."prcl_prcl";