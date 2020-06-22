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
OWNED BY core.county_id_mapping_table."county_parcel_id"; --Owned By line just causes the sequence to be dropped if the column is dropped 

/*
For each entry in the prcl_prcl table a New Parcel_Id is generated via a sequence and tied to the unique parcel_11 id this
is used in other tables in order to correctly map fields from the sorce data to tables that only have the REDB id listed
County ID and County ID type are hard coded since we are still only using a sinlge county in our table.
*/ 
INSERT INTO "core"."county_id_mapping_table"(
	"county_id"
	, "county_parcel_id"
	, "parcel_id"
	, "county_parcel_id_type"
	)
	SELECT ('10001'
	, "ParcelId"
	, CONCAT("county_id", '.', nextval('core.id_mapping'), '.000.0000')
	, 'parcel_11'
	)
	FROM "staging_2"."prcl_prcl";