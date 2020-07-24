CREATE OR REPLACE FUNCTION core.create_current_views()
RETURNS void AS $$
BEGIN

CREATE OR REPLACE VIEW "core".current_neighborhood AS
(
	SELECT "neighborhood_id"
		, "neighborhood_name"
		, "county_id"
	FROM "core"."neighborhood"
	WHERE "current_flag" = TRUE
);

CREATE OR REPLACE VIEW "core".current_address AS
(
	SELECT "address_id"
		, "street_address"
		, "county_id"
		, "city"
		, "state"
		, "country"
		, "zip"
	FROM "core"."address"
	WHERE "current_flag" = TRUE
);

CREATE OR REPLACE VIEW "core".current_county_id_mapping AS
(
	SELECT "county_id"
		, "parcel_id"
		, "county_parcel_id"
		, "county_parcel_id_type"
	FROM "core"."county_id_mapping_table"
	WHERE "current_flag" = TRUE
);

CREATE OR REPLACE VIEW "core".current_legal_entity AS
(
	SELECT "legal_entity_id"
		, "legal_entity_address"
		, "legal_entity_name"
		, "legal_entity_secondary_name"
		, "address_id"
	FROM "core"."legal_entity"
	WHERE "current_flag" = TRUE
);

CREATE OR REPLACE VIEW "core".current_parcel AS
(
	SELECT parcel_id
		, county_id
		, address_id
		, city_block_number
		, parcel_number
		, owner_id
		, description
		, frontage_to_street
		, land_area
		, zoning_class
		, ward
		, voting_precinct
		, inspection_area
		, neighborhood_id
		, police_district
		, census_tract
		, asr_neighborhood
		, special_parcel_type_code
		, sub_parcel_type_code
		, gis_city_block
		, gis_parcel
		, gis_owner_code
	FROM core.parcel
	WHERE "current_flag" = TRUE
);

CREATE OR REPLACE VIEW "core".current_building AS
(
	SELECT "parcel_id"
		, "building_id"
		, "owner_id"
		, "description"
		, "building_use"
		, "apartment_count"
	FROM "core"."building"
	WHERE "current_flag" = TRUE
);

CREATE OR REPLACE VIEW "core".current_unit AS
(
	SELECT "building_id"
		, "unit_id"
		, "description"
		, "condominium"
	FROM "core"."unit"
	WHERE "current_flag" = TRUE
);

END;
$$
LANGUAGE plpgsql;