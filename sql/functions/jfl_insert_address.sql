CREATE OR REPLACE FUNCTION public.jfl_insert_address(_street_number character varying DEFAULT NULL::character varying, _street_name_prefix character varying DEFAULT NULL::character varying, _street_name character varying DEFAULT NULL::character varying, _street_name_suffix character varying DEFAULT NULL::character varying, _secondary_designator character varying DEFAULT NULL::character varying, _county_id character varying DEFAULT NULL::character varying, _city character varying DEFAULT NULL::character varying, _state character varying DEFAULT NULL::character varying, _country character varying DEFAULT NULL::character varying, _zip character varying DEFAULT NULL::character varying)
 RETURNS SETOF core.address
 LANGUAGE plpgsql
AS $function$
DECLARE
        v_street_number varchar;
        v_street_name_prefix varchar;
        v_street_name varchar;
        v_street_name_suffix varchar;
        v_secondary_designator varchar;
        v_county_id varchar;
        v_city varchar;
        v_state varchar;
        v_country varchar;
        v_zip varchar;
        v_address_id varchar;
    BEGIN
        v_street_number := _street_number;
        v_street_name_prefix := _street_name_prefix;
        v_street_name := _street_name;
        v_street_name_suffix := _street_name_suffix;
        v_secondary_designator := _secondary_designator;
        v_county_id := _county_id;
        v_city := _city;
        v_state := _state;
        v_country := _country;
        v_zip := _zip;
        v_address_id := md5(concat(_city,_state,_country,_zip));
        INSERT INTO core.address(address_id, street_number, street_name_prefix, street_name, street_name_suffix, secondary_designator, county_id, city, state, country, zip, create_date, current_flag, removed_flag, etl_job, update_date) VALUES
            (v_address_id, v_street_number, v_street_name_prefix, v_street_name, v_street_name_suffix, v_secondary_designator, v_county_id, v_city, v_state, v_country, v_zip, current_date, TRUE, FALSE, null, current_date)
            ON CONFLICT DO NOTHING;
        RETURN QUERY SELECT * FROM core.address;
    END;
$function$
;
