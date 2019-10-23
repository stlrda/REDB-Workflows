CREATE OR REPLACE FUNCTION public.jfl_insert_county_code_lookup(_county_id character varying DEFAULT NULL::character varying, _county_code character varying DEFAULT NULL::character varying, _code_type character varying DEFAULT NULL::character varying)
 RETURNS SETOF core.county_code_lookup
 LANGUAGE plpgsql
AS $function$
DECLARE
        v_county_id varchar;
        v_county_code varchar;
        v_code_type varchar;

    BEGIN
        v_county_id := _county_id;
        v_county_code := _county_code;
        v_code_type := _code_type;
        INSERT INTO core.county_code_lookup(county_id, county_code, redb_code, code_type)
         VALUES (v_county_id, v_county_code, DEFAULT, v_code_type) ON CONFLICT DO NOTHING;
        RETURN QUERY SELECT * FROM core.county_code_lookup;
    END;
$function$
;
