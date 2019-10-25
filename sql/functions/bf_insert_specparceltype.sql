CREATE OR REPLACE FUNCTION bf_insert_specparceltype()
RETURNS void
AS $$	
    DECLARE
		code RECORD;
	BEGIN
		--put the codes in lookup table
		FOR code in
			SELECT '11111' as county_id
				, "SpecParcelType"
				, 'specparceltype' as code_type
			FROM staging.codes_cdspecparceltype
		LOOP
			EXECUTE
				'SELECT (jfl_insert_county_code_lookup(_county_id := $1, _county_code := $2, _code_type := $3))'
			USING code.county_id, code."SpecParcelType", code.code_type;
		END LOOP;
		--put the codes in spec_parcel_type table
		FOR code in
			SELECT ccl."redb_code", cd."Descr"
			FROM core.county_code_lookup ccl
			JOIN staging.codes_cdspecparceltype cd
				ON ccl."county_code" = cd."SpecParcelType"
			WHERE ccl."code_type" = 'specparceltype' AND ccl."county_id" = '11111'
		LOOP
			EXECUTE
				'INSERT INTO core.special_parcel_type(special_parcel_type_code, special_parcel_type)
				VALUES($1, $2) ON CONFLICT DO NOTHING'
				USING code."redb_code", code."Descr";
		END LOOP;
	END;
$$
LANGUAGE plpgsql