CREATE OR REPLACE FUNCTION public.jfl_insert_subparceltype()
RETURNS void
AS $$
    DECLARE
		code RECORD;
	BEGIN
		--put the codes in lookup table
		FOR code in
			SELECT '11111' as county_id
				, "SubParcelType"
				, 'subparceltype' as code_type
			FROM staging.codes_cdsubparceltype
		LOOP
			EXECUTE
				'SELECT (jfl_insert_county_code_lookup(_county_id := $1, _county_code := $2, _code_type := $3))'
			USING code.county_id, code."SubParcelType", code.code_type;
		END LOOP;
		--put the codes in sub_parcel_type table
		FOR code in
			SELECT ccl."redb_code", cd."Descr"
			FROM core.county_code_lookup ccl
			JOIN staging.codes_cdsubparceltype cd
				ON ccl."county_code" = cd."SubParcelType"
			WHERE ccl."code_type" = 'subparceltype' AND ccl."county_id" = '11111'
		LOOP
			EXECUTE
				'INSERT INTO core.sub_parcel_type(sub_parcel_type_code, sub_parcel_type)
				VALUES($1, $2) ON CONFLICT DO NOTHING'
				USING code."redb_code", code."Descr";
		END LOOP;
	END;
$$
LANGUAGE plpgsql