CREATE OR REPLACE FUNCTION public.create_parcel11_dwp(cityblock_var character varying, parcel_var character varying, ownercode_var character varying)
	RETURNS varchar
	LANGUAGE plpgsql
AS $function$
	BEGIN
	return replace(replace(concat(to_char(cityblock_var::float8,'0000.00'),to_char(parcel_var::int8,'0000'),ownercode_var),'.',''),' ','');
	END;
$function$
;
