CREATE OR REPLACE FUNCTION public.insert_address_bf()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
	DECLARE
		counter integer;
		address RECORD;
		address_id varchar;
	BEGIN
		FOR address in 
			SELECT "OwnerZIP", "OwnerCity", "OwnerState", "OwnerCountry"
			FROM staging.prcl_prcl
		LOOP
		  EXECUTE 
			'SELECT address_id 
			FROM core.address
			WHERE "city" = $2 AND "state" = $3 AND "country" = $4 AND "zip" = $1'
		  		INTO address_id
				USING address."OwnerZIP", address."OwnerCity", address."OwnerState", address."OwnerCountry";
		  RAISE NOTICE '%', address_id;
		  IF address_id is NULL THEN
			--counter = counter + 1;
			EXECUTE 
				E'SELECT current_highest_value
				FROM highest_id
				WHERE id_name = \'address_id\''
				INTO counter;
			IF counter is null THEN
				counter := 1;
			ELSE
				counter := counter + 1;
			END IF;
		  	EXECUTE
				'INSERT INTO core.address(address_id, city, state, country, zip)
				 VALUES($1, $2, $3, $4, $5)'
			USING counter, address."OwnerCity", address."OwnerState", address."OwnerCountry", address."OwnerZIP";
		  END IF;
		END LOOP;
		RETURN counter;
	END;
$function$
;
