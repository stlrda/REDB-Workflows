CREATE OR REPLACE FUNCTION public.insert_legal_entity_bf()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
	DECLARE
		counter integer;
		legal_entity RECORD;
		legal_entity_id varchar;
		address_id varchar;
	BEGIN
		FOR legal_entity in 
			SELECT "OwnerName", "OwnerName2", "OwnerZIP", "OwnerCity", "OwnerState", "OwnerCountry"
			FROM staging.prcl_prcl
		LOOP
		  EXECUTE 
		  	--check if key fields already exist
			'SELECT legal_entity_id 
			FROM core.legal_entity
			WHERE "legal_entity_name" = $1 AND "legal_entity_secondary_name" = $2'
		  		INTO legal_entity_id
				USING legal_entity."OwnerName", legal_entity."OwnerName2";
		  RAISE NOTICE '%', legal_entity_id;
		  IF legal_entity_id is NULL THEN
		  	--fetch address_id for entity
			EXECUTE 
			'SELECT address_id 
			FROM core.address
			WHERE "city" = $2 AND "state" = $3 AND "country" = $4 AND "zip" = $1'
		  		INTO address_id
				USING legal_entity."OwnerZIP", legal_entity."OwnerCity", legal_entity."OwnerState", legal_entity."OwnerCountry";
			--counter = counter + 1;
			EXECUTE 
				E'SELECT current_highest_value
				FROM core.highest_id
				WHERE id_name = \'legal_entity_id\''
				INTO counter;
			IF counter is null THEN
				counter := 1;
			ELSE
				counter := counter + 1;
			END IF;
		  	EXECUTE
				'INSERT INTO core.legal_entity(legal_entity_id, legal_entity_name, legal_entity_secondary_name, address_id)
				 VALUES($1, $2, $3, $4)'
			USING counter, legal_entity."OwnerName", legal_entity."OwnerName2", address_id;
		  END IF;
		END LOOP;
		RETURN counter;
	END;

$function$
;
