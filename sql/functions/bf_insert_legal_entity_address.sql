CREATE OR REPLACE FUNCTION insert_legal_entity_address_bf()
RETURNS VOID
AS $$
    DECLARE
		counter integer;
		address RECORD;
		address_id varchar;
	BEGIN
		FOR address in 
			SELECT "OwnerZIP", "OwnerCity", "OwnerState", "OwnerCountry"
			FROM staging.prcl_prcl
		LOOP
		-- looks for existing identical record and fetches that id
		  EXECUTE 
			'SELECT address_id 
			FROM core.address
			WHERE "city" = $2 AND "state" = $3 AND "country" = $4 AND "zip" = $1 AND "street_number" IS NULL AND "street_name_prefix" IS NULL AND "street_name" IS NULL
			  AND "street_name_suffix" IS NULL AND "secondary_designator" IS NULL AND "county_id" IS NULL'
		  		INTO address_id
				USING address."OwnerZIP", address."OwnerCity", address."OwnerState", address."OwnerCountry";
		  RAISE NOTICE 'current address_id: %', address_id;
		  IF address_id is NULL THEN
		  	-- hash new id from known fields being input
			address_id = md5(concat('city:', address."OwnerCity", '|state:', address."OwnerState", '|country:', address."OwnerCountry", '|zip:', address."OwnerZIP"));
			RAISE NOTICE 'New address_id is: %', address_id;
		  	EXECUTE
				'INSERT INTO core.address(address_id, city, state, country, zip)
				 VALUES($1, $2, $3, $4, $5)'
			USING address_id, address."OwnerCity", address."OwnerState", address."OwnerCountry", address."OwnerZIP";
		  END IF;
		END LOOP;
	END;

$$
LANGUAGE plpgsql