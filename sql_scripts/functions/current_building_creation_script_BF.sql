CREATE OR REPLACE FUNCTION public.insert_building_bf()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
	DECLARE
		building record;
		parcel_id varchar;
		county_id varchar;
		parcel_num varchar;
		building_num integer;
		building_id varchar;
		legal_entity_id varchar;
		check_parcel varchar;
	BEGIN
		FOR building IN
		--Identifies rows for redb.building table
		--Need to identify building type
			SELECT 	"CityBlock"
					, "Parcel"
					, "OwnerCode"
					, "OwnerName"
					, "OwnerName2"
					, concat("LegalDesc1", "LegalDesc2", "LegalDesc3", "LegalDesc4", "LegalDesc5") Description
					, "NbrOfApts"
					, "Handle"
			FROM staging.prcl_prcl
			WHERE "Parcel"::integer > 8000
			UNION
			SELECT 	P."CityBlock"
					, P."Parcel"
					, P."OwnerCode"
					, P."OwnerName"
					, P."OwnerName2"
					, concat(P."LegalDesc1", P."LegalDesc2", P."LegalDesc3", P."LegalDesc4", P."LegalDesc5") Description
					, P."NbrOfApts"
					, P."Handle"
			FROM staging.prcl_prcl AS P
				JOIN staging.prcl_bldgcom C
				   ON P."CityBlock" = C."CityBlock" AND P."Parcel" = C."Parcel" AND P."OwnerCode" = C."OwnerCode"
			WHERE P."Condominium" = 'FALSE'
			UNION
			SELECT 	P."CityBlock"
					, P."Parcel"
					, P."OwnerCode"
					, P."OwnerName"
					, P."OwnerName2"
					, concat(P."LegalDesc1", P."LegalDesc2", P."LegalDesc3", P."LegalDesc4", P."LegalDesc5") Description
					, P."NbrOfApts"
					, P."Handle"
			FROM staging.prcl_prcl AS P
				JOIN staging.prcl_bldgres R
				   ON P."CityBlock" = R."CityBlock" AND P."Parcel" = R."Parcel" AND P."OwnerCode" = R."OwnerCode"
			WHERE P."Condominium" = 'FALSE'
			LIMIT 1000
		LOOP
			--figure out what parcel the building belongs to
			RAISE NOTICE '% % %', building."CityBlock", building."Parcel", building."OwnerCode";
			EXECUTE 
				'SELECT "parcel_id"
				FROM core.parcel
				WHERE parcel."city_block_number" = $1 AND parcel."parcel_number" = $2 AND parcel."parcel_taxing_status" = $3'
			INTO parcel_id
			USING building."CityBlock", building."Parcel", building."OwnerCode";
			
			--figure out what legal entity the building belongs to
			EXECUTE 
				'SELECT "legal_entity_id"
				FROM core.legal_entity
				WHERE "legal_entity_name" = $1 AND "legal_entity_secondary_name" = $2'
			INTO legal_entity_id
			USING building."OwnerName", building."OwnerName2";
			
			--split parcel_id up into individual parts
			county_id = substring(parcel_id, 1, 5);
			parcel_num = substring(parcel_id, 7, 8);
			RAISE NOTICE 'parcel_id: %', parcel_id;
			
			
			--LIKE statement
			check_parcel = concat(county_id, '.', parcel_num, '.%');
			RAISE NOTICE 'check_parcel: %', check_parcel;
			--figure out which building id comes next for specific parcel
			EXECUTE
				'SELECT max(substring(building_id, 16, 3))
				FROM core.building
				WHERE parcel_id LIKE $1'
				INTO building_num
				USING check_parcel;
				
			IF building_num IS NULL then
				building_num = '101';
			ELSE
				building_num = building_num + 1;
			END IF;
			
			--build full building_id
			building_id = concat(county_id, '.', parcel_num, '.', building_num, '.', '0000');
			RAISE NOTICE 'building_id: %', building_id;
			EXECUTE
				'INSERT INTO core.building(building_id, parcel_id, owner_id, description, apartment_count)
				VALUES($1, $2, $3, $4, $5::INTEGER)'
				USING building_id, parcel_id, legal_entity_id, building."description", building."NbrOfApts";
			
			
		END LOOP;
	END;
$function$
;
