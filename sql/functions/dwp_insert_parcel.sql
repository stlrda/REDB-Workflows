CREATE OR REPLACE FUNCTION insert_parcel_dwp(countyid varchar)
RETURNS void
AS $$
    DECLARE
		counter integer;
		parcel RECORD;
		cityblock varchar;
		parcel_id integer;
		building_id varchar := '000';
		unit_id varchar := '0000';
		legal_entity_id varchar;
		county_id varchar := $1;
		redb_id varchar;
		frontage float;
		landarea integer;
	BEGIN
		FOR parcel in 
			SELECT "CityBlock" --Select all the fields we want for redb parcel table
			, "Parcel"
			, "OwnerCode"
			, "PrimAddrRecNum"
			, concat("LegalDesc1", "LegalDesc2", "LegalDesc3", "LegalDesc4", "LegalDesc5") Description
			, "Frontage"
			, "LandArea"
			, "Zoning"
			, "Ward10"
			, "Precinct10"
			, "InspArea10"
			, "Nbrhd"
			, "PoliceDist"
			, "CensTract10"
			, "CensBlock10"
			, "AsrNbrhd"
			, "SpecParcelType"
			, "SubParcelType"
			, "GisCityBLock"
			, "GisParcel"
			, "GisOwnerCode"
			, "OwnerName"
			, "OwnerName2"
			FROM staging.prcl_prcl
			WHERE "Parcel" = "GisParcel" --where the Parcel is the same as the GIS Parcel
			  AND "OwnerCode" = "GisOwnerCode"
			  AND "OwnerCode" != '8' --excluding all billboards
			  -- limit 3000 --only for first 1000 rows
		LOOP
		  -- EXECUTE 
		  	--check if key fields already exist
			-- 'SELECT "city_block_number"
			-- FROM core.parcel
--			WHERE "city_block_number" = $1 AND "parcel_number" = $2 AND "parcel_taxing_status" = $3'
--		  		INTO cityblock
--				USING parcel."CityBlock", parcel."Parcel", parcel."OwnerCode";
		  -- RAISE NOTICE '%', legal_entity_id;
		  -- IF cityblock is NULL THEN
		  	--fetch legal_entity_id
			EXECUTE 
			'SELECT legal_entity_id 
			FROM core.legal_entity
			WHERE "legal_entity_name" = $1 AND "legal_entity_secondary_name" = $2'
		  		INTO legal_entity_id
				USING parcel."OwnerName", parcel."OwnerName2";
			--counter = counter + 1;
			EXECUTE 
				E'SELECT current_highest_value
				FROM core.highest_id
				WHERE id_name = \'parcel_id\''
				INTO parcel_id;
			IF parcel_id is null THEN
				parcel_id := 10000001;
			ELSE
				parcel_id := parcel_id + 1;
			END IF;
			RAISE NOTICE 'Parcel ID: %', parcel_id;
			--build redb parcel id
			redb_id = concat(county_id, '.', parcel_id, '.', building_id, '.', unit_id);
			
			--Cast land area and frontage to integer and float, respectively
			landarea = parcel."LandArea";
			frontage = parcel."Frontage";
			
			--fix invalid spec/sub parcels
-- 			IF parcel."SpecParcelType" = '' THEN
-- 				parcel."SpecParcelType" = NULL;
-- 			END IF;
-- 			IF parcel."SubParcelType" = '' THEN
-- 				parcel."SubParcelType" = NULL;
-- 			END IF;
			--ignoring spec/sub parcels for now
			parcel."SpecParcelType" = NULL;
			parcel."SubParcelType" = NULL;
			
		  	EXECUTE
				-- change to insert into parcel with the 30ish fields
				'INSERT INTO core.parcel(parcel_id, county_id, city_block_number, parcel_number, parcel_taxing_status, primary_addresses_count, owner_id, description,
										 frontage_to_street, land_area, zoning_class, ward, voting_precinct, inspection_area, neighborhood_id, police_district, census_tract,
										 census_block, asr_neighborhood, special_parcel_type_code, sub_parcel_type_code, gis_city_block, gis_parcel, gis_owner_code)
				 VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)'
			USING redb_id, county_id, parcel."CityBlock", parcel."Parcel", parcel."OwnerCode", parcel."PrimAddrRecNum", legal_entity_id, parcel."description", 
			frontage, landarea, parcel."Zoning", parcel."Ward10", parcel."Precinct10", parcel."InspArea10", parcel."Nbrhd", parcel."PoliceDist",
			parcel."CensTract10", parcel."CensBlock10", parcel."AsrNbrhd", parcel."SpecParcelType", parcel."SubParcelType", parcel."GisCityBLock", parcel."GisParcel",
			parcel."GisOwnerCode";
		  -- END IF;
		END LOOP;
	END;
$$
LANGUAGE plpgsql