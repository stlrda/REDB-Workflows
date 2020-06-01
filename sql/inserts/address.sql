DROP TABLE IF EXISTS core.address;

CREATE TABLE IF NOT EXISTS core.address (address_id SERIAL PRIMARY KEY, street_address varchar, county_id varchar, city varchar, state varchar, country varchar, zip varchar, create_date date, current_flag boolean, removed_flag boolean, etl_job varchar, update_date date);

INSERT INTO core.address(street_address, city, state, country, zip) 
SELECT "OwnerAddr", "OwnerCity", "OwnerState", "OwnerCountry", "OwnerZIP" 
FROM "staging_2"."prcl_prcl" 
GROUP BY "OwnerAddr", "OwnerCity", "OwnerState", "OwnerCountry", "OwnerZIP";

UPDATE "core"."address" 
SET "county_id" = (SELECT "county_id" FROM "core"."county" WHERE "county"."county_name" = 'Saint Louis City County');