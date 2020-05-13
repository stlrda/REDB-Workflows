BEGIN;

DROP TABLE IF EXISTS core.addresstest;

CREATE TABLE IF NOT EXISTS core.addresstest (address_id SERIAL PRIMARY KEY, street_address varchar, county_id varchar, city varchar, state varchar, country varchar, zip varchar, create_date date, current_flag boolean, removed_flag boolean, etl_job varchar, update_date date);

INSERT INTO core.addresstest(street_address, city, state, country, zip) 
SELECT "OwnerAddr", "OwnerCity", "OwnerState", "OwnerCountry", "OwnerZIP" 
FROM "staging_1"."prcl_Prcl" 
GROUP BY "OwnerAddr", "OwnerCity", "OwnerState", "OwnerCountry", "OwnerZIP";

UPDATE "core"."addresstest" 
SET "county_id" = (SELECT "county_id" FROM "core"."countytest" WHERE "countytest"."county_name" = 'Saint Louis City County');

COMMIT;