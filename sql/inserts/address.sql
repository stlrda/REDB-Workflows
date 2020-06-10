-- Creates table for address ids which are uniquely assigned via the serial Primary Key "address_id"
CREATE TABLE IF NOT EXISTS core.address (
    address_id SERIAL PRIMARY KEY
    , street_address varchar
    , county_id varchar
    , city varchar
    , state varchar
    , country varchar
    , zip varchar
    , create_date date
    , current_flag boolean
    , removed_flag boolean
    , etl_job varchar
    , update_date date
    );

-- Selects all the address information fields from prcl_prcl and groups by them in order to find each unique combination that occurs.
-- This unique combination is what makes up a unique address_id
INSERT INTO core.address(
    street_address
    , county_id
    , city
    , state
    , country
    , zip
    ) 
SELECT "OwnerAddr"
    , (SELECT "county_id" FROM "core"."county" WHERE "county"."county_name" = 'Saint Louis City County')
    , "OwnerCity"
    , "OwnerState"
    , "OwnerCountry"
    , "OwnerZIP" 
FROM "staging_2"."prcl_prcl" 
GROUP BY "OwnerAddr", "OwnerCity", "OwnerState", "OwnerCountry", "OwnerZIP";