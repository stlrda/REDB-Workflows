BEGIN;

DROP TABLE IF EXISTS core.neighborhoodtest;

CREATE TABLE IF NOT EXISTS core.neighborhoodtest (neighborhood_id SERIAL PRIMARY KEY, neighborhood_name varchar, county_id varchar, create_date date, current_flag boolean, removed_flag boolean, etl_job varchar, update_date date);

INSERT INTO core.neighborhoodtest(neighborhood_name)  
SELECT "prcl_Prcl"."Nbrhd" 
FROM "staging_1"."prcl_Prcl" 
GROUP BY "Nbrhd" 
ORDER BY "Nbrhd";

UPDATE "core"."neighborhoodtest"
SET "county_id" = (SELECT "county_id" FROM "core"."countytest" WHERE "countytest"."county_name" = 'Saint Louis City County');

COMMIT;