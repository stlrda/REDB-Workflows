DROP TABLE IF EXISTS core.neighborhood;

CREATE TABLE IF NOT EXISTS core.neighborhood (neighborhood_id SERIAL PRIMARY KEY, neighborhood_name varchar, county_id varchar, create_date date, current_flag boolean, removed_flag boolean, etl_job varchar, update_date date);

INSERT INTO core.neighborhood(neighborhood_name)  
SELECT "prcl"."Nbrhd" 
FROM "staging_2"."prcl" 
GROUP BY "Nbrhd" 
ORDER BY "Nbrhd";

UPDATE "core"."neighborhood"
SET "county_id" = (SELECT "county_id" FROM "core"."county" WHERE "county"."county_name" = 'Saint Louis City County');