CREATE TABLE IF NOT EXISTS core.neighborhood (
    neighborhood_id SERIAL PRIMARY KEY
    , neighborhood_name varchar
    , county_id varchar
    , create_date date
    , current_flag boolean
    , removed_flag boolean
    , etl_job varchar
    , update_date date
    );

-- Selects each unique neighborhood from prcl_prcl and assigns an ID to each one via Serial Primary Key
-- County ID is hard coded to match Sait Louis City County since there is only one county
-- admittidly this method was a bit obtuse.  Could have also just selected the value '10001'
INSERT INTO core.neighborhood(neighborhood_name, county_id)  
SELECT "prcl_prcl"."Nbrhd", (SELECT "county_id" FROM "core"."county" WHERE "county"."county_name" = 'Saint Louis City County') 
FROM "staging_2"."prcl_prcl" 
GROUP BY "Nbrhd" 
ORDER BY "Nbrhd";