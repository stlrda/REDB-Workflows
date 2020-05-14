DROP TABLE IF EXISTS core.countytest;
CREATE TABLE IF NOT EXISTS core.countytest (county_id varchar PRIMARY KEY, county_name varchar, county_state varchar, create_date date, current_flag boolean, removed_flag boolean, etl_job varchar, update_date date);
INSERT INTO core.countytest(county_id, county_name, county_state) VALUES(10001, 'Saint Louis City County', 'MO');