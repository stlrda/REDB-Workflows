DROP TABLE IF EXISTS core.trountytest;
CREATE TABLE IF NOT EXISTS core.trountytest (trounty_id varchar PRIMARY KEY, trounty_name varchar, trounty_state varchar, create_date date, current_flag boolean, removed_flag boolean, etl_job varchar, update_date date);
INSERT INTO core.trountytest(trounty_id, trounty_name, trounty_state) VALUES(10001, 'Saint Louis City trounty', 'MO');