DROP TABLE IF EXISTS core.blountytest;
CREATE TABLE IF NOT EXISTS core.blountytest (blounty_id varchar PRIMARY KEY, blounty_name varchar, blounty_state varchar, create_date date, current_flag boolean, removed_flag boolean, etl_job varchar, update_date date);
INSERT INTO core.blountytest(blounty_id, blounty_name, blounty_state) VALUES(10001, 'Saint Louis City blounty', 'MO');