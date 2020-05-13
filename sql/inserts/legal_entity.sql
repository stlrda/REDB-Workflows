BEGIN;

DROP TABLE IF EXISTS core.legal_entitytest;

CREATE TABLE IF NOT EXISTS core.legal_entitytest (legal_entity_id SERIAL PRIMARY KEY, legal_entity_address varchar, legal_entity_name varchar, legal_entity_secondary_name varchar, address_id integer, create_date date, current_flag boolean, removed_flag boolean, etl_job varchar, update_date date);

INSERT INTO core.legal_entitytest(legal_entity_address, legal_entity_name, legal_entity_secondary_name) 
SELECT "prcl_Prcl"."OwnerAddr", "prcl_Prcl"."OwnerName", "prcl_Prcl"."OwnerName2" 
FROM "staging_1"."prcl_Prcl" 
GROUP BY "prcl_Prcl"."OwnerAddr", "prcl_Prcl"."OwnerName", "prcl_Prcl"."OwnerName2";

UPDATE "core"."legal_entitytest" 
SET "address_id" = "addresstest"."address_id" 
FROM "core"."addresstest" 
WHERE "addresstest"."street_address" = "legal_entitytest"."legal_entity_address";

ALTER TABLE "core"."legal_entitytest" 
DROP COLUMN "legal_entity_address";

COMMIT;