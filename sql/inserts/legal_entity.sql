DROP TABLE IF EXISTS core.legal_entity;

CREATE TABLE IF NOT EXISTS core.legal_entity (legal_entity_id SERIAL PRIMARY KEY, legal_entity_address varchar, legal_entity_name varchar, legal_entity_secondary_name varchar, address_id integer, create_date date, current_flag boolean, removed_flag boolean, etl_job varchar, update_date date);

INSERT INTO core.legal_entity(legal_entity_address, legal_entity_name, legal_entity_secondary_name) 
SELECT "prcl"."OwnerAddr", "prcl"."OwnerName", "prcl"."OwnerName2" 
FROM "staging_2"."prcl" 
GROUP BY "prcl"."OwnerAddr", "prcl"."OwnerName", "prcl"."OwnerName2";

UPDATE "core"."legal_entity" 
SET "address_id" = "address"."address_id" 
FROM "core"."address" 
WHERE "address"."street_address" = "legal_entity"."legal_entity_address"; --more detail maybe

ALTER TABLE "core"."legal_entity" 
DROP COLUMN "legal_entity_address";