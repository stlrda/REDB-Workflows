BEGIN;
CREATE OR REPLACE TEMPORARY VIEW owner_address_ids AS (
    SELECT
           ha."address_id"
           ,pp."OwnerName"
           ,pp."OwnerName2"
    FROM history.address ha
    INNER JOIN staging_1_2.prcl_prcl pp
        ON ha."street_address" = pp."OwnerAddr"
);

WITH present_legal_entities AS (
    SELECT
        CONCAT(legal_entity_name, legal_entity_secondary_name) AS entity
    FROM history.legal_entity
)
INSERT INTO history.legal_entity
	(
	address_id
	,legal_entity_name
	,legal_entity_secondary_name
	)
	SELECT DISTINCT
        "address_id"
        ,"OwnerName"
        ,"OwnerName2"
	FROM owner_address_ids oai
	WHERE NOT EXISTS(
	    SELECT
            entity
	    FROM present_legal_entities ple
	    WHERE CONCAT(oai."OwnerName", oai."OwnerName2") = ple.entity
        )
    RETURNING *;
COMMIT;
