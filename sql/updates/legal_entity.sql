--------------------------------------flag dead legal entities-------------------------------
CREATE OR REPLACE FUNCTION core.dead_legal_entity()
RETURNS void AS $$
BEGIN

WITH GET_ADDRESS_ID AS 
	(
	WITH DEAD_LEGAL_ENTITIES AS 
		(
		SELECT DISTINCT PREVIOUS_WEEK."OwnerName"
			, PREVIOUS_WEEK."OwnerName2"
			, PREVIOUS_WEEK."OwnerAddr"
			, PREVIOUS_WEEK."OwnerCity"
			, PREVIOUS_WEEK."OwnerState"
			, PREVIOUS_WEEK."OwnerCountry"
			, PREVIOUS_WEEK."OwnerZIP"
		FROM "staging_2"."prcl_prcl" AS PREVIOUS_WEEK
		LEFT JOIN "staging_1"."prcl_prcl" AS CURRENT_WEEK
		ON CONCAT(CURRENT_WEEK."OwnerAddr", CURRENT_WEEK."OwnerCity", CURRENT_WEEK."OwnerState", CURRENT_WEEK."OwnerCountry", CURRENT_WEEK."OwnerZIP") 
			= CONCAT(PREVIOUS_WEEK."OwnerAddr", PREVIOUS_WEEK."OwnerCity", PREVIOUS_WEEK."OwnerState", PREVIOUS_WEEK."OwnerCountry", PREVIOUS_WEEK."OwnerZIP")
		WHERE CURRENT_WEEK."ParcelId" IS NULL
		)
	SELECT DISTINCT "OwnerName"
		, "OwnerName2"
		, "OwnerAddr"
		, "address"."address_id"
	FROM DEAD_LEGAL_ENTITIES
	JOIN "core"."address"
	ON CONCAT(DEAD_LEGAL_ENTITIES."OwnerAddr", DEAD_LEGAL_ENTITIES."OwnerCity", DEAD_LEGAL_ENTITIES."OwnerState", DEAD_LEGAL_ENTITIES."OwnerCountry", DEAD_LEGAL_ENTITIES."OwnerZIP") 
		= CONCAT("street_address", "city", "state", "country", "zip")
	)
UPDATE "core"."legal_entity"
SET "removed_flag" = TRUE,
	"current_flag" = FALSE,
	"update_date" = CURRENT_DATE
FROM GET_ADDRESS_ID
WHERE CONCAT(GET_ADDRESS_ID."OwnerAddr", GET_ADDRESS_ID."OwnerName", GET_ADDRESS_ID."OwnerName2", GET_ADDRESS_ID."address_id")
	= CONCAT("legal_entity_address", "legal_entity_name", "legal_entity_secondary_name", "legal_entity"."address_id");

END;
$$
LANGUAGE plpgsql;
--------------------------
SELECT core.dead_legal_entity();