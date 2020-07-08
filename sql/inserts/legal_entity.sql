----------------------------insert new legal entities----------------------------
CREATE OR REPLACE FUNCTION core.new_legal_entity()
RETURNS void AS $$
BEGIN

WITH GET_ADDRESS_ID AS 
	(
	WITH NEW_LEGAL_ENTITIES AS 
		(
		SELECT DISTINCT CURRENT_WEEK."OwnerName"
			, CURRENT_WEEK."OwnerName2"
			, CURRENT_WEEK."OwnerAddr"
			, CURRENT_WEEK."OwnerCity"
			, CURRENT_WEEK."OwnerState"
			, CURRENT_WEEK."OwnerCountry"
			, CURRENT_WEEK."OwnerZIP"
		FROM "staging_1"."prcl_prcl" AS CURRENT_WEEK
		LEFT JOIN "staging_2"."prcl_prcl" AS PREVIOUS_WEEK
		ON CONCAT(CURRENT_WEEK."OwnerName", CURRENT_WEEK."OwnerName2", CURRENT_WEEK."OwnerAddr", CURRENT_WEEK."OwnerCity", CURRENT_WEEK."OwnerState", CURRENT_WEEK."OwnerCountry", CURRENT_WEEK."OwnerZIP") 
			= CONCAT(PREVIOUS_WEEK."OwnerName", PREVIOUS_WEEK."OwnerName2", PREVIOUS_WEEK."OwnerAddr", PREVIOUS_WEEK."OwnerCity", PREVIOUS_WEEK."OwnerState", PREVIOUS_WEEK."OwnerCountry", PREVIOUS_WEEK."OwnerZIP")
		WHERE PREVIOUS_WEEK."ParcelId" IS NULL
		)
	SELECT DISTINCT "OwnerName"
		, "OwnerName2"
		, "OwnerAddr"
		, "address"."address_id"
	FROM NEW_LEGAL_ENTITIES
	JOIN "core"."address"
	ON CONCAT(NEW_LEGAL_ENTITIES."OwnerAddr", NEW_LEGAL_ENTITIES."OwnerCity", NEW_LEGAL_ENTITIES."OwnerState", NEW_LEGAL_ENTITIES."OwnerCountry", NEW_LEGAL_ENTITIES."OwnerZIP") 
		= CONCAT("street_address", "city", "state", "country", "zip")
	)
INSERT INTO "core"."legal_entity"(
	"legal_entity_address"
	, "legal_entity_name"
	, "legal_entity_secondary_name"
	, "address_id"
	, "create_date"
	, "current_flag"
	, "removed_flag"
	--, "etl_job"
	, "update_date"
	)
SELECT "OwnerAddr"
	, "OwnerName"
	, "OwnerName2"
	, "address_id"
	, CURRENT_DATE
	, TRUE
	, FALSE
	, CURRENT_DATE
FROM GET_ADDRESS_ID
ON CONFLICT (COALESCE("legal_entity_address", 'NULL_ADDRESS')
	, COALESCE("legal_entity_name", 'NULL_NAME_1')
	, COALESCE("legal_entity_secondary_name", 'NULL_NAME_2')
	, "address_id")
	DO UPDATE
SET "current_flag" = TRUE
	, "removed_flag" = FALSE
	, "update_date" = CURRENT_DATE;

END;
$$
LANGUAGE plpgsql;
-------------------------
SELECT new_legal_entity();