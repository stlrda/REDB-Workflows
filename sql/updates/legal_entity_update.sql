----------------------------insert new legal entities----------------------------
WITH NEW_LEGAL_ENTITIES AS
	(
	WITH LEGAL_ENTITY_CTE AS
		(
		/* inner query selects Address_ids along with OwnerAddr and Name fields needed to create unique legal_entity_ids    
		Coalesce is necessary to correctly join on fields that may contain null values */
		WITH QRY AS 
			(
			SELECT "ParcelId"
				, "OwnerName"
				, "OwnerName2"
				, "address_id"
				, "OwnerAddr"
				, "OwnerCity"
				, "OwnerState"
				, "OwnerCountry"
				, "OwnerZIP" 
			FROM "core"."address"
			JOIN "staging_1"."prcl_test" 
			ON COALESCE("OwnerAddr", ' ') = COALESCE("street_address", ' ')
			AND COALESCE("OwnerCity", ' ') = COALESCE("city", ' ') 
			AND COALESCE("OwnerState", ' ') = COALESCE("state", ' ')
			AND COALESCE("OwnerCountry", ' ') = COALESCE("country", ' ') 
			AND COALESCE("OwnerZIP", ' ') = COALESCE("zip", ' ')
			)
		SELECT "OwnerAddr"
			, "OwnerName"
			, "OwnerName2"
			, "address_id"
		FROM QRY
		GROUP BY "OwnerAddr", "OwnerName", "OwnerName2", "address_id"
		ORDER BY "address_id"
		)
	SELECT DISTINCT "legal_entity"."legal_entity_id", "OwnerAddr", "OwnerName", "OwnerName2", LEGAL_ENTITY_CTE."address_id" FROM LEGAL_ENTITY_CTE --Current 
	LEFT JOIN "core"."legal_entity" --Core Historical
	ON COALESCE(LEGAL_ENTITY_CTE."OwnerAddr", ' ') = COALESCE("legal_entity"."legal_entity_address", ' ') 
		AND COALESCE(LEGAL_ENTITY_CTE."OwnerName", ' ') = COALESCE("legal_entity"."legal_entity_name", ' ')
		AND COALESCE(LEGAL_ENTITY_CTE."OwnerName2", ' ') = COALESCE("legal_entity"."legal_entity_secondary_name", ' ')
		AND LEGAL_ENTITY_CTE."address_id" = "legal_entity"."address_id"
	WHERE "legal_entity_id" IS NULL
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
FROM NEW_LEGAL_ENTITIES
--------------------------------------flag dead legal entities-------------------------------
WITH DEAD_LEGAL_ENTITIES AS
	(
	WITH CURRENT_LEGAL_ENTITIES AS
		(
		SELECT "ParcelId"
			, "OwnerName"
			, "OwnerName2"
			, "address_id"
			, "OwnerAddr"
			, "OwnerCity"
			, "OwnerState"
			, "OwnerCountry"
			, "OwnerZIP" 
		FROM "core"."address"
		JOIN "staging_1"."prcl_test" 
		ON COALESCE("OwnerAddr", ' ') = COALESCE("street_address", ' ')
		AND COALESCE("OwnerCity", ' ') = COALESCE("city", ' ') 
		AND COALESCE("OwnerState", ' ') = COALESCE("state", ' ')
		AND COALESCE("OwnerCountry", ' ') = COALESCE("country", ' ') 
		AND COALESCE("OwnerZIP", ' ') = COALESCE("zip", ' ')
		)
	SELECT "legal_entity_id", "legal_entity_address", "legal_entity_name", "legal_entity_secondary_name", "legal_entity"."address_id"
	FROM "core"."legal_entity"
	LEFT JOIN CURRENT_LEGAL_ENTITIES
	ON CONCAT("OwnerAddr", "OwnerName", "OwnerName2", CURRENT_LEGAL_ENTITIES."address_id") = CONCAT("legal_entity_address", "legal_entity_name", "legal_entity_secondary_name", "legal_entity"."address_id")
	WHERE CONCAT("OwnerAddr", "OwnerName", "OwnerName2", CURRENT_LEGAL_ENTITIES."address_id") NOT IN (SELECT CONCAT("legal_entity_address", "legal_entity_name", "legal_entity_secondary_name", "legal_entity"."address_id") FROM CURRENT_LEGAL_ENTITIES)
	)
UPDATE "core"."legal_entity"
SET "removed_flag" = TRUE,
	"current_flag" = FALSE,
	"update_date" = CURRENT_DATE
FROM DEAD_LEGAL_ENTITIES
WHERE "legal_entity"."legal_entity_id" = DEAD_LEGAL_ENTITIES."legal_entity_id"