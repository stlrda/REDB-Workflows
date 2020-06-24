------------Insert New addresses into core historical-------------------
WITH NEW_ADDRESS AS 
	(
	SELECT DISTINCT "prcl_test"."OwnerAddr"
		, "prcl_test"."OwnerCity"
		, "prcl_test"."OwnerState"
		, "prcl_test"."OwnerCountry"
		, "prcl_test"."OwnerZIP"
	FROM "staging_1"."prcl_test"
	LEFT JOIN "staging_2"."prcl_prcl"
		ON COALESCE("prcl_test"."OwnerAddr", ' ') = COALESCE("prcl_prcl"."OwnerAddr", ' ')
        AND COALESCE("prcl_test"."OwnerCity", ' ') = COALESCE("prcl_prcl"."OwnerCity", ' ') 
        AND COALESCE("prcl_test"."OwnerState", ' ') = COALESCE("prcl_prcl"."OwnerState", ' ')
        AND COALESCE("prcl_test"."OwnerCountry", ' ') = COALESCE("prcl_prcl"."OwnerCountry", ' ') 
        AND COALESCE("prcl_test"."OwnerZIP", ' ') = COALESCE("prcl_prcl"."OwnerZIP", ' ')
	WHERE "prcl_prcl"."OwnerAddr" IS NULL
	)
INSERT INTO "core"."address"("street_address"
	, "county_id"
	, "city"
	, "state"
	, "country"
	, "zip"
	, "create_date"
	, "current_flag"
	, "removed_flag"
    --, "etl_job"
	, "update_date"
	)
SELECT "OwnerAddr"
	, '10001'
	, "OwnerCity"
	, "OwnerState"
	, "OwnerCountry"
	, "OwnerZIP"
	, CURRENT_DATE
	, TRUE
	, FALSE
	, CURRENT_DATE
FROM NEW_ADDRESS
---------------new version compares current to previous instead of current to core------------
---------------should also not have that issue with the empty addresses also showing up as 'dead'----------

WITH DEAD_ADDRESS AS
	(
	SELECT DISTINCT
		"prcl_prcl"."OwnerAddr"
		, "prcl_prcl"."OwnerCity"
		, "prcl_prcl"."OwnerState"
		, "prcl_prcl"."OwnerCountry"
		, "prcl_prcl"."OwnerZIP"
	FROM "staging_2"."prcl_prcl"
	LEFT JOIN "staging_1"."prcl_test"
		ON CONCAT("prcl_test"."OwnerAddr","prcl_test"."OwnerCity","prcl_test"."OwnerState","prcl_test"."OwnerCountry","prcl_test"."OwnerZIP") = CONCAT("prcl_prcl"."OwnerAddr","prcl_prcl"."OwnerCity","prcl_prcl"."OwnerState","prcl_prcl"."OwnerCountry","prcl_prcl"."OwnerZIP") --Equivelent to coalescing and comparing each field not sure which way is more performant
	WHERE "prcl_test"."OwnerAddr" IS NULL AND "prcl_test"."OwnerCity" IS NULL AND "prcl_test"."OwnerState" IS NULL AND "prcl_test"."OwnerCountry" IS NULL AND "prcl_test"."OwnerZIP" IS NULL
	)
UPDATE "core"."address"
SET "current_flag" = FALSE
	, "removed_flag" = TRUE
	, "update_date" = CURRENT_DATE
FROM DEAD_ADDRESS
WHERE CONCAT(DEAD_ADDRESS."OwnerAddr", DEAD_ADDRESS."OwnerCity", DEAD_ADDRESS."OwnerState", DEAD_ADDRESS."OwnerCountry", DEAD_ADDRESS."OwnerZIP") = CONCAT("address"."street_address", "address"."city", "address"."state", "address"."country", "address"."zip")


-- Old version that includes address_id compared current to core.  
---------------Dead Addresses (But also the address ID that just has nulls everywhere.  Could maybe just purposly not select that specific ID?)----------------------
-- WITH DEAD_ADDRESS AS
-- 	(
-- 	SELECT DISTINCT "address_id"
-- 		, "OwnerAddr"
-- 		, "OwnerCity"
-- 		, "OwnerState"
-- 		, "OwnerCountry"
-- 		, "OwnerZIP"
-- 	FROM "core"."address"
-- 	LEFT JOIN "staging_1"."prcl_test"
-- 		ON CONCAT("OwnerAddr","OwnerCity","OwnerState","OwnerCountry","OwnerZIP") = CONCAT("street_address","city","state","country","zip") --Equivelent to coalescing and comparing each field not sure which way is more performant
-- 	WHERE "OwnerAddr" IS NULL AND "OwnerCity" IS NULL AND "OwnerState" IS NULL AND "OwnerCountry" IS NULL AND "OwnerZIP" IS NULL
-- 	)
-- 	UPDATE "core"."address"
-- 	SET "removed_flag" = TRUE,
-- 		"current_flag" = FALSE,
-- 		"update_date" = CURRENT_DATE
-- 	FROM DEAD_ADDRESS
-- 	WHERE DEAD_ADDRESS."address_id" = "address"."address_id" AND "address"."address_id" != 95190; --TODO VERIFY THAT THIS IS THE ACTUAL ADDRESS ID THAT HAS ALL THE REQUIRED FIELDS NULL BY DEFAULT (It was during my testing but it will likely change next time the DB is constructed)