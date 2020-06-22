------------Insert New addresses into core historical-------------------
WITH NEW_ADDRESS AS 
	(
	SELECT DISTINCT "prcl_test"."OwnerAddr"
		, "prcl_test"."OwnerCity"
		, "prcl_test"."OwnerState"
		, "prcl_test"."OwnerCountry"
		, "prcl_test"."OwnerZIP"
	FROM "staging_1"."prcl_test"
	LEFT JOIN core."address"
		ON COALESCE("OwnerAddr", ' ') = COALESCE("street_address", ' ')
        AND COALESCE("OwnerCity", ' ') = COALESCE("city", ' ') 
        AND COALESCE("OwnerState", ' ') = COALESCE("state", ' ')
        AND COALESCE("OwnerCountry", ' ') = COALESCE("country", ' ') 
        AND COALESCE("OwnerZIP", ' ') = COALESCE("zip", ' ')
	WHERE "address"."address_id" IS NULL
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

---------------Dead Addresses (But also the address ID that just has nulls everywhere.  Could maybe just purposly not select that specific ID?)----------------------
WITH DEAD_ADDRESS AS
	(
	SELECT DISTINCT "address_id"
		, "OwnerAddr"
		, "OwnerCity"
		, "OwnerState"
		, "OwnerCountry"
		, "OwnerZIP"
	FROM "core"."address"
	LEFT JOIN "staging_1"."prcl_test"
		ON CONCAT("OwnerAddr","OwnerCity","OwnerState","OwnerCountry","OwnerZIP") = CONCAT("street_address","city","state","country","zip") --Equivelent to coalescing and comparing each field not sure which way is more performant
	WHERE "OwnerAddr" IS NULL AND "OwnerCity" IS NULL AND "OwnerState" IS NULL AND "OwnerCountry" IS NULL AND "OwnerZIP" IS NULL
	)
	UPDATE "core"."address"
	SET "removed_flag" = TRUE,
		"current_flag" = FALSE,
		"update_date" = CURRENT_DATE
	FROM DEAD_ADDRESS
	WHERE DEAD_ADDRESS."address_id" = "address"."address_id" AND "address"."address_id" != 95190; --TODO VERIFY THAT THIS IS THE ACTUAL ADDRESS ID THAT HAS ALL THE REQUIRED FIELDS NULL BY DEFAULT (It was during my testing but it will likely change next time the DB is constructed)

-- -----------random stuff I frequently queried while testing-----------------
-- UPDATE "staging_1"."prcl_test"
-- SET "CityBlock" = '1234.0',
-- 	"ParcelId" = '12340009990'
-- WHERE "CityBlock" = '2115.0' AND "ParcelId" = '21150009990'

-- UPDATE "staging_1"."prcl_test"
-- SET "OwnerAddr" = '123 Fake Street'
-- WHERE "ParcelId" = '12340009990'

-- SELECT * FROM "staging_2"."prcl_prcl"
-- WHERE "ParcelId" = '21150009990'

-- SELECT * FROM "core"."address" WHERE "street_address" = '2211 S GRAND #110'

-- SELECT * FROM "core"."county_id_mapping_table" WHERE "removed_flag" = TRUE

-- DELETE FROM "core"."county_id_mapping_table"
-- --SELECT * FROM "staging_1"."prcl_test"
-- WHERE "county_parcel_id" = '12340009990'