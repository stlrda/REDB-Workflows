-- ParcelId Is included in the WHERE but not in the join because it is possible for a parcel to have all of the fields 
-- used to make an address id be null. Howerver, ParcelId is only null if the adress fields cannot be joined on
------------Insert New addresses into core historical-------------------
WITH NEW_ADDRESS AS 
	(
	SELECT DISTINCT "staging_1"."prcl_prcl"."OwnerAddr"
		, "staging_1"."prcl_prcl"."OwnerCity"
		, "staging_1"."prcl_prcl"."OwnerState"
		, "staging_1"."prcl_prcl"."OwnerCountry"
		, "staging_1"."prcl_prcl"."OwnerZIP"
	FROM "staging_1"."prcl_prcl"
	LEFT JOIN "staging_2"."prcl_prcl"
	ON CONCAT("staging_1"."prcl_prcl"."OwnerAddr", "staging_1"."prcl_prcl"."OwnerCity", "staging_1"."prcl_prcl"."OwnerState", "staging_1"."prcl_prcl"."OwnerCountry", "staging_1"."prcl_prcl"."OwnerZIP") 
		= CONCAT("staging_2"."prcl_prcl"."OwnerAddr", "staging_2"."prcl_prcl"."OwnerCity", "staging_2"."prcl_prcl"."OwnerState", "staging_2"."prcl_prcl"."OwnerCountry", "staging_2"."prcl_prcl"."OwnerZIP")
	WHERE "staging_2"."prcl_prcl"."ParcelId" IS NULL
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

-- ParcelId Is included in the WHERE but not in the join because it is possible for a parcel to have all of the fields 
-- used to make an address id be null. Howerver, ParcelId is only null if the adress fields cannot be joined on
---------------updates removed_flag field for dead addresses------------

WITH DEAD_ADDRESS AS
	(
	SELECT DISTINCT
		"staging_2"."prcl_prcl"."ParcelId"
		, "staging_2"."prcl_prcl"."OwnerAddr"
		, "staging_2"."prcl_prcl"."OwnerCity"
		, "staging_2"."prcl_prcl"."OwnerState"
		, "staging_2"."prcl_prcl"."OwnerCountry"
		, "staging_2"."prcl_prcl"."OwnerZIP"
	FROM "staging_2"."prcl_prcl"
	LEFT JOIN "staging_1"."prcl_prcl"
	ON CONCAT("staging_1"."prcl_prcl"."OwnerAddr", "staging_1"."prcl_prcl"."OwnerCity", "staging_1"."prcl_prcl"."OwnerState", "staging_1"."prcl_prcl"."OwnerCountry", "staging_1"."prcl_prcl"."OwnerZIP") 
		= CONCAT("staging_2"."prcl_prcl"."OwnerAddr", "staging_2"."prcl_prcl"."OwnerCity", "staging_2"."prcl_prcl"."OwnerState", "staging_2"."prcl_prcl"."OwnerCountry", "staging_2"."prcl_prcl"."OwnerZIP")
	WHERE "staging_1"."prcl_prcl"."ParcelId" IS NULL
	)
UPDATE "core"."address"
SET "current_flag" = FALSE
	, "removed_flag" = TRUE
	, "update_date" = CURRENT_DATE
FROM DEAD_ADDRESS
WHERE CONCAT(DEAD_ADDRESS."OwnerAddr", DEAD_ADDRESS."OwnerCity", DEAD_ADDRESS."OwnerState", DEAD_ADDRESS."OwnerCountry", DEAD_ADDRESS."OwnerZIP") = CONCAT("address"."street_address", "address"."city", "address"."state", "address"."country", "address"."zip")