-- ParcelId Is included in the WHERE but not in the join because it is possible for a parcel to have all of the fields 
-- used to make an address id be null. Howerver, ParcelId is only null if the adress fields cannot be joined on
------------Insert New addresses into core historical-------------------
CREATE OR REPLACE FUNCTION core.new_address()
RETURNS void AS $$
BEGIN

WITH NEW_ADDRESS AS 
	(
	SELECT DISTINCT CURRENT_WEEK."OwnerAddr"
		, CURRENT_WEEK."OwnerCity"
		, CURRENT_WEEK."OwnerState"
		, CURRENT_WEEK."OwnerCountry"
		, CURRENT_WEEK."OwnerZIP"
	FROM "staging_1"."prcl_prcl" AS CURRENT_WEEK
	LEFT JOIN "staging_2"."prcl_prcl" AS PREVIOUS_WEEK
	ON CONCAT(CURRENT_WEEK."OwnerAddr", CURRENT_WEEK."OwnerCity", CURRENT_WEEK."OwnerState", CURRENT_WEEK."OwnerCountry", CURRENT_WEEK."OwnerZIP") 
		= CONCAT(PREVIOUS_WEEK."OwnerAddr", PREVIOUS_WEEK."OwnerCity", PREVIOUS_WEEK."OwnerState", PREVIOUS_WEEK."OwnerCountry", PREVIOUS_WEEK."OwnerZIP")
	WHERE PREVIOUS_WEEK."ParcelId" IS NULL
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
FROM NEW_ADDRESS;

END;
$$
LANGUAGE plpgsql;
-- ParcelId Is included in the WHERE but not in the join because it is possible for a parcel to have all of the fields 
-- used to make an address id be null. Howerver, ParcelId is only null if the adress fields cannot be joined on
---------------updates removed_flag field for dead addresses------------
CREATE OR REPLACE FUNCTION core.dead_address()
RETURNS void AS $$
BEGIN

WITH DEAD_ADDRESS AS
	(
	SELECT DISTINCT
		PREVIOUS_WEEK."ParcelId"
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
UPDATE "core"."address"
SET "current_flag" = FALSE
	, "removed_flag" = TRUE
	, "update_date" = CURRENT_DATE
FROM DEAD_ADDRESS
WHERE CONCAT(DEAD_ADDRESS."OwnerAddr", DEAD_ADDRESS."OwnerCity", DEAD_ADDRESS."OwnerState", DEAD_ADDRESS."OwnerCountry", DEAD_ADDRESS."OwnerZIP") = CONCAT("address"."street_address", "address"."city", "address"."state", "address"."country", "address"."zip");

END;
$$
LANGUAGE plpgsql;