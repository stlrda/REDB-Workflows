-- ParcelId Is included in the WHERE but not in the join because it is possible for a parcel to have all of the fields 
-- used to make an address id be null. Howerver, ParcelId is only null if the adress fields cannot be joined on
---------------updates removed_flag field for dead addresses------------
CREATE OR REPLACE FUNCTION core.dead_address()
RETURNS void AS $$
BEGIN

WITH DEAD_OWNER_ADDRESS AS
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
FROM DEAD_OWNER_ADDRESS
WHERE CONCAT(DEAD_OWNER_ADDRESS."OwnerAddr", DEAD_OWNER_ADDRESS."OwnerCity", DEAD_OWNER_ADDRESS."OwnerState", DEAD_OWNER_ADDRESS."OwnerCountry", DEAD_OWNER_ADDRESS."OwnerZIP") 
	= CONCAT("address"."street_address", "address"."city", "address"."state", "address"."country", "address"."zip");

WITH DEAD_STREET_ADDRESS AS
	(
	SELECT DISTINCT
		PREVIOUS_WEEK."ParcelId"
		, COALESCE(PREVIOUS_WEEK."LowAddrNum", '') AS "LowAddrNum"
		, COALESCE(PREVIOUS_WEEK."HighAddrNum", '') AS "HighAddrNum"
		, COALESCE(PREVIOUS_WEEK."StPreDir", '') AS "StPreDir"
		, COALESCE(PREVIOUS_WEEK."StName", '') AS "StName"
		, COALESCE(PREVIOUS_WEEK."StType", '') AS "StType"
		, COALESCE(PREVIOUS_WEEK."LowAddrSuf", '') AS "LowAddrSuf"
		, COALESCE(PREVIOUS_WEEK."HighAddrSuf", '') AS "HighAddrSuf"
		FROM "staging_2"."prcl_prcl" AS PREVIOUS_WEEK
	LEFT JOIN "staging_1"."prcl_prcl" AS CURRENT_WEEK
	ON CONCAT(CURRENT_WEEK."LowAddrNum", CURRENT_WEEK."HighAddrNum", CURRENT_WEEK."StPreDir", CURRENT_WEEK."StName", CURRENT_WEEK."StType", CURRENT_WEEK."LowAddrSuf", CURRENT_WEEK."HighAddrSuf") 
		= CONCAT(PREVIOUS_WEEK."LowAddrNum", PREVIOUS_WEEK."HighAddrNum", PREVIOUS_WEEK."StPreDir", PREVIOUS_WEEK."StName", PREVIOUS_WEEK."StType", PREVIOUS_WEEK."LowAddrSuf", PREVIOUS_WEEK."HighAddrSuf")
	WHERE CURRENT_WEEK."ParcelId" IS NULL
	)
UPDATE "core"."address"
SET "current_flag" = FALSE
	, "removed_flag" = TRUE
	, "update_date" = CURRENT_DATE
FROM DEAD_STREET_ADDRESS
WHERE (SELECT core.row_level_format_street_address(DEAD_STREET_ADDRESS."LowAddrNum"
													, DEAD_STREET_ADDRESS."HighAddrNum"
													, DEAD_STREET_ADDRESS."StPreDir"
													, DEAD_STREET_ADDRESS."StName"
													, DEAD_STREET_ADDRESS."StType"
													, DEAD_STREET_ADDRESS."LowAddrSuf"
													, DEAD_STREET_ADDRESS."HighAddrSuf")) = "address"."street_address";
	
END;
$$
LANGUAGE plpgsql;
-------------------------
SELECT core.dead_address();