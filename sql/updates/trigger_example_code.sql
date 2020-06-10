UPDATE "core"."BTest"
SET "owner_id" = '100039'
WHERE "parcel_id" = '10001.10082923.000.0000'

SELECT * FROM "core"."BTest";
SELECT * FROM "core"."BHistory";

SELECT * FROM "core"."county" LIMIT 1000
SELECT * FROM "core"."address" LIMIT 1000
SELECT * FROM "core"."neighborhood" LIMIT 1000
SELECT * FROM "core"."legal_entity" LIMIT 1000
SELECT * FROM "core"."county_id_mapping_table" LIMIT 1000
SELECT * FROM "core"."parcel" LIMIT 1000
SELECT * FROM "core"."building" LIMIT 1000
SELECT * FROM "core"."unit" LIMIT 1000
SELECT * FROM "core"."special_parcel_type" LIMIT 1000
SELECT * FROM "core"."sub_parcel_type" LIMIT 1000

------------------dead parcels function---------------
SELECT find_dead_parcels()

CREATE OR REPLACE FUNCTION find_dead_parcels()
RETURNS void AS 
$$
BEGIN
	INSERT INTO staging_2.dead_parcels
	SELECT staging_2.prcl_prcl."ParcelId", staging_2.prcl_prcl."OwnerUpdate"
	FROM staging_2.prcl_prcl
	LEFT JOIN staging_1."prcl_test"
		ON staging_2.prcl_prcl."ParcelId" = staging_1."prcl_test"."ParcelId"
	WHERE staging_1."prcl_test"."ParcelId" IS NULL;
END;
$$
LANGUAGE plpgsql;

--murder a parcel
DELETE FROM "staging_1"."prcl_test"
WHERE "ParcelId" = '57750003472'

---------------FIND NEW PARCELS------------------------
SELECT staging_1.prcl_test."ParcelId"
FROM staging_1.prcl_test
LEFT JOIN staging_2."prcl_prcl"
	ON staging_1.prcl_test."ParcelId" = staging_2."prcl_prcl"."ParcelId"
WHERE staging_2."prcl_prcl"."ParcelId" IS NULL;

-- -----------Removed Flag meta procedure-----------
-- DROP FUNCTION removed_flag_meta() CASCADE
-- CREATE OR REPLACE FUNCTION removed_flag_meta()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     PERFORM removed_flag_id_mapping();
--     PERFORM removed_flag_parcel();
-- END;
-- $$
-- LANGUAGE plpgsql;

-- -----------Meta trigger---------
-- CREATE TRIGGER update_removed_flags AFTER INSERT
-- ON "staging_2"."dead_parcels"
-- EXECUTE PROCEDURE removed_flag_meta();

-------------------ID MAPPING PROC----------------
--DROP FUNCTION removed_flag_id_mapping()
CREATE OR REPLACE FUNCTION removed_flag_id_mapping()
RETURNS TRIGGER AS 
$$
BEGIN
	UPDATE "core"."county_id_mapping_table" 
	SET "removed_flag" = TRUE,
		"current_flag" = FALSE,
		"update_date" = CURRENT_DATE
	WHERE NEW."ParcelId" = "county_id_mapping_table"."county_parcel_id";
	RETURN NULL;
END;
$$
LANGUAGE plpgsql;

-------ID MAPPING TRIGGER--------
CREATE TRIGGER update_removed_flag_id_mapping AFTER INSERT
ON "staging_2"."dead_parcels"
FOR EACH ROW
EXECUTE PROCEDURE removed_flag_id_mapping();

----------------PARCEL PROC--------------------
--DROP FUNCTION removed_flag_parcel()
CREATE OR REPLACE FUNCTION removed_flag_parcel()
RETURNS TRIGGER AS 
$$
BEGIN
	WITH REDB_COUNTY AS 
	(
		SELECT DISTINCT SUBSTRING("county_id_mapping_table"."parcel_id" FROM 1 FOR 14) AS redb_county_id
		FROM "core"."county_id_mapping_table"
		WHERE NEW."ParcelId" = "county_id_mapping_table"."county_parcel_id"
	)
	UPDATE "core"."parcel" 
	SET "removed_flag" = TRUE,
		"current_flag" = FALSE,
		"update_date" = CURRENT_DATE
	FROM REDB_COUNTY
	WHERE "redb_county_id" = SUBSTRING("parcel"."parcel_id" FROM 1 FOR 14);
	RETURN NULL;
END;
$$
LANGUAGE plpgsql;

----------------------PARCEL TRIGGER---------------------
CREATE TRIGGER update_removed_flag_parcel AFTER INSERT
ON "staging_2"."dead_parcels"
FOR EACH ROW
EXECUTE PROCEDURE removed_flag_parcel();

----------------BUILDING PROC--------------------
--DROP FUNCTION removed_flag_building()
CREATE OR REPLACE FUNCTION removed_flag_building()
RETURNS TRIGGER AS 
$$
BEGIN
	WITH REDB_COUNTY AS 
	(
		SELECT DISTINCT SUBSTRING("county_id_mapping_table"."parcel_id" FROM 1 FOR 14) AS redb_county_id
		FROM "core"."county_id_mapping_table"
		WHERE NEW."ParcelId" = "county_id_mapping_table"."county_parcel_id"
	)
	UPDATE "core"."building" 
	SET "removed_flag" = TRUE,
		"current_flag" = FALSE,
		"update_date" = CURRENT_DATE
	FROM REDB_COUNTY
	WHERE "redb_county_id" = SUBSTRING("building"."parcel_id" FROM 1 FOR 14);
	RETURN NULL;
END;
$$
LANGUAGE plpgsql;

----------------------BUILDING TRIGGER---------------------
CREATE TRIGGER update_removed_flag_building AFTER INSERT
ON "staging_2"."dead_parcels"
FOR EACH ROW
EXECUTE PROCEDURE removed_flag_building();

----------------UNIT PROC--------------------
--DROP FUNCTION removed_flag_unit()
CREATE OR REPLACE FUNCTION removed_flag_unit()
RETURNS TRIGGER AS 
$$
BEGIN
	WITH REDB_COUNTY AS 
	(
		SELECT DISTINCT SUBSTRING("county_id_mapping_table"."parcel_id" FROM 1 FOR 14) AS redb_county_id
		FROM "core"."county_id_mapping_table"
		WHERE NEW."ParcelId" = "county_id_mapping_table"."county_parcel_id"
	)
	UPDATE "core"."unit" 
	SET "removed_flag" = TRUE,
		"current_flag" = FALSE,
		"update_date" = CURRENT_DATE
	FROM REDB_COUNTY
	WHERE "redb_county_id" = SUBSTRING("unit"."unit_id" FROM 1 FOR 14);
	RETURN NULL;
END;
$$
LANGUAGE plpgsql;

----------------------UNIT TRIGGER---------------------
CREATE TRIGGER update_removed_flag_unit AFTER INSERT
ON "staging_2"."dead_parcels"
FOR EACH ROW
EXECUTE PROCEDURE removed_flag_unit();

---------Check Values-----------
SELECT * FROM "staging_2"."dead_parcels"

SELECT * FROM "core"."county_id_mapping_table" --LIMIT 10
WHERE "parcel_id" = '10001.10000818.000.0000'
WHERE "removed_flag" IS NOT NULL
ORDER BY "county_parcel_id"

SELECT * FROM "core"."parcel" --LIMIT 10
--WHERE "parcel_number" = '55200002122'
WHERE "removed_flag" IS NOT NULL
ORDER BY "parcel_number"

SELECT * FROM "core"."building" -- LIMIT 10
WHERE "removed_flag" IS NOT NULL

SELECT * FROM "core"."unit" --LIMIT 10
WHERE "removed_flag" IS NOT NULL

--------RESET STUFF---------------
UPDATE "core"."county_id_mapping_table"
SET "removed_flag" = NULL
WHERE "removed_flag" IS NOT NULL

UPDATE "core"."parcel"
SET "removed_flag" = NULL
WHERE "removed_flag" IS NOT NULL

UPDATE "core"."building"
SET "removed_flag" = NULL
WHERE "removed_flag" IS NOT NULL

UPDATE "core"."unit"
SET "removed_flag" = NULL
WHERE "removed_flag" IS NOT NULL

DELETE FROM "staging_2"."dead_parcels"
WHERE "ParcelId" IS NOT NULL
