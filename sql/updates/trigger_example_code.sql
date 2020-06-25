-----------------dead parcels function---------------
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

-------------------------------------------------------------------------------------------------------
SELECT * FROM "core"."neighborhood" WHERE "removed_flag" IS NOT FALSE
SELECT * FROM "core"."address" WHERE "removed_flag" IS NOT FALSE
SELECT * FROM "core"."county_id_mapping_table" WHERE "removed_flag" IS NOT FALSE
SELECT * FROM "core"."legal_entity" WHERE "removed_flag" IS NOT FALSE
SELECT * FROM "core"."parcel" WHERE "removed_flag" IS NOT FALSE
SELECT * FROM "core"."building" WHERE "removed_flag" IS NOT FALSE
SELECT * FROM "core"."unit" WHERE "removed_flag" IS NOT FALSE

INSERT INTO "staging_2"."prcl_prcl"
SELECT * FROM "staging_1"."prcl_prcl"

INSERT INTO "staging_2"."prcl_bldgcom"
SELECT * FROM "staging_1"."prcl_bldgcom"

INSERT INTO "staging_2"."prcl_bldgres"
SELECT * FROM "staging_1"."prcl_bldgres"

INSERT INTO "staging_2"."prcl_bldgsect"
SELECT * FROM "staging_1"."prcl_bldgsect"

SELECT * FROM core.unit WHERE SUBSTRING(unit_id FROM 16 FOR 3) != '101'
SELECT * FROM core.county_id_mapping_table WHERE SUBSTRING(parcel_id FROM 7 FOR 8) = '10082904'

DELETE FROM "staging_1"."prcl_bldgcom" AS com
WHERE core.format_parcelId(com."CityBlock", com."Parcel", com."OwnerCode") = '39290000152'

DELETE FROM "staging_1"."prcl_bldgres" AS res
WHERE core.format_parcelId(res."CityBlock", res."Parcel", res."OwnerCode") = '39290000152'

DELETE FROM "staging_1"."prcl_bldgsect" as sect
WHERE core.format_parcelId(sect."CityBlock", sect."Parcel", sect."OwnerCode") = '39290000152'

DELETE FROM "staging_1"."prcl_prcl"
WHERE "ParcelId" = '39290000152'

SELECT * FROM "staging_1"."prcl_bldgsect" AS sect
WHERE sect."CityBlock" = '101.0' AND sect."Parcel" = '10' AND sect."OwnerCode" = '0'

SELECT * FROM "staging_1"."prcl_bldgsect" AS sect
WHERE core.format_parcelId(sect."CityBlock", sect."Parcel", sect."OwnerCode") = '01230000100'

INSERT INTO "staging_1"."prcl_bldgcom"
SELECT '123.0', "Parcel", "OwnerCode", "BldgNum", "BldgCategory", "BldgType", "GroundFloorArea", "ComExtWallType", "ComStoriesCode", "YearBuilt", "ComConstType", "TotApts", "NbrOfAptsEff", "NbrOfApts1Br", "NbrOfApts2Br", "NbrOfApts3Br", "NbrOfAptsOther", "FirstDate", "LastDate", "LastUpDate"
FROM "staging_1"."prcl_bldgcom" AS com
WHERE com."CityBlock" = '101.0' AND com."Parcel" = '10' AND com."OwnerCode" = '0'

INSERT INTO "staging_1"."prcl_bldgres"
SELECT '123.0', "Parcel", "OwnerCode", "BldgNum", "ResOccType", "BsmtType", "BsmtFinishType", "BsmtAreaFinished", "BsmtAreaPartial", "ResExtWallType", "LivingAreaTotal", "LivingAreaAtGrade", "LivingAreaUpDown", "RoomArea", "ResStoriesCode", "FullBaths", "HalfBaths", "AirCondCentral", "AirCondWindow", "CentralHeating", "Attic", "Garage1", "Garage2", "Carport1", "Carport2", "YearBuilt", "ResModelCode", "ResModelAdjCode", "EffectiveYearBuilt", "FunctionalDep", "LocationalDep", "FirstDate", "LastDate", "LastUpDate"
FROM "staging_1"."prcl_bldgres" AS res
WHERE res."CityBlock" = '101.0' AND res."Parcel" = '10' AND res."OwnerCode" = '0'

INSERT INTO "staging_1"."prcl_bldgsect"
SELECT '123.0', "Parcel", "OwnerCode", "BldgNum", "SectNum", "SectCategory", "SectType", "FramingType", "ExcavType", "ExtWallType", "LevelFrom", "LevelTo", "StoryHeight", "Area", "FoundationType", "RoofType", "RoofConst1", "RoofConst2", "RoofConst3", "RoofConst4", "Heating", "AirCondCentral", "AirCondWindow", "Electricity", "FireProtection", "Elevator", "BsmtStoryHeight", "BsmtNbrOfStories", "BsmtAreaFinished", "BsmtAreaUnfin", "BsmtGarageArea", "FirstDate", "LastDate", "LastUpDate"
FROM "staging_1"."prcl_bldgsect" AS sect
WHERE sect."CityBlock" = '101.0' AND sect."Parcel" = '10' AND sect."OwnerCode" = '0'

INSERT INTO "staging_1"."prcl_prcl"
SELECT '123.0', "Parcel", "OwnerCode", "ParcelId", "PrimAddrRecNum", "AddrType", "LowAddrNum", "LowAddrSuf", "HighAddrNum", "HighAddrSuf", "NLC", "Parity", "StPreDir", "StName", "StType", "StSufDir", "StdUnitNum", "OwnerName", "OwnerName2", "OwnerAddr", "OwnerCity", "OwnerState", "OwnerCountry", "OwnerZIP", "OwnerRank", "LegalDesc1", "LegalDesc2", "LegalDesc3", "LegalDesc4", "LegalDesc5", "AsrClassCode", "AsrLandUse1", "AsrLanduse2", "RedevPhase", "RedevYearEnd", "RedevPhase2", "RedevYearEnd2", "VacantLot", "SpecBusDist", "SpecBusDist2", "TIFDist", "LendingAgcy", "Condominium", "NbrOfUnits", "NbrOfApts", "Frontage", "LandArea", "RecDailyDate", "RecDailyNum", "RecBookNum", "RecPageNum", "AsdLand", "AsdImprove", "AsdTotal", "BillLand", "BillImprove", "BillTotal", "AprLand", "CostAprImprove", "AsmtAppealYear", "AsmtAppealNum", "AsmtAppealType", "PriorAsdDate", "PriorAsdLand", "PriorAsdImprove", "PriorAsdTotal", "PriorTaxAmt", "CDALandUse1", "CDALandUse2", "LRMSUnitNum", "Zoning", "NbrOfBldgsRes", "NbrOfBldgsCom", "FirstYearBuilt", "LastYearBuilt", "ResSalePrice", "ResSaleDate", "VacBldgYear", "GeoCityBlockPart", "Ward10", "Precinct10", "InspArea10", "Ward00", "Precinct02", "Precinct04", "Nbrhd", "CDADist", "CDASubDist", "PoliceDist", "CensTract10", "CensBlock10", "CensBlock00", "Ward90", "Precinct90", "CensBlock90", "HouseConsDist", "AsrNbrhd", "EntZone", "ImpactArea", "CTDArea", "LeafArea", "ZIP", "OnFloodBlock", "SpecParcelType", "SubParcelType", "NbrOfSubAccts", "NbrOfCondos", "LRMSParcel", "AcctPrimary", "GisPrimary", "GisCityBLock", "GisParcel", "GisOwnerCode", "Handle", "Parcel9", "OwnerOcc", "FirstDate", "LastDate", "OwnerUpdate"
FROM "staging_1"."prcl_prcl" 
WHERE "ParcelId" = '01010000100'

INSERT INTO "staging_1"."prcl_prcl"("CityBlock", "Parcel", "OwnerCode", "ParcelId", "PrimAddrRecNum", "AddrType", "LowAddrNum", "LowAddrSuf", "HighAddrNum", "HighAddrSuf", "NLC", "Parity", "StPreDir", "StName", "StType", "StSufDir", "StdUnitNum", "OwnerName", "OwnerName2", "OwnerAddr", "OwnerCity", "OwnerState", "OwnerCountry", "OwnerZIP", "OwnerRank", "LegalDesc1", "LegalDesc2", "LegalDesc3", "LegalDesc4", "LegalDesc5", "AsrClassCode", "AsrLandUse1", "AsrLanduse2", "RedevPhase", "RedevYearEnd", "RedevPhase2", "RedevYearEnd2", "VacantLot", "SpecBusDist", "SpecBusDist2", "TIFDist", "LendingAgcy", "Condominium", "NbrOfUnits", "NbrOfApts", "Frontage", "LandArea", "RecDailyDate", "RecDailyNum", "RecBookNum", "RecPageNum", "AsdLand", "AsdImprove", "AsdTotal", "BillLand", "BillImprove", "BillTotal", "AprLand", "CostAprImprove", "AsmtAppealYear", "AsmtAppealNum", "AsmtAppealType", "PriorAsdDate", "PriorAsdLand", "PriorAsdImprove", "PriorAsdTotal", "PriorTaxAmt", "CDALandUse1", "CDALandUse2", "LRMSUnitNum", "Zoning", "NbrOfBldgsRes", "NbrOfBldgsCom", "FirstYearBuilt", "LastYearBuilt", "ResSalePrice", "ResSaleDate", "VacBldgYear", "GeoCityBlockPart", "Ward10", "Precinct10", "InspArea10", "Ward00", "Precinct02", "Precinct04", "Nbrhd", "CDADist", "CDASubDist", "PoliceDist", "CensTract10", "CensBlock10", "CensBlock00", "Ward90", "Precinct90", "CensBlock90", "HouseConsDist", "AsrNbrhd", "EntZone", "ImpactArea", "CTDArea", "LeafArea", "ZIP", "OnFloodBlock", "SpecParcelType", "SubParcelType", "NbrOfSubAccts", "NbrOfCondos", "LRMSParcel", "AcctPrimary", "GisPrimary", "GisCityBLock", "GisParcel", "GisOwnerCode", "Handle", "Parcel9", "OwnerOcc", "FirstDate", "LastDate", "OwnerUpdate")
SELECT '7700.0', '77', '7', '77000000777', "PrimAddrRecNum", "AddrType", "LowAddrNum", "LowAddrSuf", "HighAddrNum", "HighAddrSuf", "NLC", "Parity", "StPreDir", "StName", "StType", "StSufDir", "StdUnitNum", "OwnerName", "OwnerName2", "OwnerAddr", "OwnerCity", "OwnerState", "OwnerCountry", "OwnerZIP", "OwnerRank", "LegalDesc1", "LegalDesc2", "LegalDesc3", "LegalDesc4", "LegalDesc5", "AsrClassCode", "AsrLandUse1", "AsrLanduse2", "RedevPhase", "RedevYearEnd", "RedevPhase2", "RedevYearEnd2", "VacantLot", "SpecBusDist", "SpecBusDist2", "TIFDist", "LendingAgcy", "Condominium", "NbrOfUnits", "NbrOfApts", "Frontage", "LandArea", "RecDailyDate", "RecDailyNum", "RecBookNum", "RecPageNum", "AsdLand", "AsdImprove", "AsdTotal", "BillLand", "BillImprove", "BillTotal", "AprLand", "CostAprImprove", "AsmtAppealYear", "AsmtAppealNum", "AsmtAppealType", "PriorAsdDate", "PriorAsdLand", "PriorAsdImprove", "PriorAsdTotal", "PriorTaxAmt", "CDALandUse1", "CDALandUse2", "LRMSUnitNum", "Zoning", "NbrOfBldgsRes", "NbrOfBldgsCom", "FirstYearBuilt", "LastYearBuilt", "ResSalePrice", "ResSaleDate", "VacBldgYear", "GeoCityBlockPart", "Ward10", "Precinct10", "InspArea10", "Ward00", "Precinct02", "Precinct04", '777', "CDADist", "CDASubDist", "PoliceDist", "CensTract10", "CensBlock10", "CensBlock00", "Ward90", "Precinct90", "CensBlock90", "HouseConsDist", "AsrNbrhd", "EntZone", "ImpactArea", "CTDArea", "LeafArea", "ZIP", "OnFloodBlock", "SpecParcelType", "SubParcelType", "NbrOfSubAccts", "NbrOfCondos", "LRMSParcel", "AcctPrimary", "GisPrimary", "GisCityBLock", "GisParcel", "GisOwnerCode", "Handle", "Parcel9", "OwnerOcc", "FirstDate", "LastDate", "OwnerUpdate"
FROM "staging_1"."prcl_prcl"
WHERE "ParcelId" = '33000000100'

------------START WITH EVERYTHING EMPTY---------------
----INSERT NEWEST DATA FROM CITY INTO STAGING_1------(1)

-------------COMPARE STAGING_1 TO STAGING_2----------(2)
--insert new stuff into core
--mark dead stuff in core 
--------------WIPE STAGING 2 ------------------------(3)
DELETE FROM "staging_2"."prcl_bldgcom"
DELETE FROM "staging_2"."prcl_bldgres"
DELETE FROM "staging_2"."prcl_bldgsect"
DELETE FROM "staging_2"."prcl_prcl"
--------------COPY STAGING 1 INTO STAGING 2----------(4)
INSERT INTO "staging_2"."prcl_prcl"
SELECT * FROM "staging_1"."prcl_prcl"

INSERT INTO "staging_2"."prcl_bldgcom"
SELECT * FROM "staging_1"."prcl_bldgcom"

INSERT INTO "staging_2"."prcl_bldgres"
SELECT * FROM "staging_1"."prcl_bldgres"

INSERT INTO "staging_2"."prcl_bldgsect"
SELECT * FROM "staging_1"."prcl_bldgsect"
----------------WIPE STAGING 1-------------------------(5)
DELETE FROM "staging_1"."prcl_bldgcom"
DELETE FROM "staging_1"."prcl_bldgres"
DELETE FROM "staging_1"."prcl_bldgsect"
DELETE FROM "staging_1"."prcl_prcl"
----------------GO BACK TO STEP (1)!----------------------
