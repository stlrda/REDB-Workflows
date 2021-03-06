----Function for creating all the tables used in core and any constraints/indexes/sequences necessary for them to work------------
CREATE OR REPLACE FUNCTION core.create_core_tables()
RETURNS void AS $$
BEGIN

--Creates table for county IDs----------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.county (
    county_id varchar PRIMARY KEY
    , county_name varchar
    , county_state varchar
    );

--Creates table for neighborhood IDs----------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.neighborhood (
    neighborhood_id SERIAL PRIMARY KEY
    , neighborhood_name varchar CONSTRAINT UC_Neighborhood UNIQUE
    , county_id varchar
    , create_date date
    , current_flag boolean
    , removed_flag boolean
    , etl_job varchar
    , update_date date
    );

--Creates table for address ids which are uniquely assigned via the serial Primary Key "address_id"-------------------------------
CREATE TABLE IF NOT EXISTS core.address (
    address_id SERIAL PRIMARY KEY
    , street_address varchar
    , county_id varchar
    , city varchar
    , state varchar
    , country varchar
    , zip varchar
    , create_date date
    , current_flag boolean
    , removed_flag boolean
    , etl_job varchar
    , update_date date
    );
	
-- Unique Index is necessary to account for potential nulls in address fields.
CREATE UNIQUE INDEX IF NOT EXISTS UI_OwnerAddress ON "core"."address"(COALESCE("street_address", 'NULL')
	, COALESCE("city", 'NULL')
    , COALESCE("state", 'NULL')
    , COALESCE("country", 'NULL')
    , COALESCE("zip", 'NULL')
	);

--Creates the table and constraint for Mapping Parcel11 IDs to REDb IDs-----------------------------------------------------------
CREATE TABLE IF NOT EXISTS "core"."county_id_mapping_table" (
	county_id varchar -- county_id
	, parcel_id varchar PRIMARY KEY -- REDB identifier
	, county_parcel_id varchar CONSTRAINT UC_Mapping UNIQUE -- The identifier the county uses
	, county_parcel_id_type varchar -- The name the county uses to refer to their identifier EG:'parcel_11'
	, create_date date
	, current_flag boolean
	, removed_flag boolean
	, etl_job varchar
	, update_date date
	);

CREATE SEQUENCE IF NOT EXISTS core.id_mapping
INCREMENT BY 1 
START 10000001
OWNED BY core.county_id_mapping_table."county_parcel_id";

--Creates the table and index for assigning unique Legal_Entity IDs.-------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.legal_entity (
    legal_entity_id SERIAL PRIMARY KEY
    , legal_entity_address varchar
    , legal_entity_name varchar
    , legal_entity_secondary_name varchar
    , address_id integer
    , create_date date
    , current_flag boolean
    , removed_flag boolean
    , etl_job varchar
    , update_date date
    );

--index necessary to account for potential nulls in fields needed to create legal entity
CREATE UNIQUE INDEX IF NOT EXISTS UI_Legal_Entity ON "core"."legal_entity" (COALESCE("legal_entity_address", 'NULL_ADDRESS')
, COALESCE("legal_entity_name", 'NULL_NAME_1')
, COALESCE("legal_entity_secondary_name", 'NULL_NAME_2')
, "address_id");

--Creates the table for assigning unique Parcel IDs.----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "core"."parcel" (
    "parcel_id" varchar -- CCCCCC.PPPPPPPP.000.0000 (county_id.parcel_number.building_number.unit_number)
    , "county_id" varchar -- County_Id 10001 because all the data is coming from one county at the moment but this needs to be more sophisticated down the line
	, "address_id" BIGINT
    , "city_block_number" varchar -- prcl.CityBlock
    , "parcel_number" varchar -- generated with a sequence starting at 10000001
    --, "parcel_taxing_status" varchar -- May be coming from a different table don't know for now.
    --, "primary_addresses_count" varchar -- DROP
    , "owner_id" varchar -- core.legal_entity.legal_entity_id
    , "description" varchar -- CONCAT(prcl.LegalDesc1,' ',prcl.LegalDesc2,' ',prcl.LegalDesc3,' ',prcl.LegalDesc4,' ',prcl.LegalDesc5)
    , "frontage_to_street" int -- prcl.Frontage
    , "land_area" int -- prcl.LandArea
    , "zoning_class" varchar -- prcl.Zoning
    , "ward" varchar -- prcl.Ward10
    , "voting_precinct" varchar -- prcl.Precinct10
    , "inspection_area" varchar -- prcl.InspArea10
    , "neighborhood_id" varchar -- core.neighborhood.neighborhood_id
    , "police_district" varchar -- prcl.PoliceDist
    , "census_tract" varchar -- prcl.CensTract10
    --, "census_block" varchar -- DROP
    , "asr_neighborhood" varchar -- prcl.AsrNbrhd
    , "special_parcel_type_code" varchar -- prcl.SpecParcelType
    , "sub_parcel_type_code" varchar -- prcl.SubParcelType
    , "gis_city_block" varchar -- prcl.GisCityBLock (That's Capital BL in BLock because the city data sucks)
    , "gis_parcel" varchar --prcl.GisParcel
    , "gis_owner_code" varchar --prcl.GisOwnerCode
    , "create_date" date  -- NYI
    , "current_flag" boolean -- NYI
    , "removed_flag" boolean -- NYI
    , "etl_job" varchar -- NYI
    , "update_date" date -- NYI
);

CREATE UNIQUE INDEX IF NOT EXISTS UI_Active_Parcel ON "core"."parcel"(parcel_id, current_flag) WHERE current_flag = TRUE;

CREATE UNIQUE INDEX IF NOT EXISTS UI_Parcel ON "core"."parcel"(COALESCE("parcel_id", 'NULL')
    , COALESCE("county_id", 'NULL')
    , COALESCE("city_block_number", 'NULL')
    , COALESCE("parcel_number", 'NULL')
    , COALESCE("owner_id", 'NULL')
    , COALESCE("description", 'NULL')
    , COALESCE("frontage_to_street", 666)
    , COALESCE("land_area", 666)
    , COALESCE("zoning_class", 'NULL')
    , COALESCE("ward", 'NULL')
    , COALESCE("voting_precinct", 'NULL')
    , COALESCE("inspection_area", 'NULL')
    , COALESCE("neighborhood_id", 'NULL')
    , COALESCE("police_district", 'NULL')
    , COALESCE("census_tract", 'NULL')
    , COALESCE("asr_neighborhood", 'NULL')
    , COALESCE("special_parcel_type_code", 'NULL')
    , COALESCE("sub_parcel_type_code", 'NULL')
    , COALESCE("gis_city_block", 'NULL')
    , COALESCE("gis_parcel", 'NULL')
    , COALESCE("gis_owner_code", 'NULL')
    );

--Creates the table for assigning unique Building IDs.--------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "core"."building" (
    "parcel_id" varchar
	, "building_id" varchar -- CCCCCC.PPPPPPPP.BBB.0000 (county_id.parcel_number.building_number.unit_number)
	, "owner_id" varchar
	, "description" varchar
	, "apartment_count" int
	, "create_date" date
	, "current_flag" boolean
	, "removed_flag" boolean
	, "etl_job" varchar
	, "update_date" date
);

CREATE UNIQUE INDEX IF NOT EXISTS UI_Active_Building ON "core"."building"(building_id, current_flag) WHERE current_flag = TRUE;

CREATE UNIQUE INDEX IF NOT EXISTS UI_Building ON "core"."building"(COALESCE("parcel_id", 'NULL')
	, COALESCE("building_id", 'NULL')
	, COALESCE("owner_id", 'NULL')
	, COALESCE("description", 'NULL')
	, COALESCE("apartment_count", '777')
	);
	
--Creates the table for assigning unique Unit IDs.------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "core"."unit" (
    "unit_id" varchar -- CCCCCC.PPPPPPPP.BBB.UUUU (county_id.parcel_number.building_number.unit_number)
	, "building_id" varchar
	, "owner_id" varchar
	, "description" varchar
	, "condominium" boolean
	, "create_date" date
	, "current_flag" boolean
	, "removed_flag" boolean
	, "etl_job" varchar
	, "update_date" date
);

CREATE UNIQUE INDEX IF NOT EXISTS UI_Active_Unit ON "core"."unit"(unit_id, current_flag) WHERE current_flag = TRUE;

CREATE UNIQUE INDEX IF NOT EXISTS UI_Unit ON "core"."unit"(COALESCE("unit_id", 'NULL')
	, COALESCE("building_id", 'NULL')
	, COALESCE("owner_id", 'NULL')
	, COALESCE("description", 'NULL')
	, "condominium"
	);
	
END;
$$
LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION core.format_parcelId
	(
		IN CityBlock varchar(1000)
		,IN Parcel varchar(1000)
		,IN OwnerCode varchar(1000)
	)
RETURNS text AS
$BODY$
DECLARE
	ParcelId varchar;
BEGIN

--ParcelId := replace(replace(concat(to_char(CityBlock::float8,'0000.00'),to_char(Parcel::int8,'0000'),OwnerCode),'.',''),' ','');
ParcelId := (concat(to_char(CityBlock::float8,'0000.00'),to_char(Parcel::int8,'0000'),OwnerCode));
ParcelId := replace((ParcelId), '.', '');
ParcelId := replace((ParcelId), ' ', '');

RETURN ParcelId;

END
$BODY$
LANGUAGE plpgsql;

---------------------------------------------------------
CREATE OR REPLACE FUNCTION core.staging1_to_staging2()
RETURNS void AS $$
BEGIN

--Clear staging_2
DELETE FROM "staging_2"."prcl_bldgcom";
DELETE FROM "staging_2"."prcl_bldgres";
DELETE FROM "staging_2"."prcl_bldgsect";
DELETE FROM "staging_2"."prcl_prcl";

--Move data from staging_1 to staging_2
INSERT INTO "staging_2"."prcl_prcl"
SELECT * FROM "staging_1"."prcl_prcl";

INSERT INTO "staging_2"."prcl_bldgcom"
SELECT * FROM "staging_1"."prcl_bldgcom";

INSERT INTO "staging_2"."prcl_bldgres"
SELECT * FROM "staging_1"."prcl_bldgres";

INSERT INTO "staging_2"."prcl_bldgsect"
SELECT * FROM "staging_1"."prcl_bldgsect";

--Clear staging_1
DELETE FROM "staging_1"."prcl_bldgcom";
DELETE FROM "staging_1"."prcl_bldgres";
DELETE FROM "staging_1"."prcl_bldgsect";
DELETE FROM "staging_1"."prcl_prcl";

END;
$$
LANGUAGE plpgsql;

-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION core.insert_week1_to_staging1()
RETURNS void AS $$
BEGIN

INSERT INTO "staging_1"."prcl_prcl"
SELECT * FROM "staging_1"."week_1_prcl";

INSERT INTO "staging_1"."prcl_bldgcom"
SELECT * FROM "staging_1"."week_1_bldgcom";

INSERT INTO "staging_1"."prcl_bldgres"
SELECT * FROM "staging_1"."week_1_bldgres";

INSERT INTO "staging_1"."prcl_bldgsect"
SELECT * FROM "staging_1"."week_1_bldgsect";

END;
$$
LANGUAGE plpgsql;
---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION core.insert_week2_to_staging1()
RETURNS void AS $$
BEGIN

INSERT INTO "staging_1"."prcl_prcl"
SELECT * FROM "staging_1"."week_2_prcl";

INSERT INTO "staging_1"."prcl_bldgcom"
SELECT * FROM "staging_1"."week_2_bldgcom";

INSERT INTO "staging_1"."prcl_bldgres"
SELECT * FROM "staging_1"."week_2_bldgres";

INSERT INTO "staging_1"."prcl_bldgsect"
SELECT * FROM "staging_1"."week_2_bldgsect";

END;
$$
LANGUAGE plpgsql;

---------------------------------------------------------
CREATE OR REPLACE FUNCTION core.format_parcel_address
(
	IN _row staging_1.prcl_prcl
)
RETURNS text AS
$BODY$
DECLARE
	val varchar;
	Low varchar = _row."LowAddrNum";
	High varchar = _row."HighAddrNum";
	StPreDir varchar = COALESCE(_row."StPreDir", '');
	StName varchar = _row."StName";
	StType varchar = _row."StType";
	AddrSuf varchar = COALESCE(_row."LowAddrSuf", _row."HighAddrSuf", '');
	address varchar;
BEGIN
	IF Low = High THEN
		val := Low;
	ELSE
		val := CONCAT(Low, '-', High);
	END IF;
	
	IF StPreDir = '' THEN
		address := CONCAT(val, ' ', StName, ' ', StType);
	ELSE
		address := CONCAT(val, ' ', StPreDir, ' ', StName, ' ', StType);
	END IF;
	
	IF AddrSuf != '' THEN
		address := CONCAT(address, ' ', '#', AddrSuf);
	END IF;
	
	RETURN address;
END
$BODY$
LANGUAGE plpgsql;

---------------------------------------------------------
CREATE OR REPLACE FUNCTION core.add_county
(
    IN name VARCHAR(50)
    ,IN state VARCHAR(3)
)
RETURNS SETOF "core"."county" AS
$BODY$
DECLARE
    aCounty_id varchar;
BEGIN
    CREATE SEQUENCE IF NOT EXISTS core.county_id_seq START 10001 OWNED BY core.county."county_id";

    aCounty_id := NEXTVAL('core.county_id_seq');
    aCounty_id = CONCAT(aCounty_id::text, '.00000000.000.0000');

    RETURN QUERY
        INSERT INTO "core"."county"
        (
            "county_id"
            ,"county_name"
            ,"county_state"
        )
        VALUES
            (
                aCounty_id
                ,name
                ,state
            )
        RETURNING *;
END
$BODY$
LANGUAGE plpgsql;

---------------------------------------------------------
CREATE OR REPLACE FUNCTION core.create_current_views()
RETURNS void AS $$
BEGIN

CREATE OR REPLACE VIEW "core".current_neighborhood AS
(
	SELECT "neighborhood_id"
		, "neighborhood_name"
		, "county_id"
	FROM "core"."neighborhood"
	WHERE "current_flag" = TRUE
);

CREATE OR REPLACE VIEW "core".current_address AS
(
	SELECT "address_id"
		, "street_address"
		, "county_id"
		, "city"
		, "state"
		, "country"
		, "zip"
	FROM "core"."address"
	WHERE "current_flag" = TRUE
);

CREATE OR REPLACE VIEW "core".current_county_id_mapping AS
(
	SELECT "county_id"
		, "parcel_id"
		, "county_parcel_id"
		, "county_parcel_id_type"
	FROM "core"."county_id_mapping_table"
	WHERE "current_flag" = TRUE
);

CREATE OR REPLACE VIEW "core".current_legal_entity AS
(
	SELECT "legal_entity_id"
		, "legal_entity_address"
		, "legal_entity_name"
		, "legal_entity_secondary_name"
		, "address_id"
	FROM "core"."legal_entity"
	WHERE "current_flag" = TRUE
);

CREATE OR REPLACE VIEW "core".current_parcel AS
(
	SELECT parcel_id
		, county_id
		, address_id
		, city_block_number
		, parcel_number
		, owner_id
		, description
		, frontage_to_street
		, land_area
		, zoning_class
		, ward
		, voting_precinct
		, inspection_area
		, neighborhood_id
		, police_district
		, census_tract
		, asr_neighborhood
		, special_parcel_type_code
		, sub_parcel_type_code
		, gis_city_block
		, gis_parcel
		, gis_owner_code
	FROM core.parcel
	WHERE "current_flag" = TRUE
);

CREATE OR REPLACE VIEW "core".current_building AS
(
	SELECT "parcel_id"
		, "building_id"
		, "owner_id"
		, "description"
		, "apartment_count"
	FROM "core"."building"
	WHERE "current_flag" = TRUE
);

CREATE OR REPLACE VIEW "core".current_unit AS
(
	SELECT "building_id"
		, "unit_id"
		, "description"
		, "condominium"
	FROM "core"."unit"
	WHERE "current_flag" = TRUE
);

END;
$$
LANGUAGE plpgsql;

-- Triggers aren't currently working out in prod 
-- I believe the issue is update functions are switching the removed flag
-- back to true after they are switched to match a row that has it's current flag changed
-- will need to revisit this later when time allows.  KB 7/21/20
---------------------------------------------------
-- CREATE OR REPLACE FUNCTION core.dead_parcel_proc()
-- RETURNS TRIGGER AS 
-- $$
-- BEGIN

-- 	UPDATE "core"."parcel" 
-- 	SET "removed_flag" = NEW."removed_flag"
-- 	WHERE "parcel"."parcel_id" = NEW."parcel_id";
-- 	RETURN NULL;

-- END;
-- $$
-- LANGUAGE plpgsql;

-- CREATE TRIGGER dead_parcel_trigger 
-- AFTER INSERT OR UPDATE 
-- OF "current_flag"
-- ON "core"."parcel"
-- FOR EACH ROW
-- EXECUTE PROCEDURE core.dead_parcel_proc();
-- ---------------------------------------------------
-- CREATE OR REPLACE FUNCTION core.dead_building_proc()
-- RETURNS TRIGGER AS 
-- $$
-- BEGIN

-- 	UPDATE "core"."building" 
-- 	SET "removed_flag" = NEW."removed_flag"
-- 	WHERE "building"."building_id" = NEW."building_id";
-- 	RETURN NULL;

-- END;
-- $$
-- LANGUAGE plpgsql;

-- CREATE TRIGGER dead_building_trigger 
-- AFTER INSERT OR UPDATE 
-- OF "current_flag"
-- ON "core"."building"
-- FOR EACH ROW
-- EXECUTE PROCEDURE core.dead_building_proc();
-- ---------------------------------------------------
-- CREATE OR REPLACE FUNCTION core.dead_unit_proc()
-- RETURNS TRIGGER AS 
-- $$
-- BEGIN

-- 	UPDATE "core"."unit" 
-- 	SET "removed_flag" = NEW."removed_flag"
-- 	WHERE "unit"."unit_id" = NEW."unit_id";
-- 	RETURN NULL;

-- END;
-- $$
-- LANGUAGE plpgsql;

-- CREATE TRIGGER dead_unit_trigger 
-- AFTER INSERT OR UPDATE 
-- OF "current_flag"
-- ON "core"."unit"
-- FOR EACH ROW
-- EXECUTE PROCEDURE core.dead_unit_proc();
-----------------------------------------------
CREATE OR REPLACE FUNCTION core.row_level_format_street_address
(
	IN LowAddrNum varchar(1000)
	, IN HighAddrNum varchar(1000)
	, IN StPreDir varchar(1000)
	, IN StName varchar(1000)
	, IN StType varchar(1000)
	, IN LowAddrSuf varchar(1000)
	, IN HighAddrSuf varchar(1000)
)
RETURNS text AS
$BODY$
DECLARE
	val varchar;
	AddrSuf varchar = COALESCE(LowAddrSuf, HighAddrSuf, '');
	address varchar;
BEGIN
	
	StPreDir = COALESCE(StPreDir, '');
	
	IF LowAddrNum = HighAddrNum THEN
		val := LowAddrNum;
	ELSE
		val := CONCAT(LowAddrNum, '-', HighAddrNum);
	END IF;
	
	IF StPreDir = '' THEN
		address := CONCAT(val, ' ', StName, ' ', StType);
	ELSE
		address := CONCAT(val, ' ', StPreDir, ' ', StName, ' ', StType);
	END IF;
	
	IF AddrSuf != '' THEN
		address := CONCAT(address, ' ', '#', AddrSuf);
	END IF;
	
	RETURN address;
	
END
$BODY$
LANGUAGE plpgsql;