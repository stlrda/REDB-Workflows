CREATE OR REPLACE FUNCTION history.add_county
(
    IN name VARCHAR(50)
    ,IN state VARCHAR(3)
)
RETURNS SETOF "history"."county" AS
$BODY$
DECLARE
    aCounty_id varchar;
BEGIN
    CREATE SEQUENCE IF NOT EXISTS history.county_id_seq START 10000;

    aCounty_id := NEXTVAL('history.county_id_seq');
    aCounty_id = CONCAT(aCounty_id::text, '.00000000.000.0000');

    RETURN QUERY
        INSERT INTO "history"."county"
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


CREATE OR REPLACE FUNCTION history.format_parcel_address
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

