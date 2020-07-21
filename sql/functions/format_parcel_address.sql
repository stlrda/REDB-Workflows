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
