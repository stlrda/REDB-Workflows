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