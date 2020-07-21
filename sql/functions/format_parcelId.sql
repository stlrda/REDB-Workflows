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