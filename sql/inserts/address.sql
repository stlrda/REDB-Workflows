BEGIN;

WITH formatted_addresses AS (
	SELECT DISTINCT
		(SELECT history.format_parcel_address(prcl_table)) as street_address
		,(SELECT county_id FROM history.county WHERE county.county_name = 'Saint Louis City County') as county_id
		,'MO' as state
		,'USA' as country
		,"ZIP" as zip
	FROM staging_1_2.prcl_prcl prcl_table
)
INSERT INTO history.address
	(
	 street_address
	 ,county_id
	 ,state
	 ,country
	 ,zip
	)
	SELECT
		*
	FROM formatted_addresses fa
    WHERE NOT EXISTS(
        SELECT
               street_address
        FROM history.address ha
        WHERE ha.street_address = fa.street_address
        )
    RETURNING *;


WITH owner_addresses AS (
	SELECT DISTINCT
		"OwnerAddr" as street_address
		,"OwnerCity" as city
		,"OwnerState" as state
		,"OwnerCountry" as country
		,"OwnerZIP" as zip
		,(SELECT county_id FROM history.county WHERE county.county_name = 'Saint Louis City County') as county_id
	FROM staging_1_2.prcl_prcl
)
INSERT INTO history.address
	(
	"street_address"
    ,"city"
    ,"state"
    ,"country"
    ,"zip"
	,"county_id"
	)
	SELECT
		*
	FROM owner_addresses oa
    WHERE NOT EXISTS(
        SELECT
               street_address
        FROM history.address ha
        WHERE CONCAT(ha.street_address, ha.city, ha.state, ha.country, ha.zip, ha.county_id) = CONCAT(oa.street_address, oa.city, oa.state, oa.country, oa.zip, oa.county_id)
        )
    RETURNING *;

COMMIT;