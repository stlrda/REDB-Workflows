-- TODO Add ON CONFLICT DO NOTHING so script can be ran weekly.
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
    CREATE SEQUENCE IF NOT EXISTS core.county_id_seq START 10001;

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