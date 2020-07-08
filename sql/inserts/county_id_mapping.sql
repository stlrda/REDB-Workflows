DO $$
DECLARE
    aCountyID varchar;
    aCountyIDStripped varchar;
BEGIN

CREATE SEQUENCE IF NOT EXISTS history.parcel_id_seq START 10000;

aCountyID = (SELECT "county_id"::text
            FROM history.county
            WHERE county_name = 'Saint Louis City County');

aCountyIDStripped := SUBSTRING(aCountyID FROM 1 FOR 5);

INSERT INTO "history"."county_id_mapping"
    (
    county_id
    ,parcel_id
    ,county_parcel_id
    ,county_parcel_id_type
    )
    SELECT
        aCountyID
        ,CONCAT(aCountyIDStripped
            ,'.'
            ,LPAD(nextval('parcel_id_seq')::text, 8, '0')
            ,'.000.0000')
         ,"ParcelId"
        ,'parcel_11'
    FROM staging_1_2.prcl_prcl pp
    WHERE NOT EXISTS(
            SELECT
                   parcel_id
            FROM history.county_id_mapping hcim
            WHERE hcim.parcel_id = pp."ParcelId"
            );
END $$
