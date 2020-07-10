-- ParcelId Is included in the WHERE but not in the join because it is possible for a parcel to have all of the fields
-- used to make an address id be null. Howerver, ParcelId is only null if the adress fields cannot be joined on
------------Insert New addresses into core historical-------------------
CREATE OR REPLACE FUNCTION core.new_address()
RETURNS void AS $$
BEGIN
    WITH formatted_addresses AS (
        SELECT DISTINCT
            (SELECT core.format_parcel_address(prcl_table)) as street_address
            ,(SELECT county_id FROM core.county WHERE county.county_name = 'Saint Louis City County') as county_id
            ,'' as city
            ,'MO' as state
            ,'USA' as country
            ,"ZIP" as zip
        FROM staging_1.prcl_prcl prcl_table
    )
    INSERT INTO core.address
        (
        street_address
        ,county_id
        ,city
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
            FROM core.address ca
            WHERE ca.street_address = fa.street_address
                AND ca.current_flag = True
            )
            ON CONFLICT ON CONSTRAINT UC_Address DO UPDATE
                SET "current_flag" = TRUE
                    , "removed_flag" = FALSE
                    , "update_date" = CURRENT_DATE;



    WITH owner_addresses AS (
        SELECT DISTINCT
            "OwnerAddr" as street_address
            ,"OwnerCity" as city
            ,"OwnerState" as state
            ,"OwnerCountry" as country
            ,"OwnerZIP" as zip
            ,(SELECT county_id FROM core.county WHERE county.county_name = 'Saint Louis City County') as county_id
        FROM staging_1.prcl_prcl
    )
    INSERT INTO core.address
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
            FROM core.address ca
            WHERE CONCAT(ca.street_address, ca.city, ca.state, ca.country, ca.zip, ca.county_id) = CONCAT(oa.street_address, oa.city, oa.state, oa.country, oa.zip, oa.county_id)
                AND ca.current_flag = True
            )
            ON CONFLICT ON CONSTRAINT UC_Address DO UPDATE
                SET "current_flag" = TRUE
                    , "removed_flag" = FALSE
                    , "update_date" = CURRENT_DATE;
END;
$$
LANGUAGE plpgsql;
-------------------------
SELECT core.new_address();