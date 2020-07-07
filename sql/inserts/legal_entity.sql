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

-- Selects each unique combination of OwnerAddr, OwnerName, OwnerName2 & address_id
WITH LegalEntity AS
    (
    /* inner query selects Address_ids along with OwnerAddr and Name fields needed to create unique legal_entity_ids    
    Coalesce is necessary to correctly join on fields that may contain null values */
    WITH Qry AS 
        (
        SELECT "ParcelId"
            , "OwnerName"
            , "OwnerName2"
            , "address_id"
            , "OwnerAddr"
            , "OwnerCity"
            , "OwnerState"
            , "OwnerCountry"
            , "OwnerZIP" 
        FROM "core"."address"
        JOIN "staging_2"."prcl_prcl" 
        ON COALESCE("OwnerAddr", ' ') = COALESCE("street_address", ' ')
        AND COALESCE("OwnerCity", ' ') = COALESCE("city", ' ') 
        AND COALESCE("OwnerState", ' ') = COALESCE("state", ' ')
        AND COALESCE("OwnerCountry", ' ') = COALESCE("country", ' ') 
        AND COALESCE("OwnerZIP", ' ') = COALESCE("zip", ' ')
        )
    SELECT "OwnerAddr"
        , "OwnerName"
        , "OwnerName2"
        , "address_id"
    FROM Qry
    GROUP BY "OwnerAddr", "OwnerName", "OwnerName2", "address_id"
    ORDER BY "address_id"
    )
-- inserts unique group values and assigns a legal_entity_id via Serial Primary Key
INSERT INTO core.legal_entity(
    legal_entity_address
    , legal_entity_name
    , legal_entity_secondary_name
    , address_id
    ) 
SELECT "OwnerAddr"
    , "OwnerName"
    , "OwnerName2"
    , "address_id" 
FROM LegalEntity 
GROUP BY "OwnerAddr", "OwnerName", "OwnerName2", "address_id";