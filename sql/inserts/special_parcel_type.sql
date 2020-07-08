CREATE TABLE IF NOT EXISTS "core"."special_parcel_type" (
    special_parcel_type_code varchar
    , special_parcel_type varchar
);

-- Creates and inserts values into the special_parcel_type lookup table.
INSERT INTO "core"."special_parcel_type" (special_parcel_type_code, special_parcel_type) 
VALUES ('C', 'Condo Master - Res/Mixed')
    , ('H', 'Highway ROW')
    , ('K', 'Condo Master - Non-Res')
    , ('R', 'Other ROW')
    , ('S', 'Special Account')
    , ('W', 'Water')
    , ('X', 'Non-parcel Area')