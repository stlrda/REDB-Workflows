CREATE TABLE IF NOT EXISTS "core"."sub_parcel_type" (
    sub_parcel_type_code varchar
    , sub_parcel_type varchar
);

-- Creates and inserts values into the sub_parcel_type lookup table.
INSERT INTO "core"."sub_parcel_type" (sub_parcel_type_code, sub_parcel_type) 
VALUES ('A', 'Account Separation')
    , ('B', 'Back Taxes Owed')
    , ('C', 'Condo')
    , ('D', 'Deleted')
    , ('E', '11th Digit')
    , ('G', 'Garage/Parking Condo')
    , ('I', 'Industrial Condo')
    , ('K', 'Commercial Condo')
    , ('M', 'Multi-Owner')
    , ('P', E'Shouldn\'t be retired')
    , ('Q', 'Should be retired')
    , ('R', 'Retired')
    , ('X', 'Other')
    , ('Y', 'Old retired')