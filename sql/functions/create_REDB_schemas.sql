CREATE OR REPLACE FUNCTION create_schemas()
RETURNS void AS $$
BEGIN
    CREATE SCHEMA IF NOT EXISTS staging_1;
    CREATE SCHEMA IF NOT EXISTS staging_2;
    CREATE SCHEMA IF NOT EXISTS core;
END;
$$
LANGUAGE plpgsql;

--------------------------

SELECT create_schemas()