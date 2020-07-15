CREATE OR REPLACE FUNCTION core.create_schemas()
RETURNS void AS $$
BEGIN
    CREATE SCHEMA IF NOT EXISTS staging_1;
    CREATE SCHEMA IF NOT EXISTS staging_2;
    CREATE SCHEMA IF NOT EXISTS core;
END;
$$
LANGUAGE plpgsql;

--------------------------

core.create_schemas()