import psycopg2
import pandas as pd


def create_schema(database, host, username, password, port):
    # Create conncetion to database
    connection = psycopg2.connect(
        f"""
        host={host}
        dbname={database}
        port={port}
        user={username}
        password={password}
        """
    )

    # Create cursor to execute sql statements/queries
    cursor = connection.cursor()

    # Create query string to create staging_2 schema
    create_staging2_schema = "CREATE SCHEMA IF NOT EXISTS staging_2;"

    # Execute sql statement to create staging_2 schema
    cursor.execute(create_staging2_schema)

    # Commit changes to database
    connection.commit()

    # Close cursor and connection
    cursor.close()
    connection.close()


def create_tables(database, host, username, password, port):
    # Create conncetion to database
    connection = psycopg2.connect(
        f"""
        host={host}
        dbname={database}
        port={port}
        user={username}
        password={password}
        """
    )

    # Create cursor to execute sql statements/queries
    cursor = connection.cursor()

    # Retrieve list of table names using pandas
    query_table_names = "select table_name from information_schema.tables WHERE table_schema = 'staging_1'"
    df_tables = pd.read(query_table_names, connection)
    table_names = list(df_tables["table_name"])

    # Loop through table names and copy tables from staging_1
    # Commit changes made to database
    for table in table_names:
        try:
            query = f'CREATE TABLE IF NOT EXISTS staging_2.{table} AS TABLE staging_1."{table}" WITH NO DATA;'
            cursor.execute(query)
            connection.commit()
        except:
            print(f"Table {table} does not exist.")

    # Close cursor and connection
    cursor.close()
    connection.close()


def create_dead_parcels_table(database, host, username, password, port):
    # Create conncetion to database
    connection = psycopg2.connect(
        f"""
        host={host}
        dbname={database}
        port={port}
        user={username}
        password={password}
        """
    )

    # Create cursor to execute sql statements/queries
    cursor = connection.cursor()

    # Create the dead_parcels table with parcelID and a timestamp
    create_dead_parcels = "CREATE TABLE IF NOT EXISTS staging_2.dead_parcels (ParcelId VARCHAR(255), Date VARCHAR(255))"

    # Execute sql to create table
    cursor.execute(create_dead_parcels)
    connection.commit()

    # Close cursor and connection
    cursor.close()
    connection.close()


def create_dead_parcels_function(database, host, username, password, port):
    # Create conncetion to database
    connection = psycopg2.connect(
        f"""
        host={host}
        dbname={database}
        port={port}
        user={username}
        password={password}
        """
    )

    # Create cursor to execute sql statements/queries
    cursor = connection.cursor()

    # Create sql statement that will create a function that will find the dead parcels
    dead_parcels_function = """
        CREATE FUNCTION staging_2.find_dead_parcels ()
        RETURNS void AS $$
        BEGIN
        INSERT INTO staging_2.dead_parcels
            SELECT staging_2.Prcl."ParcelId", staging_2.prcl."OwnerUpdate"
            FROM staging_2.Prcl
            LEFT JOIN staging_1."Prcl"
                ON staging_2.Prcl."ParcelId" = staging_1."Prcl"."ParcelId"
            WHERE staging_1."Prcl"."ParcelId" IS NULL
        ;
        END;
        $$ LANGUAGE plpgsql;
        """


    # Execute command to create stored procedure to find dead parcels
    cursor.execute(dead_parcels_function)
    cursor.commit()

    cursor.close()
    connection.close()


def poulate_dead_parcels_table(database, host, username, password, port):
    # Create conncetion to database
    connection = psycopg2.connect(
        f"""
        host={host}
        dbname={database}
        port={port}
        user={username}
        password={password}
        """
    )

    # Create cursor to execute sql statements/queries
    cursor = connection.cursor()

    # Create sql statement to run stored procedure that will find dead parcels
    call_dead_parcels_function = 'SELECT staging_2.find_dead_parcels()'

    # Execute the stored procedure
    cursor.execute(call_dead_parcels_function)
    connection.commit()

    # close cursor and connection
    cursor.close()
    connection.close()


    # TODO check out OwnerUpdate field is the right field for dead_parcels table


def copy_data(database, host, username, password, port):
    # Create conncetion to database
    connection = psycopg2.connect(
        f"""
        host={host}
        dbname={database}
        port={port}
        user={username}
        password={password}
        """
    )

    # Create cursor to execute sql statements/querires
    cursor = connection.cursor()

    # Retrieve list of table names using pandas
    query_table_names = "select table_name from information_schema.tables WHERE table_schema = 'staging_1'"
    df_tables = pd.read(query_table_names, connection)
    table_names = list(df_tables["table_name"])
    
    # loop through table names and copy data from staging_1 to staging_2
    for table in table_names:
        try:
            query = f'INSERT INTO staging_2.{table} SELECT * FROM staging_1."{table}";'
            cursor.execture(query)
            connection.commit()
        except:
            print(f"Table {table} did not copy data.")

    # Close cursor and connection
    cursor.close()
    connection.close()
