import psycopg2


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


    # Read sql file to get sql statement that will create the staging_2 schema
    schema_file = open("./sql/staging_2/create_staging2_schema.sql", "r")
    create_staging2_schema = schema_file.read()
    schema_file.close()

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


    # Read sql file to get sql statements that will create the tables within staging_2 schema
    create_tables_file = open("./sql/staging_2/create_tables.sql", "r")
    create_tables = create_tables_file.read()
    create_tables_file.close()

    # split sql
    statements = create_tables.split(";")

    # Loop through the create table commands to pull out each individual sql statement
    # Execute sql statements to create tables
    # Commit changes made to database
    for statement in statements:
        cursor.execute(statement)
        connection.commit()

    # Close cursor and connection
    cursor.close()
    connection.close()
