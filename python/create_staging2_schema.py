import psycopg2


# Create conncetion to database
connection = psycopg2.connect(
    f"""
    host={redb_host}
    database={redb_dbname}
    port={redb_port}
    user={redb_username}
    password={redb_password}
    """
)

# Create cursor to execute sql statements/queries
cursor = connection.cursor()

# Read sql file to get sql statements/queries
file = open("./sql/staging_2/create_staging2_schema.sql", "r")
create_staging2_schema = file.read()
file.close()


cursor.execute(create_staging2_schema)
