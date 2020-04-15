import psycopg2
# from credentials import redb_host, redb_dbname, redb_port, redb_username, redb_password

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

cursor = connection.cursor()

sql_statement_create_schema = """
    CREATE SCHEMA IF NOT EXISTS staging_2;
    """

cursor.execute(sql_statement_create_schema)