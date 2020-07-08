from .Database import Database

def main(**kwargs):
    """Connects to database, creates 'core' schema, and adds table defintions.
    """
    PG_DATABASE = kwargs["pg_database"]
    PG_HOST = kwargs["pg_host"]
    PG_USER = kwargs["pg_user"]
    PG_PASSWORD = kwargs["pg_password"]
    PG_PORT = kwargs["pg_port"]

    DB = Database(PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, PG_DATABASE)

    DB.create_schema("core")

    with open("/usr/local/airflow/dags/efs/redb/sql/core-table-definitions/Interrim REDB Schema.sql", "r") as sql:
        query = ""
        for line in sql:
            query += line

    conn = DB.get_raw_connection()
    curr = conn.cursor()
    curr.execute("SET search_path TO core;")
    curr.execute(query)
    conn.commit()
