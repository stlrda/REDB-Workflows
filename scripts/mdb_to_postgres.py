# Standard library
import os
import csv
import tempfile
from subprocess import (check_output, CalledProcessError)

# Third party
import pandas as pd

# Custom
from .classes.S3 import S3
from .classes.Database import Database
from .utils.custom_logging import print_time
from .utils.data_transformations import (convert_scientific_notation
                                        , generate_rows)


def initializeIO(kwargs):
    """Initializes S3 Bucket and Database using the credentials passed in via Dictionary.
    Returns S3 Bucket and Database object as tuple.
    Expecting : {credential_name : credential_value, ...}
    """

    BUCKET_NAME = kwargs["bucket"]
    AWS_ACCESS_KEY_ID = kwargs["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = kwargs["aws_secret_access_key"]

    PG_DATABASE = kwargs["pg_database"]
    PG_HOST = kwargs["pg_host"]
    PG_USER = kwargs["pg_user"]
    PG_PASSWORD = kwargs["pg_password"]
    PG_PORT = kwargs["pg_port"]

    s3 = S3(BUCKET_NAME, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    db = Database(PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, PG_DATABASE)
    db.create_schema("staging_1")

    return (s3, db)


def get_tables(path_to_database):
    """ Returns a list of every table within a .mdb file.

    :param path_to_database: A path to desired .mdb database.
    """

    try:
        # Will not work via DAG in Airflow.
        tables = check_output(["mdb-tables", path_to_database])

    except CalledProcessError as exc:
        # Will assign the same value as the try block, but DOES work via Airflow.
        tables = exc.output

    tables = tables.decode().split()
    print(f'Tables found = {tables}')

    return tables


def get_table_columns(table, path_to_database):
    """ Returns a list of every column name for a specified .mdb table.
    :param table: Name of table to search for columns.
    :param path_to_database: A path to .mdb database to search for table.
    """

    columns = []

    try:
        # Will not work via DAG in Airflow.
        arr = check_output(["mdb-schema", "--table", table, path_to_database])

    except CalledProcessError as exc:
        # Will assign the same value as the try block, but DOES work via Airflow.
        arr = exc.output

    arr = arr.decode().split()

    for index, element in enumerate(arr):
        if element.startswith("[") and element.endswith("]"):
            column_name = element.replace("[", "").replace("]", "")
            column_type = arr[index + 1]
            column = column_name
            columns.append(column)

    columns.pop(0) # The first column is just the table name and an open bracket.

    print(f'Columns ({len(columns)}) for table: {table} @ {path_to_database}:\n{columns}')

    return columns


def initialize_csv(table, columns, row, csv_path, limit=50_000):
    """ Creates a CSV from a Python Generator with a select number of rows.

    :param table: The name of the table and target CSV.
    :param limit: The amount of rows appended to CSV at a time.

    The "batch" list will store @limit number of rows into RAM before
    bulk transformation to Pandas DataFrame. That DataFrame then
    creates the initial CSV.
    """

    batch = []

    try:
        snapshot_row = next(row)
    except StopIteration:
        print(f"{table} doesn't have any rows or rows are not valid. Skipping...")
        row = None
        return None

    snapshot_row = convert_scientific_notation(snapshot_row)
    batch.append(snapshot_row)
    rows_generated = 1

    print_time("init_start", table)

    while (rows_generated < limit) and (row != None):
        try:
            current_row = next(row)
            current_row = convert_scientific_notation(current_row)
            batch.append(current_row)
            rows_generated += 1

        # Python 3.4 returns "StopIteration" once Generator reaches the end.
        except StopIteration:
            row = None
            break
    
    table_dataframe = pd.DataFrame(batch, columns=columns)
    batch.clear()
    table_dataframe.to_csv(csv_path,
                           index=False,
                           sep="|",
                           na_rep="",
                           line_terminator="\r\n",
                           encoding="utf-8")
                           
    print_time("init_complete", table)

    return csv_path


def append_to_csv(table, columns, row, csv_path, limit=50_000):
    """Appends table rows from a Python Generator to an existing CSV.

    :param table: The name of the table and target CSV.
    :param limit: The amount of rows appended to CSV at a time.

    The "batch" list will store @limit number of rows into RAM before
    bulk transformation to Pandas DataFrame. That DataFrame is then
    appended to the CSV.
    """

    batch = []
    rows_generated = 0

    print_time("append_start", table)

    while (rows_generated < limit) and (row != None):
        try:
            current_row = next(row)
            current_row = convert_scientific_notation(current_row)
            batch.append(current_row)
            rows_generated += 1

        # Python 3.4 returns "StopIteration" once Generator reaches the end.
        except StopIteration:
            row = None
            break
    
    table_dataframe = pd.DataFrame(batch, columns=columns)
    batch.clear()
    table_dataframe.to_csv(csv_path,
                            header=False,
                            index=False,
                            mode="a",
                            sep="|",
                            line_terminator="\r\n",
                            encoding="utf-8")

    print_time("append_complete", table)

    if row != None:
        # Iteration is preferred here as opposed to recursion to save on RAM.
        append_to_csv(table, columns, row, csv_path, limit)
    else:
        return csv_path


def copy_csv_to_database(redb_table_name, columns, csv_path, db):
    """
    """
    try:
        f = open(csv_path, 'r')
        f.close()
    except Exception as err:
        print(f"WARNING: {err} - Table being skipped.")
        return False
    
    # Opens CSV then copies to database.
    with open(csv_path, 'r+') as csvfile:
        db.replace_table("staging_1", redb_table_name, columns) # snapshot_row created upon invoking "initialize_csv" function.
        conn = db.get_raw_connection()
        cursor = conn.cursor()
        cmd = f'COPY staging_1."{redb_table_name}" FROM STDIN WITH (FORMAT CSV, DELIMITER "|", HEADER TRUE, ENCODING "utf-8")'
        cursor.copy_expert(cmd, csvfile)
        conn.commit()
        print(f"{redb_table_name} copied into staging_1.")
    return True


def main(**kwargs):
    """Downloads .mdb files from S3 Bucket,
    creates CSVs for each table within .mdb file, then copies CSV
    to target database (one table/csv at a time).

    :param bucket:
    :param aws_access_key_id:
    :param aws_secret_access_key:

    :param pg_database:
    :param pg_host:
    :param pg_password:
    :param pg_port:
    """

    (s3, db) = initializeIO(kwargs)
    access_files_in_s3 = s3.list_objects(extension=".mdb", field="Key")

    for access_file in access_files_in_s3:
        with tempfile.TemporaryDirectory() as tmp:
            path_to_database =  os.path.join(tmp, access_file)
            s3.download_file(s3.bucket_name, access_file, path_to_database)

            for city_table_name in get_tables(path_to_database):
                columns = get_table_columns(city_table_name, path_to_database)
                access_name = access_file[:-4] # name of Access database
                redb_table_name = access_name.lower() + "_" + city_table_name.lower()
                csv_path = os.path.join(tmp, f"{redb_table_name}.csv")
                row = generate_rows(path_to_database, city_table_name)

                initialize_csv(redb_table_name, columns, row, csv_path)

                if row != None:
                    append_to_csv(redb_table_name, columns, row, csv_path)

                copy_csv_to_database(redb_table_name, columns, csv_path, db)