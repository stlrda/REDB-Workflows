# Standard library
import os
import re
import csv
import tempfile
from io import StringIO
from datetime import datetime
from subprocess import check_output, Popen, PIPE, CalledProcessError

# Third party
import pandas as pd

# Custom
from .Database import Database
from .S3 import S3

pd.set_option('display.max_columns', None)  
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', -1)


def initialize_global_IO(kwargs):
    """Initializes S3 Bucket and Database using the credentials passed in via DAG.
    The S3 Bucket and Database objects then become global variables.
    Expecting : {credential_name : credential_value, ...}
    """
    
    global s3, db

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
    """ Returns a list of every column name and column type for a specified .mdb table.

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
            column = (column_name, column_type)
            columns.append(column)

    columns.pop(0) # The first column is just the table name and an open bracket.

    print(f'Columns ({len(columns)}) for table: {table} @ {path_to_database}:\n{columns}')

    return columns


def generate_rows(filepath, table, **kwargs):
    """Reads an MS Access file
    Args:
        filepath (str): The mdb file path.
        table (str): The table to load.
        kwargs (dict): Keyword arguments that are passed to the csv reader.
    Yields:
        dict: A row of data whose keys are the field names.
    Examples:
        >>> records = generate_rows(filepath, table)
        >>> expected = {
        ...     'surname': 'Aaron',
        ...     'forenames': 'William',
        ...     'freedom': '07/03/60 00:00:00',
        ...     'notes': 'Order of Court'}
        >>> first_row = next(records)
        >>> (expected == first_row) if first_row else True
        True
    """
    pkwargs = {'stdout': PIPE, 'bufsize': 1, 'universal_newlines': True}

    # https://stackoverflow.com/a/2813530/408556
    # https://stackoverflow.com/a/17698359/408556
    with Popen(['mdb-export', filepath, table, '-d |'], **pkwargs).stdout as pipe:
        first_line = StringIO(str(pipe.readline()))
        names = next(csv.reader(first_line, **kwargs))
        headers = [name.rstrip() for name in names]

        for line in iter(pipe.readline, b''):
            next_line = StringIO(str(line))
            values = next(csv.reader(next_line, **kwargs))
            values = [value.rstrip() for value in values]
            yield dict(zip(headers, values))


def print_time(phase="unspecified", table="unspecified"):
    """
    Prints time to stdout / console in custom format.
    """

    now = datetime.now()
    now = f"{now.hour}:{now.minute}:{now.second}"
    init_start = f"Table: {table} has begun initializing. Time : {now}"
    init_complete = f"Table: {table} has initialized. Time : {now}"
    append_start = f"Table: {table} is being appended. Time : {now}"
    append_complete = f"Table: {table} successfully appended. Time : {now}"

    if phase == "unspecified":
        print(now)
    elif phase == "init_start":
        print(init_start)
    elif phase == "init_complete":
        print(init_complete)
    elif phase == "append_start":
        print(append_start)
    elif phase == "append_complete":
        print(append_complete)
    else:
        print("Qué?" + " " + now)


# TODO Incorporate column_types into function to make more efficient.
def convert_scientific_notation(current_row):
    """ Identifies scientific notation values then converts to float.

    :param current_row: The row that is to be converted.
    The current_row must be passed in as a Python Dictionary.
    """
    converted_row = {}

    for key, value in current_row.items():

        value = str(value)
        
        # Regex reads: one digit > period > zero or more digits > "E" or "e" > "+" or "-" > one or more digits.
        if re.search(r'^\d{1}[.]\d*[Ee][+-]\d+$', value):
            converted_row[key] = float(value)
            
        else:
            converted_row[key] = value

    return converted_row


# TODO Output the location / values of the malformed rows and fields to a log file.
def merge_split_rows(column_names, broken_row):
    """ Will merge rows that have been broken into multiple parts thanks to a newline in one of the fields.

    :param column names: A List of column names to be returned in fixed row.
    :current_row: Current iteration of Generator object representing broken row.
    """

    # Start with a fresh row.
    pending_row = {}

    # Assign columns to be added to fresh row.
    pending_columns = column_names.copy()

    # Add all of the columns and values already present in broken row to pending row.
    for key, value in broken_row.items():
        pending_column = pending_columns.pop(0)
        pending_row[pending_column] = value

    # The last column present in broken row is the row with the newline.
    column_with_newline = pending_column
    print(f"Merging fields split by newline. Column with newline: {column_with_newline}\n{pending_row[column_with_newline]}")
    print(f"Current row data where the newline is currently being handled:\n {broken_row}")
        
    next_row = next(row)

    # An empty Dictionary is passed if two back to back newlines occur...
    # this statement replaces the empty dict with appropriate key/value.
    if len(next_row.keys()) == 0:
        next_row = {column_names[0] : "\n"}
   
    next_row_values = list(next_row.values())
    
    # The first value in the next row was the field split in half by the newline.
    next_line = next_row_values.pop(0)

    if next_line == "\n":
        # if the next value is just a newline, concatenate the newline into the broken/split field
        pending_row[column_with_newline] += next_line
    else:
        # Because the newline is omitted from the string, add the newline back in before concatenating with field.
        pending_row[column_with_newline] += ("\n" + next_line)

    # For the remaining values, add the next pending column and current value to pending row.
    for value in next_row_values:
        pending_column = pending_columns.pop(0)
        pending_row[pending_column] = value
    
    # If all columns have not been added to pending row,
    # execute merge function again with pending row as broken row argument.
    if len(pending_columns) != 0:
        return merge_split_rows(column_names, pending_row)
    else:
        # Removes trailing double quote from broken field.
        pending_row[column_with_newline] = pending_row[column_with_newline][0:-1]

        print(f"Fixed row:\n {pending_row}")

        return pending_row
    

def initialize_csv(table, columns, limit=50_000):
    """ Creates a CSV from a Python Generator with a select number of rows.

    :param table: The name of the table and target CSV.
    :param limit: The amount of rows appended to CSV at a time.

    The "batch" list will store @limit number of rows into RAM before
    bulk transformation to Pandas DataFrame. That DataFrame then
    creates the initial CSV.
    """

    global row, snapshot_row

    batch = []
    
    # snapshot_row is used to initialize table via "replace_table" method and to assign columns to "table_dataframe".
    # snapshot_row is initialized globally as to be available throughout script.
    snapshot_row = next(row)
    snapshot_row = convert_scientific_notation(snapshot_row)
    batch.append(snapshot_row)


    rows_generated = 1

    column_count = len(columns)
    column_names = [column[0] for column in columns]
    column_types = [column[1] for column in columns]

    print_time("init_start", table)

    while (rows_generated < limit) and (row != None):
        try:
            current_row = next(row)

            if len(current_row.keys()) < column_count:
                current_row = merge_split_rows(column_names, current_row)

            current_row = convert_scientific_notation(current_row)
            
            batch.append(current_row)
            rows_generated += 1

        # Python 3.4 returns "RuntimeError" once Generator reaches the end.
        except RuntimeError as err:
            row = None
            print(err)
            break
    
    table_dataframe = pd.DataFrame(batch, columns=snapshot_row.keys())
    batch.clear()

    table_dataframe.to_csv(f'./resources/{table}.csv',
                           index=False,
                           sep="|",
                           na_rep="",
                           line_terminator="\r\n",
                           encoding="utf-8")
                           
    print_time("init_complete", table)


def append_to_csv(table, columns, limit=50_000):
    """ Recursively appends table rows from a Python Generator to an existing CSV.

    :param table: The name of the table and target CSV.
    :param limit: The amount of rows appended to CSV at a time.

    The "batch" list will store @limit number of rows into RAM before
    bulk transformation to Pandas DataFrame. That DataFrame is then
    appended to the CSV.
    """

    global row

    batch = []
    rows_generated = 0

    column_count = len(columns)
    column_names = [column[0] for column in columns]
    column_types = [column[1] for column in columns]

    print_time("append_start", table)

    while (rows_generated < limit) and (row != None):
        try:
            current_row = next(row)

            if len(current_row.keys()) < column_count:
                current_row = merge_split_rows(column_names, current_row)

            current_row = convert_scientific_notation(current_row)
            batch.append(current_row)
            rows_generated += 1

        # Python 3.4 returns "RuntimeError" once Generator reaches the end.
        except RuntimeError as err:
            row = None
            print(err)
            break
    
    table_dataframe = pd.DataFrame(batch, columns=snapshot_row.keys())
    batch.clear()
    table_dataframe.to_csv(f'./resources/{table}.csv',
                            header=False,
                            index=False,
                            mode="a",
                            sep="|",
                            line_terminator="\r\n",
                            encoding="utf-8")

    print_time("append_complete", table)

    if row != None:
        # TODO Investigate lowering memory by removing return / recursion.
        return append_to_csv(table, columns, limit)


def main(**kwargs):
    """ Mainline logic for script. Downloads .mdb files from S3 Bucket,
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

    global row, snapshot_row

    initialize_global_IO(kwargs) # Initializes S3 Bucket and target database to global scope.

    mdb_files_in_s3 = s3.list_objects(extension=".mdb", field="Key")

    for mdb in mdb_files_in_s3:

        # ! Uncomment / comment and modify to specify a database.
        if mdb != "prcl.mdb":
            continue

        with tempfile.TemporaryDirectory() as tmp:

            
            path_to_database =  os.path.join(tmp, mdb)
            s3.download_file(s3.bucket_name, mdb, path_to_database)

            # Creates CSVs from tables.
            for table in get_tables(path_to_database):
                
                # ! Uncomment / comment and modify to specify a table.
                # if table != "Prcl":
                #     continue

                columns = get_table_columns(table, path_to_database)
                column_names = [column[0] for column in columns]

                # "generate_rows" returns a Python Generator that is being initialized globally via the "global" keyword. 
                # The Generator (row) can thus be shared throughout the script.
                row = generate_rows(path_to_database, table=table, delimiter="|")

                mdb_name = mdb[:-4] # name of Access database
                table = mdb_name + "_" + table # prepends database name to table

                initialize_csv(table, columns, limit=50_000)

                if row != None:
                    append_to_csv(table, columns, limit=50_000)

                # Opens CSV then copies to database.
                with open(f"./resources/{table}.csv", 'r+') as csvfile:

                    db.replace_table("staging_1", table, snapshot_row) # snapshot_row created upon invoking "initialize_csv" function.

                    conn = db.get_raw_connection()
                    cursor = conn.cursor()
                    cmd = f'COPY staging_1."{table}" FROM STDIN WITH (FORMAT CSV, DELIMITER "|", HEADER TRUE, ENCODING "utf-8")'
                    cursor.copy_expert(cmd, csvfile)
                    conn.commit()
                    print(f"{table} copied into staging_1.")
                
                os.remove(f"./resources/{table}.csv")
            
