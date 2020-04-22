# Standard library
import re
import csv
import subprocess
from datetime import datetime

# Third party
import pprint
import pandas as pd
from meza import io
from colorama import Fore, Style

# Custom
from Classes.Database import Database


def print_time(phase="Unspecified", table="Unspecified"):
    """
    Prints time to stdout / console in custom format.
    """

    now = datetime.now()
    now = f"{now.hour}:{now.minute}:{now.second}"
    init_start = f"{Fore.YELLOW}Table: {table} has begun initializing.{Style.RESET_ALL} Time : {now}"
    init_complete = f"{Fore.GREEN}Table: {table} has initialized.{Style.RESET_ALL} Time : {now}"
    append_start = f"{Fore.YELLOW}Table: {table} is being appended.{Style.RESET_ALL} Time : {now}"
    append_complete = f"{Fore.GREEN}Table: {table} successfully appended.{Style.RESET_ALL} Time : {now}"

    if phase == "Unspecified":
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


def get_tables(path_to_database):
    """
    """

    try:
        tables = subprocess.check_output(["mdb-tables", path_to_database])

    except subprocess.CalledProcessError as exc:
        tables = exc.output

    tables = tables.decode().split()
    print(f'Tables found = {tables}')

    return tables



def convert_scientific_notation(current_row):
    """
    """
    converted_row = {}

    for key, value in current_row.items():
        if re.search(r'^\d{1}[.]\d*[Ee][+-]\d+$', value):
            converted_row[key] = float(value)
        else:
            converted_row[key] = value

    return converted_row



def initialize_csv(table, limit=50_000):
    """
    """


    global row, snapshot_row

    batch = []

    snapshot_row = next(row)
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

        except RuntimeError as err:
            row = None
            print(err)
            break
    
    table_dataframe = pd.DataFrame(batch, index=None)
    batch.clear()
    table_dataframe.to_csv(f'resources/{table}.csv',
                           index=False,
                           sep="|",
                           line_terminator="\r\n",
                           encoding="utf-8",
                           escapechar="\\",
                           quoting=csv.QUOTE_ALL)
                           
    print_time("init_complete", table)


def append_to_csv(table, limit=50_000):
    """
    """


    global row

    batch = []
    rows_generated = 0

    print_time("append_start", table)

    while (rows_generated < limit) and (row != None):
        try:
            current_row = next(row)
            current_row = convert_scientific_notation(current_row)
            batch.append(current_row)
            rows_generated += 1

        except RuntimeError as err:
            row = None
            print(err)
            break
    
    table_dataframe = pd.DataFrame(batch, index=None)
    batch.clear()
    table_dataframe.to_csv(f'resources/{table}.csv',
                            header=False,
                            index=False,
                            mode="a",
                            sep="|",
                            line_terminator="\r\n",
                            encoding="utf-8",
                            escapechar="\\",
                            quoting=csv.QUOTE_ALL)

    print_time("append_complete", table)

    if row != None:
        append_to_csv(table, limit)


def main(**kwargs):
    """
    """


    global row, snapshot_row, db

    BUCKET_NAME = kwargs["bucket"]
    AWS_ACCESS_KEY_ID = kwargs["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = kwargs["aws_secret_access_key"]

    PG_DATABASE = kwargs["pg_database"]
    PG_HOST = kwargs["pg_host"]
    PG_USER = kwargs["pg_user"]
    PG_PASSWORD = kwargs["pg_password"]
    PG_PORT = kwargs["pg_port"]

    db = Database(PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, PG_DATABASE)

    path_to_database = 'resources/prcl.mdb'

    for table in get_tables(path_to_database):
        
        row = io.read_mdb(path_to_database, table=table, encoding='utf-8') # only file path, no file objects

        if row != None:
            initialize_csv(table, limit=100_000)

        if row != None:
            append_to_csv(table, limit=100_000)

        with open(f"resources/{table}.csv", 'r+') as csv:

            db.create_table("staging_1", table, snapshot_row)

            conn = db.get_raw_connection()
            cursor = conn.cursor()
            cmd = f'COPY staging_1."{table}" FROM STDIN WITH (FORMAT CSV, DELIMITER "|", HEADER TRUE, ENCODING "utf-8")'
            cursor.copy_expert(cmd, csv)
            conn.commit()
