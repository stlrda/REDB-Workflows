from sqlalchemy import *


""" Class for target database that overrides default sqlalchemy functionality with custom features.
The most import feature here is the "replace_table" method which dynamically creates tables
thus removing the need to define them prior to ingest."""
class Database():

    def __init__(self, user, password, host, port, database_name, schema=None):
        self.ENGINE = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')
        self.METADATA = MetaData(self.ENGINE) if schema == None else MetaData(self.ENGINE, schema=schema)


    def create_schema(self, schema):
        """ Creates a schema within the database if it doesn't exists.

        :param schema:  Name of schema to be created.
        """
        if self.ENGINE.dialect.has_schema(self.ENGINE, schema=schema):
            print(f"Schema: '{schema}' already exists.")
            return False
            
        try:
            self.ENGINE.execute(f'CREATE SCHEMA IF NOT EXISTS {schema};')
            print(f"Schema: '{schema}' created.")
            return True

        except Exception as err:
            print(err)
            return False



    def replace_table(self, schema, table_name, columns):
        """ Creates table within database with no constraints and all types as VARCHAR(250)
        Will delete table if it already exists in the target database.

        :param schema: Desired schema for table.
        :param table_name: Desired name for table.
        :param example_row: A dictionary containing keys that represent the table's columns
        """

        table = Table(table_name, self.METADATA, schema=schema)

        for column in columns:
            table.append_column(Column(column, VARCHAR(1000)))

        if self.ENGINE.dialect.has_table(self.ENGINE, table_name, schema=schema):
            self.ENGINE.execute(f"DROP TABLE {schema}.{table_name} CASCADE;")

        try:
            table.create()
            print(f"{table_name} created.")
            return True

        except Exception as err:
            print(err)
            return False


    def create_table(self, schema, table_name, columns):
        """Same as replace table, but Will NOT delete table if it already exists.

        :param schema: Desired schema for table.
        :param table_name: Desired name for table.
        :param example_row: A dictionary containing keys that represent the table's columns
        """

        table = Table(table_name, self.METADATA, schema=schema)

        for column in columns:
            table.append_column(Column(column, VARCHAR(1000)))

        if self.ENGINE.dialect.has_table(self.ENGINE, table_name, schema=schema):
            print(f"{table_name} already exists in {schema}.")
            return False

        try:
            table.create()
            print(f"{table_name} created.")
            return True

        except Exception as err:
            print(err)
            return False


    def get_raw_connection(self):
        return self.ENGINE.raw_connection()
