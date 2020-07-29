"""This unit testing suite is designed with the intent to rapidly test the process
of copying specific Access tables into REDB as opposed to running the entire
"mdb_to_postgres" script wherein the 'REDB_ELT' DAG must be executed first and
the script copies every Access file into REDB indiscriminately."""

# Native
import os
import sys
import unittest

# Custom
sys.path.append("/usr/local/airflow/dags/efs/")
#from redb.scripts.tests.config import config # ! Database and S3 credentials, must be git ignored!
from redb.scripts.utils.data_transformations import generate_rows
from redb.scripts.mdb_to_postgres import (
                            initializeIO
                            , get_tables
                            , get_table_columns
                            , create_csv
                            , copy_csv_to_database)

class TestMDBtoPostgres(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.city_table_name = "BldgAll"
        cls.database_name = "prcl.mdb"
        cls.redb_table_name = f"test_{cls.database_name.lower()}_{cls.city_table_name.lower()}"
        cls.csv_path = f"/usr/local/airflow/dags/efs/redb/scripts/tests/{cls.city_table_name}"
        cls.path_to_database = f"/usr/local/airflow/dags/efs/redb/resources/{cls.database_name}"
        cls.row = generate_rows(cls.path_to_database, cls.city_table_name)
        (cls.s3, cls.db) = initializeIO(config)


    @classmethod
    def tearDownClass(cls):
        cls.db.ENGINE.execute(f'DROP TABLE "staging_1"."{cls.redb_table_name}";')
        os.remove(cls.csv_path)


    def test_get_tables(self):
        expected_tables = ['BldgAll'
                            , 'BldgCom'
                            , 'BldgRes'
                            , 'BldgResImp'
                            , 'BldgSect'
                            , 'CdAttrTypeNum'
                            , 'CxPrclCnBlk10'
                            , 'Prcl'
                            , 'PrclAddr'
                            , 'PrclAddrLRMS'
                            , 'PrclAsmt'
                            , 'PrclDate'
                            , 'PrclImp'
                            , 'PrclREAR'
                            , 'PrclSBD'
                            , 'PrclAttr']
        actual_tables = get_tables(self.path_to_database)
        self.assertListEqual(expected_tables, actual_tables)

    
    def test_get_columns(self):
        expected_columns = ['CityBlock'
                            , 'Parcel'
                            , 'OwnerCode'
                            , 'BldgNum'
                            , 'StructureId'
                            , 'BldgUse'
                            , 'BldgUseCode'
                            , 'BldgName'
                            , 'BldgCategory'
                            , 'BldgType'
                            , 'ExtWallType'
                            , 'NbrOfUnits'
                            , 'YearBuilt'
                            , 'EffectiveYearBuilt'
                            , 'TotalArea'
                            , 'NbrOfStories'
                            , 'Notes'
                            , 'ParcelId']
        actual_columns = get_table_columns(self.city_table_name, self.path_to_database)
        self.assertListEqual(expected_columns, actual_columns)


    def test_csv_to_redb(self):
        columns = get_table_columns(self.city_table_name, self.path_to_database)
        csvFile = create_csv(self.city_table_name, columns, self.row, self.csv_path)

        with open(csvFile, 'r+') as table_txt:
            lines = table_txt.readlines()
            self.assertGreater(len(lines), 0, msg=f"{self.city_table_name}.csv has no data.")

        copy_csv_to_database(self.redb_table_name, columns, self.csv_path, self.db)
        table_created = self.db.ENGINE.execute(f"""SELECT EXISTS (
                                SELECT FROM pg_tables
                                WHERE  schemaname = 'staging_1'
                                AND    tablename  = '{self.redb_table_name}'
                                );""")
        self.assertTrue(table_created)


if __name__ == '__main__':
    # Buffer suppresses stdout.
    unittest.main(buffer=True)
