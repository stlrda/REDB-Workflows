# Native
import os
import sys
import types
import unittest

# Custom
sys.path.append("..")
from utils.data_transformations import (convert_scientific_notation, generate_rows, merge_split_rows)


# TODO Write unit tests.
class TestDataTransformations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        table_name = "BldgAll"
        database_name = "prcl.mdb"
        path_to_database = f"/usr/local/airflow/dags/efs/redb/resources/{database_name}"
        output_txt_file = f"/usr/local/airflow/dags/efs/redb/resources/{table_name}.txt"
        cls.row = generate_rows(path_to_database, table_name, delimiter="|")
        os.system(f"mdb-export {path_to_database} {table_name} -d '|' > {output_txt_file}")
        with open(output_txt_file, 'r+') as table_txt:
            cls.table_txt = table_txt
            cls.column_names = table_txt.readline().split("|")
            cls.column_names[-1] = cls.column_names[-1].replace("\n", "")
            cls.column_count = len(cls.column_names)


    @classmethod
    def tearDownClass(cls):
        pass


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def test_convert_scientific_notation(self):
        pass


    def test_generate_rows(self):
        # TODO check type and len of generator.
        # self.assertTrue( len(list(self.row)) > 0)
        # self.assertTrue(isinstance(self.row, types.GeneratorType))
        pass


    def test_merge_split_rows_on_txt_file(self):
        pass


    def test_merge_split_rows_on_generated_rows(self):
        row = self.row
        column_count = self.column_count
        column_names = self.column_names
        while row != None:
            try:
                current_row = next(row)

                if len(current_row.keys()) < column_count:
                    # Must also disable buffering for unittest for errors to print to stdout.
                    current_row = merge_split_rows(column_names, current_row, row, True)

            # Python 3.4 returns "RuntimeError" once Generator reaches the end.
            except RuntimeError:
                row = None
                break


if __name__ == '__main__':
    # Buffer suppresses stdout.
    unittest.main(buffer=True)