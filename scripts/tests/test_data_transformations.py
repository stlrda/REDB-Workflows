# Native
import os
import sys
import unittest

# Custom
sys.path.append("..") # TODO Fix path.
from utils.data_transformations import (convert_scientific_notation, generate_rows, merge_split_rows)

# TODO Write unit tests.
class TestDataTransformations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        path_to_database = "/usr/local/airflow/dags/efs/redb/resources/prcl.mdb"
        table_name = "BldgAll"
        cls.row = generate_rows(path_to_database, table_name, delimiter="|")
        # cls.row2 = generate_rows(path_to_database, table_name, delimiter="|")
        os.system(f"mdb-export {path_to_database} {table_name} -d '|' > BldgAll.txt")
        with open(f"{table_name}.txt", 'r+') as table_txt:
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
        pass


    def test_merge_split_rows(self):
        row = self.row
        column_count = self.column_count
        column_names = self.column_names
        while row != None:
            try:
                current_row = next(row)

                if len(current_row.keys()) < column_count:
                    current_row = merge_split_rows(column_names, current_row, row)

            # Python 3.4 returns "RuntimeError" once Generator reaches the end.
            except RuntimeError as err:
                row = None
                print(err)
                break


if __name__ == '__main__':
    # buffer suppresses stdout
    unittest.main(buffer=False)