# Native
import os
import sys
import types
import unittest

# Custom
sys.path.append("..")
from utils.data_transformations import (convert_scientific_notation
                                        , generate_rows
                                        , mdb_to_txt)


# TODO Write unit tests.
class TestDataTransformations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.table_name = "BldgAll"
        cls.database_name = "prcl.mdb"
        cls.path_to_database = f"/usr/local/airflow/dags/efs/redb/resources/{cls.database_name}"
        cls.output_path = f"/usr/local/airflow/dags/efs/redb/resources/"
        cls.mdb_txt = mdb_to_txt(cls.path_to_database, cls.table_name, cls.output_path)
        cls.generated_row = generate_rows(cls.path_to_database, cls.table_name, delimiter="|")

        with open(cls.mdb_txt, 'r+') as table_txt:
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

    
    def test_generate_rows(self):
        column_count = self.column_count
        row = self.generated_row
    
        first_row = next(row)
        self.assertIsInstance(first_row, dict)
        self.assertEqual(column_count, len(first_row.keys()))

        row_count = 1

        while row != None:
            try:
                next(row)
                row_count += 1
            # Python 3.4 returns "StopIteration" once Generator reaches the end.
            except StopIteration:
                row = None
                break
        self.assertGreater(row_count, 0)
        self.assertTrue(isinstance(self.generated_row, types.GeneratorType))


    def test_mdb_to_txt(self):
        self.assertIsInstance(self.column_names, list)
        self.assertEqual(len(self.column_names), self.column_count)

    # TODO finish test
    def test_convert_scientific_notation(self):
        example1 = "1.0000000000000000e+00"
        example2 = "5.0000000000000000e+00"
        example3 = "0.0000000000000000e+00"
        pass


if __name__ == '__main__':
    # Buffer suppresses stdout.
    unittest.main(buffer=False)