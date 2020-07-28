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

    
    def test_convert_scientific_notation(self):
        notation1 = "1.0000000000000000e+00"
        notation2 = "5.0000000000000000e+00"
        notation3 = "0.0000000000000000e+00"

        expected1 = 1.0
        expected2 = 5.0
        expected3 = 0.0

        _dict = {
            "Key1": notation1
            , "Key2": notation2
            , "Key3": notation3
            , "Key4": "Shouldn't change."
            , "Key5": 123456
        }

        expected_dict = {
            "Key1": expected1
            , "Key2": expected2
            , "Key3": expected3
            , "Key4": "Shouldn't change."
            , "Key5": "123456"
        }

        actual_dict = convert_scientific_notation(_dict)

        for key in actual_dict.keys():
            actual = actual_dict[key]
            expected = expected_dict[key]

            self.assertEqual(actual, expected)


if __name__ == '__main__':
    # Buffer suppresses stdout.
    unittest.main(buffer=False)