import re
import os
import csv
import tempfile
from io import StringIO


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


def generate_rows(filepath, table, delimiter="|"):
    """Reads an MS Access file
    Args:
        filepath (str): The mdb file path.
        table (str): The table to load.
        delimiter (dict): Keyword arguments that are passed to the csv reader.
    Yields:
        dict: A row of data whose keys are the field names.
    """
    with tempfile.TemporaryDirectory() as tmp:
        output_txt_file = os.path.join(tmp, f"{table}.txt")
        os.system(f"mdb-export {filepath} {table} -d '{delimiter}' > {output_txt_file}")

        with open(output_txt_file, 'r') as csvfile:
            headers = next(csv.reader(csvfile, delimiter=delimiter, quotechar='"'))
            
            for line in csv.reader(csvfile, delimiter=delimiter, quotechar='"'):
                values = [value.rstrip() for value in line]
                row = dict(zip(headers, values))
                yield convert_scientific_notation(row)


def mdb_to_txt(path_to_database, table_name, output_path, delimiter="|"):
    """Returns .txt file at specified path for specified Access database and table.
    :param path_to_database:
    :param table_name:
    :param output_path:
    :param delimiter:
    """
    output_txt_file = f"{output_path}{table_name}.txt"
    os.system(f"mdb-export {path_to_database} {table_name} -d '{delimiter}' > {output_txt_file}")
    return output_txt_file