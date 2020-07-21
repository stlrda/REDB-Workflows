import os

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