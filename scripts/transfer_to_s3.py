# Standard library
import os
import re
import tempfile
from zipfile import ZipFile

# Third party
import wget
import pandas as pd

# Custom
from .S3 import S3


def get_list_of_files(directory):
    
    """ 
    For the given path, return a List of all files in the directory tree.
    """

    items_in_directory = os.listdir(directory)
    all_files = list()

    # Iterate over all of the items.
    for item in items_in_directory:

        # Create full path
        fullPath = os.path.join(directory, item)

        # If item is a directory then get the list of files in this directory.
        if os.path.isdir(fullPath):
            all_files = all_files + get_list_of_files(fullPath)
        else:
            all_files.append(fullPath)
                
    return all_files 


def unzip(files, targetDirectory):
    """
    Unzips a List of files to specified directory.

    :param files: List of absolute filepaths.
    :param targetDirectory: absolute path for result of unzipped file.
    """
    for file in files:

        if (file.endswith(".zip")):

            with ZipFile(file, 'r') as zipObj:
                zipObj.extractall(targetDirectory)
            os.remove(file)
    
    
    if (".zip" in [name[-4:] for name in os.listdir(targetDirectory)]):
        unzip(get_list_of_files(targetDirectory), targetDirectory)
    else:
        # Prints a preview of file extentions located in targetDirectory.
        print([name[-4:] for name in os.listdir(targetDirectory)])


def tempfile_to_s3(SOURCES_DATAFRAME, s3):
    """
      Creates temporary folder that is then used to unzip and upload source files.

      :param SOURCES_DATAFRAME: A pandas DataFrame containing rows for "Link Name", "File Name", and "Direct URL"
      :param s3: An s3 client object that includes an "upload_file" method.
    """
    SOURCES_VISITED = []

    for index, row in SOURCES_DATAFRAME.iterrows():

        link_name = row["Link Name"]
        file_name = row["File Name"]
        url = row["Direct URL"]

        # ? strings for stdout
        downloading = f'\nDownloading {file_name}- {link_name} \nFrom: {url}'
        success = f'\nDownload for {file_name}: SUCCESSFUL'

        if (url not in SOURCES_VISITED):
            with tempfile.TemporaryDirectory() as tmp:

                # ? Destination for temporary file.
                path = os.path.join(tmp, file_name)

                try:
                    print(downloading)
                    SOURCES_VISITED.append(url)
                    wget.download(url, path, bar=None) # ! Use bar=None to avoid Airflow errors.
                    print(success + " @ " + path)
                    unzip(get_list_of_files(tmp), tmp)
                    
                except Exception as err:
                    message = f'\nDownload for {file_name}: UNSUCCESSFUL: {err}'
                    print(message)

                for file in os.listdir(tmp):
                    path = os.path.join(tmp, file)
                    s3.upload_file(path)


def main(bucket, aws_access_key_id, aws_secret_access_key):

    SOURCES_CSV = "/usr/local/airflow/dags/efs/redb/resources/redb_source_databases_all-info.csv" # Prepend "../" if executing outside Airflow container. 
    SOURCES_DATAFRAME = pd.read_csv(SOURCES_CSV)

    s3 = S3(bucket, aws_access_key_id, aws_secret_access_key)

    tempfile_to_s3(SOURCES_DATAFRAME, s3)