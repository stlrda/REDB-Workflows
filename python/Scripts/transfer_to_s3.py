# Standard Library
import os
import re
import logging
import tempfile
import datetime as dt
from zipfile import ZipFile

# Third party
import wget
import boto3
import pandas as pd
from colorama import Fore, Style
from botocore.exceptions import ClientError

# Relative (for Dockerized Airflow testing)
SOURCES_CSV = "resources/redb_source_databases_all-info.csv"

# Relative for local testing
# SOURCES_CSV = "../resources/redb_source_databases_all-info.csv"

SOURCES_DATAFRAME = pd.read_csv(SOURCES_CSV)
TIMESTAMP = '{:%Y-%m-%d %H:%M:%S}'.format
sources_visited = []

def upload_file(file, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file: Path for file to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        # Forward slash is for UNIX/Linus systems only. Change to backslash for Windows.
        object_name = re.sub(r'.*/', '', file)

    s3_client = boto3.client('s3', aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                      aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"])

    # Upload the file
    try:
        print(object_name)
        print(f'\nUploading {file} to {bucket} in s3...')
        s3_client.upload_file(file, bucket, object_name)
        print(f'{object_name} successfully uploaded to {bucket} in s3.')

    except Exception as e:
        print(e)
        logging.error(e)
        return False

    return True


# ? For the given path, get the List of all files in the directory tree 
def getListOfFiles(directory):
    
    # create a list of file and sub directories 
    # names in the given directory 
    listOfFile = os.listdir(directory)
    allFiles = list()

    # Iterate over all the entries
    for entry in listOfFile:

        # Create full path
        fullPath = os.path.join(directory, entry)

        # If entry is a directory then get the list of files in this directory 
        if os.path.isdir(fullPath):
            allFiles = allFiles + getListOfFiles(fullPath)
        else:
            allFiles.append(fullPath)
                
    return allFiles 


def unzip(files, targetDirectory):

    for file in files:

        if (file.endswith(".zip")):

            with ZipFile(file, 'r') as zipObj:
                zipObj.extractall(targetDirectory)
            os.remove(f'{file}')
    
    if (".zip" in [name[-4:] for name in os.listdir(targetDirectory)]):
        unzip(getListOfFiles(targetDirectory), targetDirectory)
    else:
        print([name[-4:] for name in os.listdir(targetDirectory)])


def tempfile_to_s3():

    for index, row in SOURCES_DATAFRAME.iterrows():

        link_name = row["Link Name"]
        file_name = row["File Name"]
        url = row["Direct URL"]

        # ? strings for stdout
        downloading = f'\nDownloading {file_name}- {link_name} \nFrom: {url}'
        success = f'\nDownload for {file_name}: {Fore.GREEN}Successful.{Style.RESET_ALL}'

        if (url not in sources_visited):
            with tempfile.TemporaryDirectory() as tmp:
                try:
                    print(downloading)
                    sources_visited.append(url)
                    # ! bar must be set to None to comply with Airflow default settings
                    wget.download(url, f'{tmp}/{file_name}', bar=None) 
                    print(success)
                    unzip(getListOfFiles(f'{tmp}'), f'{tmp}')
                    print(os.listdir(tmp))
                    
                except Exception as err:
                    time = TIMESTAMP(dt.datetime.now())
                    message = f'\nDownload for {file_name}: {Fore.RED}Unsuccessful:{Style.RESET_ALL} {err}'
                    print(message)
                    logging.error(time + message)

                for file in os.listdir(tmp):
                    # Forward slash is for UNIX/Linus systems only. Change to backslash for Windows.
                    file = f'{tmp}/{file}'
                    print(file)
                    upload_file(file, 'redb-test')