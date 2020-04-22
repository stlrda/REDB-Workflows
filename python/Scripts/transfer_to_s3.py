# Standard library
import os
import re
import tempfile
from zipfile import ZipFile

# Third party
import wget
import boto3
import pandas as pd
from colorama import Fore, Style
from botocore.exceptions import ClientError

SOURCES_CSV = "resources/redb_source_databases_all-info.csv" # ! Relative (for Dockerized Airflow testing)
#SOURCES_CSV = "../resources/redb_source_databases_all-info.csv" # ! Relative for local testing.
SOURCES_DATAFRAME = pd.read_csv(SOURCES_CSV)
SOURCES_VISITED = []


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

    :param: files -> List of absolute filepaths.
    :param: targetDirectory -> absolute path for result of unzipped file.
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


def upload_file(file):
    """Upload a file to an S3 bucket

    :param file: Path for file to upload
    :param bucket: Name of bucket to upload to
    :return: True if file was uploaded, else False
    """
    
    # Removes all text prior to final forward slash (UNIX) or final backslash (Windows).
    # This value then becomes the name of the file in the S3 bucket.
    object_name = re.sub(r'.*(/|\\)', '', file)

    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Upload the file
    try:
        print(f'\nUploading {file} to {BUCKET_NAME} in s3...')
        s3_client.upload_file(file, BUCKET_NAME, object_name)
        print(f'{Fore.GREEN}{object_name} successfully uploaded to {BUCKET_NAME} in s3.{Style.RESET_ALL}')

    except Exception as e:
        print(e)
        return False

    return True


def tempfile_to_s3():
    """
      Creates temporary folder that is then used to unzip and upload source files.
    """

    for index, row in SOURCES_DATAFRAME.iterrows():

        link_name = row["Link Name"]
        file_name = row["File Name"]
        url = row["Direct URL"]

        # ? strings for stdout
        downloading = f'\nDownloading {file_name}- {link_name} \nFrom: {url}'
        success = f'\nDownload for {file_name}: {Fore.GREEN}SUCCESSFUL{Style.RESET_ALL}'

        if (url not in SOURCES_VISITED):
            with tempfile.TemporaryDirectory() as tmp:

                # ? Destination for temporary file.
                path = os.path.join(tmp, file_name)

                try:
                    print(downloading)
                    SOURCES_VISITED.append(url)
                    wget.download(url, path, bar=None) # ! Use bar=None to avoid errors.
                    print(success + " @ " + path)
                    unzip(get_list_of_files(tmp), tmp)
                    
                except Exception as err:
                    message = f'\nDownload for {file_name}: {Fore.RED}UNSUCCESSFUL:{Style.RESET_ALL} {err}'
                    print(message)

                for file in os.listdir(tmp):
                    path = os.path.join(tmp, file)
                    upload_file(path)


def main(bucket, aws_access_key_id, aws_secret_access_key):
    
    global BUCKET_NAME, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

    BUCKET_NAME = bucket
    AWS_ACCESS_KEY_ID = aws_access_key_id
    AWS_SECRET_ACCESS_KEY = aws_secret_access_key

    tempfile_to_s3()