import datetime

from python.download_city_file import download_city_file
from python.list_files import listfiles
from python.retrieve_city_file_array import files_to_download
from python.variables import source_urls, lambda_folder
from python.unzip_files_in_directory import unpack_dir
import os
import ssl

if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
    getattr(ssl, '_create_unverified_context', None)):
    ssl._create_default_https_context = ssl._create_unverified_context

#Create function looping through, downloading files to ~

db_file_array = files_to_download(source_urls)

# Prints current time
print('Starting file downloads at: ',datetime.datetime.now().strftime("%a, %b %d, %Y %I:%M:%S %p"))

# for each array row in 'db_file_array'
for row in db_file_array[2]:
    # download into lambda_folder directory
    download_city_file(row, lambda_folder)

print('Downloads finished at: ',datetime.datetime.now().strftime("%a, %b %d, %Y %I:%M:%S %p"))

listfiles(lambda_folder)

#unpack all zip files in lambda_folder directory
unpack_dir(lambda_folder)

listfiles(lambda_folder)

#determine file type and read

#export to ~ delimited file, save to S3

#Boto is library for AWS functions*