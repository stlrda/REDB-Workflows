import os
import zipfile
from python.variables import lambda_folder

for item in os.listdir(lambda_folder):  # loop through items in dir
    if item.endswith('.zip'):  # check for ".zip" extension
        arch_name = lambda_folder + '/' + item  # get full path of files
        print(arch_name)
        zip_ref = zipfile.ZipFile(arch_name)  # create zipfile object
        zip_ref.extractall(lambda_folder)  # extract file to dir
        zip_ref.close()  # close file
        os.remove(arch_name)  # delete zipped file
