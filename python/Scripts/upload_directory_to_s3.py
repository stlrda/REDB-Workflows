import boto3
import os

s3C = boto3.client('s3')
working_directory = '/Users/jonathanleek/Desktop/working'
bucketname = 'stl-rda-airflow-bucket'

def uploadDirectory(path, bucketname):
    for root, dirs, files in os.walk(path):
        for file in files:
            print('Uploading '+ file)
            s3C.upload_file(os.path.join(root,file),bucketname,folder/file) # TODO add foldername/ to file at end of this line so files are loaded in directories(pathlib iterdir)
            print(file + ' uploaded')

uploadDirectory(working_directory,bucketname)