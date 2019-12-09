import logging
import boto3
from botocore.exceptions import ClientError
import requests
import wget
import tempfile
import io
import pandas as pd

def upload_file(url, bucket, profile='default'):
    
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    
    #Create list of url to pull zip from
    try:
        s = requests.get(url).content
        targets = pd.read_csv(io.StringIO(s.decode('utf-8')))
        
    except:
        return (f'error {url}')
        
    #If no profile specified use default
    boto3.setup_default_session(profile_name=profile) 
    
    #iterate through list of url and download zip
    for index, row in targets.iterrows():
        with tempfile.TemporaryDirectory() as tmpdirname:
            print("Retieving " + row['Zip File Name'])
            wget.download(row['Direct URL'], tmpdirname + "/" + row['Zip File Name'])
            print("Downloaded "+ row['Zip File Name'])
            s3_client = boto3.client('s3')
            try:
                s3_client.upload_file(tmpdirname + "/" + row['Zip File Name'], bucket, row['Zip File Name'])
            except ClientError as e:
                logging.error(e)
                return False
            return True