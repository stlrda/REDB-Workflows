import logging
import boto3
from botocore.exceptions import ClientError
import requests
import urllib.request
import tempfile
import io
import pandas as pd

def upload_file(url, bucket, profile='default'):
    
    """Upload a file to an S3 bucket
    :param url: URL Path to files
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    
    #Create list of url to pull zip from
    try:
        s = requests.get(url).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        targets = df['Direct URL']
        targets = list(set(targets))
    except:
        return (f'ERROR - check URL:  {url}')

    #If no profile specified use default
    boto3.setup_default_session(profile_name=profile) 
    
    #iterate through list of url and download zip
    for target in targets:
        with tempfile.TemporaryDirectory() as tmpdirname:
            print("Retieving " + target)
            urllib.request.urlretrieve(target, tmpdirname + "/" + target.rsplit('/', 1)[-1]) # TODO is there a better way to retrieve file name?
            print("Downloaded "+target.rsplit('/', 1)[-1])
            s3_client = boto3.client('s3')
            try:
                s3_client.upload_file(tmpdirname + "/" + target.rsplit('/', 1)[-1], bucket, target.rsplit('/', 1)[-1])
            except ClientError as e:
                logging.error(e)
                return False
            return True