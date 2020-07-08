# Standard Library
import io
import os
import logging
import zipfile

# Third party
import boto3
from botocore.exceptions import ClientError

def unzip(bucket='redb-workbucket'):
    
    s3_resource = boto3.resource('s3')
    s3_client = boto3.client('s3', aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                      aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"])
    paginator = s3_client.get_paginator("list_objects_v2")

    zip_files = []

    for page in paginator.paginate(Bucket=bucket):
        for obj in page['Contents']:
            if obj['Key'].endswith('.zip'):
                zip_files.append(obj['Key'])
            else:
                pass

    if len(zip_files) > 0:
        print(zip_files)
        for key in zip_files:
            zip_obj = s3_resource.Object(bucket_name=bucket, key=key)
            buffer = io.BytesIO(zip_obj.get()["Body"].read())
            z = zipfile.ZipFile(buffer)
            for filename in z.namelist():
                file_info = z.getinfo(filename)
                s3_resource.meta.client.upload_fileobj(
                    z.open(filename),
                    Bucket=bucket,
                    Key = f'{key[:-4]}/{filename}')
            s3_resource.Object(bucket, key).delete()
        unzip(bucket)
    else:
        print('Finished')