import logging
import boto3
from botocore.exceptions import ClientError


def upload_file(file_name, bucket, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

##EXAMPLE CODE FOR USE
# s3=boto3.client('s3')
# # with open("/Users/jonathanleek/Dropbox/THE GREAT GAME NOTES", "rb") as f:
# #     s3.upload_fileobj(f, "jfltestbucket", "testfile")