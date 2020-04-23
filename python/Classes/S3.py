# Standard library
import os
import re

# Third party
import boto3
from colorama import Fore, Style
from botocore.exceptions import ClientError


# Class for target s3 Bucket that overrides boto3 default functionality with custom features.
class S3():

    def __init__(self, bucket_name, aws_access_key_id, aws_secret_access_key):
        self.bucket_name = bucket_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)

    def upload_file(self, file):
        """Upload a file to an S3 bucket

        :param file: Path for file to upload
        :return: True if file was uploaded, else False
        """
        
        # Removes all text prior to final forward slash (UNIX) or final backslash (Windows).
        # This value then becomes the name of the file in the S3 bucket.
        object_name = re.sub(r'.*(/|\\)', '', file)

        # Upload the file
        try:
            print(f'\nUploading {file} to {self.bucket_name} in s3...')
            self.client.upload_file(file, self.bucket_name, object_name)
            print(f'{Fore.GREEN}{object_name} successfully uploaded to {self.bucket_name} in s3.{Style.RESET_ALL}')

        except Exception as e:
            print(e)
            return False

        return True


    def download_file(self, bucket_name, key, save_as):
        """ Download an s3 object as a file from s3.

        :param bucket_name: Name of bucket.
        :param key: Name of s3 object to be downloaded.
        :param save_as: The desired directory + file name of s3 object once downloaded.
        """

        # Download the file
        try:
            print(f'\nDownloading {key} from {self.bucket_name} in s3...')
            self.client.download_file(bucket_name, key, save_as)
            print(f'{Fore.GREEN}{key} successfully downloaded from {self.bucket_name} in s3.{Style.RESET_ALL}')
            print(f'Your download can be found @ {Fore.BLUE}{save_as}{Style.RESET_ALL}')

        except Exception as e:
            print(e)
            return False

        return True


    def list_objects(self, extension=None, field=None):
        """ Return a list of s3 objects
        :param extension: Limits objects returned to specified file extension. Default = None.
        :param field: Limits objects returned to specified field. Default = None.

        boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects
        """

        s3_objects = self.client.list_objects(Bucket=self.bucket_name)['Contents']
        extension_length = 0 if extension == None else len(extension)
        filtered_objects = []
        
        if (field == None) and (extension == None):
            return s3_objects

        for s3_object in s3_objects:
            s3_object = s3_object[field] if field != None else s3_object
            
            if s3_object[extension_length:] == extension:
                filtered_objects.append(s3_object)

            return filtered_objects