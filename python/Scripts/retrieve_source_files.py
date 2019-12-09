import pandas as pd
import io
import requests
import boto3
import urllib.request
import wget

url="https://raw.githubusercontent.com/stlrda/redb_python/master/config/redb_source_databases_urls.csv"
s=requests.get(url).content
working_directory = '/Users/jonathanleek/Desktop/working/'
def retreive_source_files():
    targets=pd.read_csv(io.StringIO(s.decode('utf-8')))
    print(targets)
    for index, row in targets.iterrows():
        print("Retrieving" + row['Zip File Name'])
        wget.download(row['Direct URL'], working_directory + row['Zip File Name'])
retreive_source_files()