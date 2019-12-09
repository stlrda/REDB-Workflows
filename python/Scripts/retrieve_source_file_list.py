import pandas as pd
import io
import requests
import boto3
import urllib.request

url="https://raw.githubusercontent.com/stlrda/redb_python/master/config/redb_source_databases_urls.csv"
s=requests.get(url).content


def retreive_source_file_list():
    targets=pd.read_csv(io.StringIO(s.decode('utf-8')))
    targets = list(set(targets))
    for target in targets:
        print("Retieving " + target["Zip File Name"])
        urllib.request.urlretrieve(target["Direct URL"], "/temp/" + target["Zip File Name"])
        print("Downloaded " + target["Zip File Name"])

retreive_source_file_list()