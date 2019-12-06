import pandas as pd
import io
import requests
url="https://raw.githubusercontent.com/stlrda/redb_python/master/config/redb_source_databases_urls.csv"
s=requests.get(url).content

def retreive_source_file_list():
    df=pd.read_csv(io.StringIO(s.decode('utf-8')))

