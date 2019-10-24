# Retrieves list from github of files to be downloaded from Saint Louis City Open Data Portal

import pandas as pd
import io
import requests


def files_to_download(url):
    print('Acquiring list of files to download...l')
    s = requests.get(url).content
    city_files_list = pd.read_csv(io.StringIO(s.decode('utf-8')))
    print(city_files_list)

    urls = city_files_list['Direct URL']
    city_files = urls.values.tolist()
    return city_files
#
# for item in city_files:
#     print(item)
