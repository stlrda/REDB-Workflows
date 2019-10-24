# Retrieves list from github of files to be downloaded from Saint Louis City Open Data Portal

import pandas as pd
import io
import requests


def files_to_download(url):
    print('Acquiring list of files to download...')
    s = requests.get(url).content
    city_files_list = pd.read_csv(io.StringIO(s.decode('utf-8')))
    print(city_files_list)

    zip_file_list = city_files_list['Zip File Name']
    database_file_list = city_files_list['Database File Names']
    url_list = city_files_list['Direct URL']

    city_file_array = [
        zip_file_list.values.tolist(),
        database_file_list.values.tolist(),
        url_list.values.tolist()
    ]
    return city_file_array
#
# for item in city_files:
#     print(item)
